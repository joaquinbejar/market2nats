use std::time::Duration;

use async_nats::jetstream;
use futures_util::StreamExt;
use tracing::{debug, error, info, instrument, warn};

use crate::application::ports::{NatsError, NatsPublisher};
use crate::config::model::{ConsumerConfig, StreamConfig};

/// Retry delays for JetStream publish: 100ms, 500ms, 2000ms.
const RETRY_DELAYS_MS: [u64; 3] = [100, 500, 2000];

/// JetStream publisher implementation.
pub struct JetStreamPublisher {
    jetstream: jetstream::Context,
    /// Per-publish ack wait. Each `await` on the ack future is wrapped in a
    /// `tokio::time::timeout` of this duration so a missing PubAck (broker
    /// overload, subject not bound to a stream, JetStream not enabled) fails
    /// fast instead of hanging.
    ack_timeout: Duration,
}

impl JetStreamPublisher {
    /// Creates a new `JetStreamPublisher` from an existing NATS connection.
    #[must_use]
    pub fn new(client: async_nats::Client, ack_timeout: Duration) -> Self {
        let jetstream = jetstream::new(client);
        Self {
            jetstream,
            ack_timeout,
        }
    }

    /// Returns a reference to the JetStream context.
    #[must_use]
    pub fn jetstream_context(&self) -> &jetstream::Context {
        &self.jetstream
    }
}

impl NatsPublisher for JetStreamPublisher {
    #[instrument(skip(self, payload, content_type), fields(subject = %subject))]
    fn publish(
        &self,
        subject: &str,
        payload: &[u8],
        content_type: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), NatsError>> + Send + '_>>
    {
        let subject = subject.to_owned();
        let payload = bytes::Bytes::copy_from_slice(payload);
        let content_type = content_type.to_owned();

        Box::pin(async move {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Content-Type", content_type.as_str());

            let mut last_err = None;
            for (attempt, delay_ms) in RETRY_DELAYS_MS.iter().enumerate() {
                let publish_result = self
                    .jetstream
                    .publish_with_headers(subject.clone(), headers.clone(), payload.clone())
                    .await;

                match publish_result {
                    Ok(ack_future) => {
                        match tokio::time::timeout(self.ack_timeout, ack_future).await {
                            Ok(Ok(_ack)) => {
                                debug!(subject = %subject, "published to jetstream");
                                return Ok(());
                            }
                            Ok(Err(e)) => {
                                warn!(
                                    subject = %subject,
                                    attempt = attempt + 1,
                                    error = %e,
                                    "jetstream ack failed, retrying"
                                );
                                last_err = Some(e.to_string());
                            }
                            Err(_) => {
                                warn!(
                                    subject = %subject,
                                    attempt = attempt + 1,
                                    timeout_ms = self.ack_timeout.as_millis() as u64,
                                    "jetstream ack timeout (no PubAck — check stream binding and broker load), retrying"
                                );
                                last_err = Some(format!(
                                    "ack timeout after {}ms (no PubAck — check stream binding and broker load)",
                                    self.ack_timeout.as_millis()
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            subject = %subject,
                            attempt = attempt + 1,
                            error = %e,
                            "jetstream publish failed, retrying"
                        );
                        last_err = Some(e.to_string());
                    }
                }

                tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
            }

            let reason = last_err.unwrap_or_else(|| "unknown error".to_owned());
            error!(subject = %subject, reason = %reason, "publish failed after all retries");
            Err(NatsError::PublishFailed { subject, reason })
        })
    }

    #[instrument(skip(self, config), fields(stream = %config.name))]
    fn ensure_stream(
        &self,
        config: &StreamConfig,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), NatsError>> + Send + '_>>
    {
        let config = config.clone();
        Box::pin(async move {
            let storage = match config.storage.as_str() {
                "memory" => jetstream::stream::StorageType::Memory,
                _ => jetstream::stream::StorageType::File,
            };

            let retention = match config.retention.as_str() {
                "interest" => jetstream::stream::RetentionPolicy::Interest,
                "workqueue" => jetstream::stream::RetentionPolicy::WorkQueue,
                _ => jetstream::stream::RetentionPolicy::Limits,
            };

            let discard = match config.discard.as_str() {
                "new" => jetstream::stream::DiscardPolicy::New,
                _ => jetstream::stream::DiscardPolicy::Old,
            };

            let stream_config = jetstream::stream::Config {
                name: config.name.clone(),
                subjects: config.subjects.clone(),
                storage,
                retention,
                max_age: Duration::from_secs(config.max_age_secs),
                max_bytes: config.max_bytes,
                max_messages: config.max_msgs,
                max_message_size: config.max_msg_size,
                discard,
                num_replicas: config.num_replicas,
                duplicate_window: Duration::from_secs(config.duplicate_window_secs),
                ..Default::default()
            };

            match self.jetstream.get_stream(&config.name).await {
                Ok(existing) => {
                    // Reconcile subjects: if the existing stream's subjects
                    // drifted from what TOML declares, update in place.
                    // Without this, a stream created by an older version of
                    // the relay (or edited out-of-band) keeps routing only
                    // its old subjects — every new subject hangs with "no
                    // PubAck" until someone fixes it manually.
                    let existing_subjects = &existing.cached_info().config.subjects;
                    if existing_subjects != &config.subjects {
                        info!(
                            stream = %config.name,
                            existing = ?existing_subjects,
                            desired = ?config.subjects,
                            "stream subjects differ from config, reconciling"
                        );
                        self.jetstream
                            .update_stream(stream_config)
                            .await
                            .map_err(|e| NatsError::StreamSetupFailed {
                                stream: config.name.clone(),
                                reason: format!("reconcile subjects: {e}"),
                            })?;
                        info!(stream = %config.name, "stream subjects reconciled");
                    } else {
                        debug!(stream = %config.name, "stream already exists with matching subjects");
                    }
                    Ok(())
                }
                Err(_) => {
                    self.jetstream
                        .create_stream(stream_config)
                        .await
                        .map_err(|e| NatsError::StreamSetupFailed {
                            stream: config.name.clone(),
                            reason: e.to_string(),
                        })?;
                    debug!(stream = %config.name, "stream created");
                    Ok(())
                }
            }
        })
    }

    #[instrument(skip(self, config), fields(consumer = %config.name, stream = %config.stream))]
    fn ensure_consumer(
        &self,
        config: &ConsumerConfig,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), NatsError>> + Send + '_>>
    {
        let config = config.clone();
        Box::pin(async move {
            let stream = self
                .jetstream
                .get_stream(&config.stream)
                .await
                .map_err(|e| NatsError::ConsumerSetupFailed {
                    consumer: config.name.clone(),
                    reason: format!("stream not found: {e}"),
                })?;

            let ack_policy = match config.ack_policy.as_str() {
                "none" => jetstream::consumer::AckPolicy::None,
                "all" => jetstream::consumer::AckPolicy::All,
                _ => jetstream::consumer::AckPolicy::Explicit,
            };

            let deliver_policy = match config.deliver_policy.as_str() {
                "last" => jetstream::consumer::DeliverPolicy::Last,
                "new" => jetstream::consumer::DeliverPolicy::New,
                _ => jetstream::consumer::DeliverPolicy::All,
            };

            let inactive_threshold = if config.inactive_threshold_secs > 0 {
                Duration::from_secs(config.inactive_threshold_secs)
            } else {
                Duration::ZERO
            };

            let consumer_config = jetstream::consumer::pull::Config {
                durable_name: if config.durable {
                    Some(config.name.clone())
                } else {
                    None
                },
                name: Some(config.name.clone()),
                ack_policy,
                ack_wait: Duration::from_secs(config.ack_wait_secs),
                max_deliver: config.max_deliver,
                deliver_policy,
                filter_subject: config.filter_subject.clone().unwrap_or_default(),
                max_ack_pending: config.max_ack_pending,
                inactive_threshold,
                ..Default::default()
            };

            match stream
                .get_consumer::<jetstream::consumer::pull::Config>(&config.name)
                .await
            {
                Ok(_existing) => {
                    debug!(consumer = %config.name, "consumer already exists");
                    Ok(())
                }
                Err(_) => {
                    stream.create_consumer(consumer_config).await.map_err(|e| {
                        NatsError::ConsumerSetupFailed {
                            consumer: config.name.clone(),
                            reason: e.to_string(),
                        }
                    })?;
                    debug!(consumer = %config.name, "consumer created");
                    Ok(())
                }
            }
        })
    }

    fn health_check(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), NatsError>> + Send + '_>>
    {
        Box::pin(async move {
            // Check that we can list streams as a health probe.
            let mut stream_list = self.jetstream.streams();
            // Try to get one item to verify connectivity.
            match stream_list.next().await {
                Some(Ok(_)) | None => Ok(()),
                Some(Err(e)) => Err(NatsError::HealthCheckFailed(e.to_string())),
            }
        })
    }
}
