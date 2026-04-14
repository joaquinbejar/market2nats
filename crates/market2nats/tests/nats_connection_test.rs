//! Integration tests for NATS connection and JetStream stream/consumer setup.
//!
//! Requires a running NATS server with JetStream enabled.
//! Start one with: `docker compose up -d`

use std::time::Duration;

use async_nats::jetstream;
use futures_util::StreamExt;

use market2nats::application::ports::NatsPublisher;

async fn connect_nats() -> async_nats::Client {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_owned());
    async_nats::ConnectOptions::new()
        .connection_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
        .unwrap_or_else(|e| {
            panic!("cannot connect to NATS at {url}: {e}. Is NATS running? Try: docker compose -f Docker/docker-compose.yml up -d nats")
        })
}

fn unique_stream_name(prefix: &str) -> String {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    format!("{prefix}_{id}")
}

fn unique_subject_prefix() -> String {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    format!("test_{id}")
}
use market2nats::config::model::{ConsumerConfig, StreamConfig};
use market2nats::infrastructure::nats::JetStreamPublisher;

/// Test that we can connect to NATS.
#[tokio::test]
#[ignore]
async fn test_nats_connection() {
    let client = connect_nats().await;
    assert_eq!(
        client.connection_state(),
        async_nats::connection::State::Connected
    );
}

/// Test that JetStream context is available.
#[tokio::test]
#[ignore]
async fn test_jetstream_available() {
    let client = connect_nats().await;
    let js = jetstream::new(client);

    // Account info should be available if JetStream is enabled.
    let info = js.query_account().await;
    assert!(info.is_ok(), "JetStream not available: {:?}", info.err());
}

/// Test creating a JetStream stream via our publisher.
#[tokio::test]
#[ignore]
async fn test_create_stream() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client, std::time::Duration::from_secs(10));
    let stream_name = unique_stream_name("TEST_STREAM");

    let config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![format!("{stream_name}.>")],
        storage: "memory".to_owned(),
        retention: "limits".to_owned(),
        max_age_secs: 60,
        max_bytes: 0,
        max_msgs: 0,
        max_msg_size: 65536,
        discard: "old".to_owned(),
        num_replicas: 1,
        duplicate_window_secs: 10,
    };

    let result = publisher.ensure_stream(&config).await;
    assert!(result.is_ok(), "stream creation failed: {:?}", result.err());

    // Verify the stream exists.
    let js = publisher.jetstream_context();
    let stream = js.get_stream(&stream_name).await;
    assert!(
        stream.is_ok(),
        "stream not found after creation: {:?}",
        stream.err()
    );

    // Cleanup.
    let _ = js.delete_stream(&stream_name).await;
}

/// Test creating a stream is idempotent (calling ensure_stream twice doesn't error).
#[tokio::test]
#[ignore]
async fn test_create_stream_idempotent() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client, std::time::Duration::from_secs(10));
    let stream_name = unique_stream_name("TEST_IDEMPOTENT");

    let config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![format!("{stream_name}.>")],
        storage: "memory".to_owned(),
        retention: "limits".to_owned(),
        max_age_secs: 60,
        max_bytes: 0,
        max_msgs: 0,
        max_msg_size: 65536,
        discard: "old".to_owned(),
        num_replicas: 1,
        duplicate_window_secs: 10,
    };

    publisher.ensure_stream(&config).await.unwrap();
    // Second call should not error.
    let result = publisher.ensure_stream(&config).await;
    assert!(
        result.is_ok(),
        "idempotent stream creation failed: {:?}",
        result.err()
    );

    // Cleanup.
    let js = publisher.jetstream_context();
    let _ = js.delete_stream(&stream_name).await;
}

/// Test creating a consumer on a stream.
#[tokio::test]
#[ignore]
async fn test_create_consumer() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client, std::time::Duration::from_secs(10));
    let stream_name = unique_stream_name("TEST_CONSUMER_STREAM");

    // Create stream first.
    let stream_config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![format!("{stream_name}.>")],
        storage: "memory".to_owned(),
        retention: "limits".to_owned(),
        max_age_secs: 60,
        max_bytes: 0,
        max_msgs: 0,
        max_msg_size: 65536,
        discard: "old".to_owned(),
        num_replicas: 1,
        duplicate_window_secs: 10,
    };
    publisher.ensure_stream(&stream_config).await.unwrap();

    // Create consumer.
    let consumer_name = format!("{stream_name}_consumer");
    let consumer_config = ConsumerConfig {
        stream: stream_name.clone(),
        name: consumer_name.clone(),
        durable: true,
        ack_policy: "explicit".to_owned(),
        ack_wait_secs: 30,
        max_deliver: 5,
        filter_subject: None,
        deliver_policy: "all".to_owned(),
        start_time: None,
        max_ack_pending: 1000,
        inactive_threshold_secs: 0,
    };

    let result = publisher.ensure_consumer(&consumer_config).await;
    assert!(
        result.is_ok(),
        "consumer creation failed: {:?}",
        result.err()
    );

    // Verify consumer exists.
    let js = publisher.jetstream_context();
    let stream = js.get_stream(&stream_name).await.unwrap();
    let consumer = stream
        .get_consumer::<jetstream::consumer::pull::Config>(&consumer_name)
        .await;
    assert!(
        consumer.is_ok(),
        "consumer not found after creation: {:?}",
        consumer.err()
    );

    // Cleanup.
    let _ = js.delete_stream(&stream_name).await;
}

/// Test consumer creation fails when stream doesn't exist.
#[tokio::test]
#[ignore]
async fn test_create_consumer_missing_stream() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client, std::time::Duration::from_secs(10));

    let consumer_config = ConsumerConfig {
        stream: "NONEXISTENT_STREAM_12345".to_owned(),
        name: "orphan_consumer".to_owned(),
        durable: true,
        ack_policy: "explicit".to_owned(),
        ack_wait_secs: 30,
        max_deliver: 5,
        filter_subject: None,
        deliver_policy: "all".to_owned(),
        start_time: None,
        max_ack_pending: 1000,
        inactive_threshold_secs: 0,
    };

    let result = publisher.ensure_consumer(&consumer_config).await;
    assert!(result.is_err(), "should fail when stream doesn't exist");
}

/// Test health check returns Ok when NATS is up.
#[tokio::test]
#[ignore]
async fn test_health_check_ok() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client, std::time::Duration::from_secs(10));

    let result = publisher.health_check().await;
    assert!(result.is_ok(), "health check failed: {:?}", result.err());
}

/// Test creating multiple streams in sequence (simulates startup).
#[tokio::test]
#[ignore]
async fn test_setup_multiple_streams() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client, std::time::Duration::from_secs(10));
    let prefix = unique_subject_prefix();

    let stream_configs = vec![
        StreamConfig {
            name: format!("{prefix}_TRADES"),
            subjects: vec![format!("{prefix}.*.*.trade")],
            storage: "memory".to_owned(),
            retention: "limits".to_owned(),
            max_age_secs: 60,
            max_bytes: 0,
            max_msgs: 0,
            max_msg_size: 65536,
            discard: "old".to_owned(),
            num_replicas: 1,
            duplicate_window_secs: 10,
        },
        StreamConfig {
            name: format!("{prefix}_TICKER"),
            subjects: vec![format!("{prefix}.*.*.ticker")],
            storage: "memory".to_owned(),
            retention: "limits".to_owned(),
            max_age_secs: 30,
            max_bytes: 0,
            max_msgs: 0,
            max_msg_size: 65536,
            discard: "old".to_owned(),
            num_replicas: 1,
            duplicate_window_secs: 10,
        },
        StreamConfig {
            name: format!("{prefix}_ORDERBOOK"),
            subjects: vec![format!("{prefix}.*.*.l2_orderbook")],
            storage: "memory".to_owned(),
            retention: "limits".to_owned(),
            max_age_secs: 30,
            max_bytes: 0,
            max_msgs: 0,
            max_msg_size: 65536,
            discard: "old".to_owned(),
            num_replicas: 1,
            duplicate_window_secs: 10,
        },
    ];

    for config in &stream_configs {
        publisher.ensure_stream(config).await.unwrap();
    }

    // Verify all streams.
    let js = publisher.jetstream_context();
    let mut stream_list = js.streams();
    let mut found = Vec::new();
    while let Some(Ok(stream)) = stream_list.next().await {
        if stream.config.name.starts_with(&prefix) {
            found.push(stream.config.name.clone());
        }
    }
    assert_eq!(found.len(), 3, "expected 3 streams, found: {found:?}");

    // Cleanup.
    for config in &stream_configs {
        let _ = js.delete_stream(&config.name).await;
    }
}

/// Test stream with different retention policies.
#[tokio::test]
#[ignore]
async fn test_stream_retention_policies() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client, std::time::Duration::from_secs(10));
    let js = publisher.jetstream_context();

    for (retention, storage) in [
        ("limits", "memory"),
        ("interest", "memory"),
        ("workqueue", "memory"),
        ("limits", "file"),
    ] {
        let stream_name = unique_stream_name(&format!("TEST_{retention}_{storage}"));
        let config = StreamConfig {
            name: stream_name.clone(),
            subjects: vec![format!("{stream_name}.>")],
            storage: storage.to_owned(),
            retention: retention.to_owned(),
            max_age_secs: 60,
            max_bytes: 0,
            max_msgs: 0,
            max_msg_size: 65536,
            discard: "old".to_owned(),
            num_replicas: 1,
            duplicate_window_secs: 10,
        };

        let result = publisher.ensure_stream(&config).await;
        assert!(
            result.is_ok(),
            "stream creation with retention={retention}, storage={storage} failed: {:?}",
            result.err()
        );

        let _ = js.delete_stream(&stream_name).await;
    }
}

/// Test consumer with different ack policies.
#[tokio::test]
#[ignore]
async fn test_consumer_ack_policies() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client, std::time::Duration::from_secs(10));
    let js = publisher.jetstream_context();
    let stream_name = unique_stream_name("TEST_ACK_POLICIES");

    let stream_config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![format!("{stream_name}.>")],
        storage: "memory".to_owned(),
        retention: "limits".to_owned(),
        max_age_secs: 60,
        max_bytes: 0,
        max_msgs: 0,
        max_msg_size: 65536,
        discard: "old".to_owned(),
        num_replicas: 1,
        duplicate_window_secs: 10,
    };
    publisher.ensure_stream(&stream_config).await.unwrap();

    for ack_policy in ["none", "all", "explicit"] {
        let consumer_name = format!("{stream_name}_{ack_policy}");
        let consumer_config = ConsumerConfig {
            stream: stream_name.clone(),
            name: consumer_name.clone(),
            durable: true,
            ack_policy: ack_policy.to_owned(),
            ack_wait_secs: 30,
            max_deliver: 3,
            filter_subject: None,
            deliver_policy: "all".to_owned(),
            start_time: None,
            max_ack_pending: 100,
            inactive_threshold_secs: 0,
        };

        let result = publisher.ensure_consumer(&consumer_config).await;
        assert!(
            result.is_ok(),
            "consumer with ack_policy={ack_policy} failed: {:?}",
            result.err()
        );
    }

    let _ = js.delete_stream(&stream_name).await;
}
