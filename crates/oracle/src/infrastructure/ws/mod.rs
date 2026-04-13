/// WebSocket server for fan-out of oracle prices to connected clients.
mod server;

pub use server::OracleWsServer;
