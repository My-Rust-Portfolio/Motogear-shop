use std::{future::Future, io, pin::Pin};

/// Single-shot Ctrl+C future. Once `wait` resolves, the worker should exit.
pub struct ShutdownSignal {
    signal: Pin<Box<dyn Future<Output = io::Result<()>> + Send>>,
}

impl ShutdownSignal {
    pub fn new() -> Self {
        Self {
            signal: Box::pin(tokio::signal::ctrl_c()),
        }
    }

    /// Waits for the one shutdown signal; do not poll it again after it resolves.
    pub async fn wait(&mut self) -> io::Result<()> {
        self.signal.as_mut().await
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}
