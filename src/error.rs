use kafka_protocol::protocol::{DecodeError, EncodeError};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum Error {
    Connection(ConnectionError),
}

#[derive(Debug)]
pub enum ConnectionError {
    Io(io::Error),
    Disconnected,
    Unexpected(String),
    Decoding(String),
    Encoding(String),
    SocketAddr(String),
    UnexpectedResponse(String),
    Tls(native_tls::Error),
    NotFound,
    Canceled,
    Shutdown,
}

impl From<EncodeError> for ConnectionError {
    fn from(_: EncodeError) -> Self {
        ConnectionError::Encoding("encode error".into())
    }
}

impl From<DecodeError> for ConnectionError {
    fn from(_: DecodeError) -> Self {
        ConnectionError::Decoding("decode error".into())
    }
}

impl From<std::io::Error> for ConnectionError {
    fn from(e: std::io::Error) -> Self {
        ConnectionError::Io(e)
    }
}

#[derive(Clone)]
pub struct SharedError {
    error_set: Arc<AtomicBool>,
    error: Arc<Mutex<Option<ConnectionError>>>,
}

impl SharedError {
    pub fn new() -> SharedError {
        SharedError {
            error_set: Arc::new(AtomicBool::new(false)),
            error: Arc::new(Mutex::new(None)),
        }
    }

    pub fn is_set(&self) -> bool {
        self.error_set.load(Ordering::Relaxed)
    }

    pub fn remove(&self) -> Option<ConnectionError> {
        let mut lock = self.error.lock().unwrap();
        let error = lock.take();
        self.error_set.store(false, Ordering::Release);
        error
    }

    pub fn set(&self, error: ConnectionError) {
        let mut lock = self.error.lock().unwrap();
        *lock = Some(error);
        self.error_set.store(true, Ordering::Release);
    }
}

impl Default for SharedError {
    fn default() -> Self {
        Self::new()
    }
}