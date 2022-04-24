use std::io::{Read, Write};
use std::string::FromUtf8Error;
use thiserror::Error;

pub mod api_key;
pub mod primitive;
pub mod record;

#[derive(Error, Debug)]
pub enum SerdeError {
    #[error("Cannot read data: {0}")]
    IO(#[from] std::io::Error),

    #[error("Overflow converting integer: {0}")]
    Overflow(#[from] std::num::TryFromIntError),

    #[error("Malformed data: {0}")]
    Malformed(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub trait Serializer: Sized {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError>;
    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError>;
}

impl From<FromUtf8Error> for SerdeError {
    fn from(err: FromUtf8Error) -> Self {
        SerdeError::Malformed(Box::new(err))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Command{
    Request {
        api_version: i16,
        correlation_id: i32,
        client_id: String,
        body: Request,
    },
    Response {
        correlation_id: i32,
        throttle_time_ms: i32,
        body: Response,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Request {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Response {}