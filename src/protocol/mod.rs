use alloc::string::FromUtf8Error;
use std::io::{Read, Write};
use thiserror::Error;

pub mod api_key;
pub mod primitive;

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
