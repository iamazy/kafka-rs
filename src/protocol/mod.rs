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

pub trait Serializer<R: Read, W: Write>: Sized {
    fn decode(reader :&mut R) -> Result<Self, SerdeError>;
    fn encode(&self, writer: &mut W) -> Result<(), SerdeError>;
}