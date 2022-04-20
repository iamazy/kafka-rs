use std::io::{Read, Write};
use integer_encoding::{VarIntReader, VarIntWriter};
use crate::protocol::{SerdeError, Serializer};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Boolean(pub bool);

impl<R: Read, W: Write> Serializer<R, W> for Boolean {
    fn decode(reader: &mut R) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        match buf[0] {
            0 => Ok(Boolean(false)),
            _ => Ok(Boolean(true)),
        }
    }

    fn encode(&self, writer: &mut W) -> Result<(), SerdeError> {
        match self.0 {
            true => Ok(writer.write_all(&[1u8])?),
            false => Ok(writer.write_all(&[0u8])?),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Int8(pub i8);

impl<R: Read, W: Write> Serializer<R, W> for Int8 {
    fn decode(reader: &mut R) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(Self(i8::from_be_bytes(buf)))
    }

    fn encode(&self, writer: &mut W) -> Result<(), SerdeError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Int16(pub i16);

impl<R: Read, W: Write> Serializer<R, W> for Int16 {
    fn decode(reader: &mut R) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(Self(i16::from_be_bytes(buf)))
    }

    fn encode(&self, writer: &mut W) -> Result<(), SerdeError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Int32(pub i32);

impl<R: Read, W: Write> Serializer<R, W> for Int32 {
    fn decode(reader: &mut R) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(Self(i32::from_be_bytes(buf)))
    }

    fn encode(&self, writer: &mut W) -> Result<(), SerdeError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Int64(pub i64);

impl<R: Read, W: Write> Serializer<R, W> for Int64 {
    fn decode(reader: &mut R) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(Self(i64::from_be_bytes(buf)))
    }

    fn encode(&self, writer: &mut W) -> Result<(), SerdeError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Varint(pub i32);

impl<R: Read, W: Write> Serializer<R, W> for Varint {
    fn decode(reader: &mut R) -> Result<Self, SerdeError> {
        let i: i64 = reader.read_varint()?;
        Ok(Self(i32::try_from(i)?))
    }

    fn encode(&self, writer: &mut W) -> Result<(), SerdeError> {
        writer.write_varint(self.0)?;
        Ok(())
    }
}