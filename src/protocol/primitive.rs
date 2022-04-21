use crate::protocol::{SerdeError, Serializer};
use integer_encoding::{VarIntReader, VarIntWriter};
use std::io::{Read, Write};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Boolean(pub bool);

impl Serializer for Boolean {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        match buf[0] {
            0 => Ok(Boolean(false)),
            _ => Ok(Boolean(true)),
        }
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        match self.0 {
            true => Ok(writer.write_all(&[1u8])?),
            false => Ok(writer.write_all(&[0u8])?),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Int8(pub i8);

impl Serializer for Int8 {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(Self(i8::from_be_bytes(buf)))
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Int16(pub i16);

impl Serializer for Int16 {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(Self(i16::from_be_bytes(buf)))
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Int32(pub i32);

impl Serializer for Int32 {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(Self(i32::from_be_bytes(buf)))
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Int64(pub i64);

impl Serializer for Int64 {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(Self(i64::from_be_bytes(buf)))
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct VarInt(pub i32);

impl Serializer for VarInt {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let i: i64 = reader.read_varint()?;
        Ok(Self(i32::try_from(i)?))
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        writer.write_varint(self.0)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct VarLong(pub i64);

impl Serializer for VarLong {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        Ok(Self(reader.read_varint()?))
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        writer.write_varint(self.0)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct UnsignedVarInt(pub u64);

impl Serializer for UnsignedVarInt {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let mut buf = [0u8; 1];
        let mut res = 0u64;
        let mut shift = 0;
        loop {
            reader.read_exact(&mut buf)?;
            let c: u64 = buf[0].into();
            res |= (c & 0x7f) << shift;
            shift += 7;
            if c & 0x80 == 0 {
                break;
            }
            if shift > 63 {
                return Err(SerdeError::Malformed(
                    String::from("Overflow while reading unsigned varint.").into(),
                ));
            }
        }
        Ok(Self(res))
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        let mut cur = self.0;
        loop {
            let mut c = u8::try_from(cur & 0x7f).map_err(SerdeError::Overflow)?;
            cur >>= 7;
            if cur != 0 {
                c |= 0x80;
            }
            writer.write_all(&[c])?;
            if cur == 0 {
                break;
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct NullableString(pub Option<String>);

impl Serializer for NullableString {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let len = Int16::decode(reader)?;
        match len.0 {
            l if l < -1 => Err(SerdeError::Malformed(
                String::from("Nullable string length is negative.").into(),
            )),
            -1 => Ok(Self(None)),
            l => {
                let mut buf = vec![0u8; l as usize];
                reader.read_exact(&mut buf)?;
                Ok(Self(Some(String::from_utf8(buf)?)))
            }
        }
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        match self.0 {
            Some(ref s) => {
                let len = i16::try_from(s.len()).map_err(|e| SerdeError::Malformed(e.into()))?;
                Int16(len).encode(writer)?;
                writer.write_all(s.as_bytes())?;
                Ok(())
            }
            None => Int16(-1).encode(writer),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct String_(pub String);

impl Serializer for String_ {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let len = Int16::decode(reader)?;
        let len = usize::try_from(len.0).map_err(|e| SerdeError::Malformed(e.into()))?;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        Ok(Self(String::from_utf8(buf)?))
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        let len = i16::try_from(self.0.len()).map_err(SerdeError::Overflow)?;
        Int16(len).encode(writer)?;
        writer.write_all(self.0.as_bytes())?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CompactString(pub String);

impl Serializer for CompactString {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let len = UnsignedVarInt::decode(reader)?;
        match len.0 {
            0 => Err(SerdeError::Malformed(
               "Compact string length is zero.".into(),
            )),
            len => {
                let len = usize::try_from(len)?;
                let len = len - 1;
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;
                Ok(Self(String::from_utf8(buf)?))
            }
        }
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        let len = u64::try_from(self.0.len() + 1).map_err(SerdeError::Overflow)?;
        UnsignedVarInt(len).encode(writer)?;
        writer.write_all(self.0.as_bytes())?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CompactNullableString(pub Option<String>);

impl Serializer for CompactNullableString {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let len = UnsignedVarInt::decode(reader)?;
        match len.0 {
            0 => Ok(Self(None)),
            len => {
                let len = usize::try_from(len)?;
                let len = len - 1;
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;
                Ok(Self(Some(String::from_utf8(buf)?)))
            }
        }
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        match self.0 {
            Some(ref s) => {
                let len = u64::try_from(s.len() + 1).map_err(SerdeError::Overflow)?;
                UnsignedVarInt(len).encode(writer)?;
                writer.write_all(s.as_bytes())?;
                Ok(())
            }
            None => UnsignedVarInt(0).encode(writer),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct NullableBytes(pub Option<Vec<u8>>);

impl Serializer for NullableBytes {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let len = Int32::decode(reader)?;
        match len.0 {
            l if l < -1 => Err(SerdeError::Malformed(
                String::from("Nullable bytes length is negative.").into(),
            )),
            -1 => Ok(Self(None)),
            l => {
                let len = usize::try_from(l)?;
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;
                Ok(Self(Some(buf)))
            }
        }
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        match self.0 {
            Some(ref s) => {
                let len = i32::try_from(s.len()).map_err(|e| SerdeError::Malformed(e.into()))?;
                Int32(len).encode(writer)?;
                writer.write_all(s)?;
                Ok(())
            }
            None => Int32(-1).encode(writer),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TaggedFields(pub Vec<(UnsignedVarInt, Vec<u8>)>);

impl Serializer for TaggedFields {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let len = UnsignedVarInt::decode(reader)?;
        let len = usize::try_from(len.0).map_err(SerdeError::Overflow)?;
        let mut fields = Vec::with_capacity(len);
        for _ in 0..len {
            let tag = UnsignedVarInt::decode(reader)?;
            let len = UnsignedVarInt::decode(reader)?;
            let len = usize::try_from(len.0).map_err(SerdeError::Overflow)?;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            fields.push((tag, buf));
        }
        Ok(Self(fields))
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        let len = u64::try_from(self.0.len()).map_err(SerdeError::Overflow)?;
        UnsignedVarInt(len).encode(writer)?;
        for (tag, buf) in self.0.iter() {
            tag.encode(writer)?;
            let len = u64::try_from(buf.len()).map_err(SerdeError::Overflow)?;
            UnsignedVarInt(len).encode(writer)?;
            writer.write_all(buf)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct Array<T: Serializer>(pub Option<Vec<T>>);

impl<T: Serializer> Serializer for Array<T> {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let len = Int32::decode(reader)?;
        if len.0 == - 1 {
            Ok(Self(None))
        } else {
            let len = usize::try_from(len.0).map_err(SerdeError::Overflow)?;
            let mut array = Vec::with_capacity(len);
            for _ in 0..len {
                array.push(T::decode(reader)?);
            }
            Ok(Self(Some(array)))
        }
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        match self.0 {
            None => Int32(-1).encode(writer),
            Some(ref array) => {
                let len = i32::try_from(array.len()).map_err(|e| SerdeError::Malformed(e.into()))?;
                Int32(len).encode(writer)?;
                for item in array.iter() {
                    item.encode(writer)?;
                }
                Ok(())
            }
        }
    }
}