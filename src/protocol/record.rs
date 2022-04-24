use std::io::{Read, Write};
use crate::protocol::{SerdeError, Serializer};
use crate::protocol::primitive::{Int8, VarInt, VarLong};

#[derive(Debug, PartialEq, Eq)]
pub struct RecordHeader {
    pub key: String,
    pub value: Vec<u8>,
}

impl Serializer for RecordHeader {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let len = VarInt::decode(reader)?;
        let len = usize::try_from(len.0).map_err(|e| SerdeError::Malformed(Box::new(e)))?;
        let mut buf = vec![0; len];
        reader.read_exact(&mut buf)?;
        let key = String::from_utf8(buf)?;

        let len = VarInt::decode(reader)?;
        let len = usize::try_from(len.0).map_err(|e| SerdeError::Malformed(Box::new(e)))?;
        let mut value = vec![0; len];
        reader.read_exact(&mut value)?;
        Ok(Self {
            key,
            value,
        })

    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        let len = i32::try_from(self.key.len()).map_err(|e| SerdeError::Malformed(Box::new(e)))?;
        VarInt(len).encode( writer)?;
        writer.write_all(self.key.as_bytes())?;

        let len = i32::try_from(self.value.len()).map_err(|e| SerdeError::Malformed(Box::new(e)))?;
        VarInt(len).encode( writer)?;
        writer.write_all(&self.value)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub header: Vec<RecordHeader>
}

impl Serializer for Record {
    fn decode(reader: &mut impl Read) -> Result<Self, SerdeError> {
        let len = VarInt::decode(reader)?;
        let len = u64::try_from(len.0).map_err(|e| SerdeError::Malformed(Box::new(e)))?;
        let reader = &mut reader.take(len);
        // attributes
        Int8::decode(reader)?;
        let timestamp_delta = VarLong::decode(reader)?.0;
        let offset_delta = VarInt::decode(reader)?.0;
        todo!()


    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), SerdeError> {
        todo!()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ControlRecord {
    Abort,
    Commit,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Records {
    ControlRecord(ControlRecord),
    Records(Vec<Record>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub compression: Compression,
    pub is_transactional: bool,
    pub timestamp_type: TimestampType,
    pub records: Records,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    CreateTime,
    LogAppendTime
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd
}