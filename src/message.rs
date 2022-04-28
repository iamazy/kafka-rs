use crate::error::ConnectionError;
use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};

pub struct KafkaCodec;

#[cfg(feature = "tokio-runtime")]
impl tokio_util::codec::Encoder<Command> for KafkaCodec {
    type Error = ConnectionError;

    fn encode(&mut self, cmd: Command, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if let Command::Request(request) = cmd {
            let api_version = request.header.request_api_version;
            let mut bytes = BytesMut::new();
            match request.body {
                RequestKind::ProduceRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, ProduceRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::FetchRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, FetchRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::ListOffsetsRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, ListOffsetsRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::MetadataRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, MetadataRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::LeaderAndIsrRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, LeaderAndIsrRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::StopReplicaRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, StopReplicaRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::UpdateMetadataRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        UpdateMetadataRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::ControlledShutdownRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        ControlledShutdownRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::OffsetCommitRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, OffsetCommitRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::OffsetFetchRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, OffsetFetchRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::FindCoordinatorRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        FindCoordinatorRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::JoinGroupRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, JoinGroupRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::HeartbeatRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, HeartbeatRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::LeaveGroupRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, LeaveGroupRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::SyncGroupRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, SyncGroupRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeGroupsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DescribeGroupsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::ListGroupsRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, ListGroupsRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::SaslHandshakeRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        SaslHandshakeRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::ApiVersionsRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, ApiVersionsRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::CreateTopicsRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, CreateTopicsRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DeleteTopicsRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, DeleteTopicsRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DeleteRecordsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DeleteRecordsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::InitProducerIdRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        InitProducerIdRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::OffsetForLeaderEpochRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        OffsetForLeaderEpochRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::AddPartitionsToTxnRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        AddPartitionsToTxnRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::AddOffsetsToTxnRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        AddOffsetsToTxnRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::EndTxnRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, EndTxnRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::WriteTxnMarkersRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        WriteTxnMarkersRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::TxnOffsetCommitRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        TxnOffsetCommitRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeAclsRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, DescribeAclsRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::CreateAclsRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, CreateAclsRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DeleteAclsRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, DeleteAclsRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeConfigsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DescribeConfigsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::AlterConfigsRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, AlterConfigsRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::AlterReplicaLogDirsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        AlterReplicaLogDirsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeLogDirsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DescribeLogDirsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::SaslAuthenticateRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        SaslAuthenticateRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::CreatePartitionsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        CreatePartitionsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::CreateDelegationTokenRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        CreateDelegationTokenRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::RenewDelegationTokenRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        RenewDelegationTokenRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::ExpireDelegationTokenRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        ExpireDelegationTokenRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeDelegationTokenRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DescribeDelegationTokenRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DeleteGroupsRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, DeleteGroupsRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::ElectLeadersRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, ElectLeadersRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::IncrementalAlterConfigsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        IncrementalAlterConfigsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::AlterPartitionReassignmentsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        AlterPartitionReassignmentsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::ListPartitionReassignmentsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        ListPartitionReassignmentsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::OffsetDeleteRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, OffsetDeleteRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeClientQuotasRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DescribeClientQuotasRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::AlterClientQuotasRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        AlterClientQuotasRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeUserScramCredentialsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DescribeUserScramCredentialsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::AlterUserScramCredentialsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        AlterUserScramCredentialsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::UpdateFeaturesRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        UpdateFeaturesRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeClusterRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DescribeClusterRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeProducersRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DescribeProducersRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeTransactionsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DescribeTransactionsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::ListTransactionsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        ListTransactionsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::AllocateProducerIdsRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        AllocateProducerIdsRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::VoteRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, VoteRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::BeginQuorumEpochRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        BeginQuorumEpochRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::EndQuorumEpochRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        EndQuorumEpochRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::DescribeQuorumRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        DescribeQuorumRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::AlterPartitionRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        AlterPartitionRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::EnvelopeRequest(req) => {
                    request
                        .header
                        .encode(&mut bytes, EnvelopeRequest::header_version(api_version))?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::FetchSnapshotRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        FetchSnapshotRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::BrokerRegistrationRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        BrokerRegistrationRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::BrokerHeartbeatRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        BrokerHeartbeatRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
                RequestKind::UnregisterBrokerRequest(req) => {
                    request.header.encode(
                        &mut bytes,
                        UnregisterBrokerRequest::header_version(api_version),
                    )?;
                    req.encode(&mut bytes, api_version)?;
                }
            }
            dst.put_u32(bytes.len() as u32);
            if dst.remaining_mut() < bytes.len() {
                dst.reserve(bytes.len());
            }
            dst.put_slice(&bytes);
        }
        Ok(())
    }
}

#[cfg(feature = "tokio-runtime")]
impl tokio_util::codec::Decoder for KafkaCodec {
    type Item = Command;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() >= 4 {
            let _ = src.get_u32();
            let header = ResponseHeader::decode(src, 1)?;
            return Ok(Some(Command::Response(Response {
                header,
                body: src.to_vec(),
            })));
        }
        Ok(None)
    }
}

pub struct Request {
    pub header: RequestHeader,
    pub body: RequestKind,
}

pub struct Response {
    pub header: ResponseHeader,
    pub body: Vec<u8>,
}

pub enum Command {
    Request(Request),
    Response(Response),
}

impl Command {
    pub fn is_request(&self) -> bool {
        match self {
            Command::Request(_) => true,
            _ => false,
        }
    }

    pub fn correlation_id(&self) -> i32 {
        match self {
            Command::Request(req) => req.header.correlation_id,
            Command::Response(res) => res.header.correlation_id,
        }
    }
}
