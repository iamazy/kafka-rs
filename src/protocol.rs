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
            return Ok(Some(Command::Response(KafkaResponse::new(header, src.to_vec()))));
        }
        Ok(None)
    }
}

pub struct KafkaRequest {
    pub header: RequestHeader,
    pub body: RequestKind,
}

pub struct KafkaResponse {
    pub header: ResponseHeader,
    pub raw_body: Vec<u8>,
    pub body: Option<ResponseKind>,
}

impl KafkaResponse {

    pub fn new(header: ResponseHeader, body: Vec<u8>) -> Self {
        Self {
            header,
            raw_body: body,
            body: None,
        }
    }

    pub fn fill_body(&mut self, api_key: i16, version: i16) -> Result<(), ConnectionError> {
        if let Ok(key) = api_key.try_into() {
            match key {
                ApiKey::ProduceKey => {
                    let producer = ProduceResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::ProduceResponse(producer));
                }
                ApiKey::FetchKey => {
                    let fetch = FetchResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::FetchResponse(fetch));
                }
                ApiKey::ListOffsetsKey => {
                    let list_offsets = ListOffsetsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::ListOffsetsResponse(list_offsets));
                }
                ApiKey::MetadataKey => {
                    let metadata = MetadataResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::MetadataResponse(metadata));
                }
                ApiKey::LeaderAndIsrKey => {
                    let leader_and_isr = LeaderAndIsrResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::LeaderAndIsrResponse(leader_and_isr));
                }
                ApiKey::StopReplicaKey => {
                    let stop_replica = StopReplicaResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::StopReplicaResponse(stop_replica));
                }
                ApiKey::UpdateMetadataKey => {
                    let update_metadata = UpdateMetadataResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::UpdateMetadataResponse(update_metadata));
                }
                ApiKey::ControlledShutdownKey => {
                    let controlled_shutdown = ControlledShutdownResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::ControlledShutdownResponse(controlled_shutdown));
                }
                ApiKey::OffsetCommitKey => {
                    let offset_commit = OffsetCommitResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::OffsetCommitResponse(offset_commit));
                }
                ApiKey::OffsetFetchKey => {
                    let offset_fetch = OffsetFetchResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::OffsetFetchResponse(offset_fetch));
                }
                ApiKey::FindCoordinatorKey => {
                    let find_coordinator = FindCoordinatorResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::FindCoordinatorResponse(find_coordinator));
                }
                ApiKey::JoinGroupKey => {
                    let join_group = JoinGroupResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::JoinGroupResponse(join_group));
                }
                ApiKey::HeartbeatKey => {
                    let heartbeat = HeartbeatResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::HeartbeatResponse(heartbeat));
                }
                ApiKey::LeaveGroupKey => {
                    let leave_group = LeaveGroupResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::LeaveGroupResponse(leave_group));
                }
                ApiKey::SyncGroupKey => {
                    let sync_group = SyncGroupResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::SyncGroupResponse(sync_group));
                }
                ApiKey::DescribeGroupsKey => {
                    let describe_groups = DescribeGroupsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeGroupsResponse(describe_groups));
                }
                ApiKey::ListGroupsKey => {
                    let list_groups = ListGroupsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::ListGroupsResponse(list_groups));
                }
                ApiKey::SaslHandshakeKey => {
                    let sasl_handshake = SaslHandshakeResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::SaslHandshakeResponse(sasl_handshake));
                }
                ApiKey::ApiVersionsKey => {
                    let api_versions = ApiVersionsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::ApiVersionsResponse(api_versions));
                }
                ApiKey::CreateTopicsKey => {
                    let create_topics = CreateTopicsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::CreateTopicsResponse(create_topics));
                }
                ApiKey::DeleteTopicsKey => {
                    let delete_topics = DeleteTopicsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DeleteTopicsResponse(delete_topics));
                }
                ApiKey::DeleteRecordsKey => {
                    let delete_records = DeleteRecordsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DeleteRecordsResponse(delete_records));
                }
                ApiKey::InitProducerIdKey => {
                    let init_producer_id = InitProducerIdResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::InitProducerIdResponse(init_producer_id));
                }
                ApiKey::OffsetForLeaderEpochKey => {
                    let offset_for_leader_epoch = OffsetForLeaderEpochResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::OffsetForLeaderEpochResponse(offset_for_leader_epoch));
                }
                ApiKey::AddPartitionsToTxnKey => {
                    let add_partitions_to_txn = AddPartitionsToTxnResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::AddPartitionsToTxnResponse(add_partitions_to_txn));
                }
                ApiKey::AddOffsetsToTxnKey => {
                    let add_offsets_to_txn = AddOffsetsToTxnResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::AddOffsetsToTxnResponse(add_offsets_to_txn));
                }
                ApiKey::EndTxnKey => {
                    let end_txn = EndTxnResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::EndTxnResponse(end_txn));
                }
                ApiKey::WriteTxnMarkersKey => {
                    let write_txn_markers = WriteTxnMarkersResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::WriteTxnMarkersResponse(write_txn_markers));
                }
                ApiKey::TxnOffsetCommitKey => {
                    let txn_offset_commit = TxnOffsetCommitResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::TxnOffsetCommitResponse(txn_offset_commit));
                }
                ApiKey::DescribeAclsKey => {
                    let describe_acls = DescribeAclsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeAclsResponse(describe_acls));
                }
                ApiKey::CreateAclsKey => {
                    let create_acls = CreateAclsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::CreateAclsResponse(create_acls));
                }
                ApiKey::DeleteAclsKey => {
                    let delete_acls = DeleteAclsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DeleteAclsResponse(delete_acls));
                }
                ApiKey::DescribeConfigsKey => {
                    let describe_configs = DescribeConfigsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeConfigsResponse(describe_configs));
                }
                ApiKey::AlterConfigsKey => {
                    let alter_configs = AlterConfigsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::AlterConfigsResponse(alter_configs));
                }
                ApiKey::AlterReplicaLogDirsKey => {
                    let alter_replica_log_dirs = AlterReplicaLogDirsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::AlterReplicaLogDirsResponse(alter_replica_log_dirs));
                }
                ApiKey::DescribeLogDirsKey => {
                    let describe_log_dirs = DescribeLogDirsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeLogDirsResponse(describe_log_dirs));
                }
                ApiKey::SaslAuthenticateKey => {
                    let sasl_authenticate = SaslAuthenticateResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::SaslAuthenticateResponse(sasl_authenticate));
                }
                ApiKey::CreatePartitionsKey => {
                    let create_partitions = CreatePartitionsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::CreatePartitionsResponse(create_partitions));
                }
                ApiKey::CreateDelegationTokenKey => {
                    let create_delegation_token = CreateDelegationTokenResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::CreateDelegationTokenResponse(create_delegation_token));
                }
                ApiKey::RenewDelegationTokenKey => {
                    let renew_delegation_token = RenewDelegationTokenResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::RenewDelegationTokenResponse(renew_delegation_token));
                }
                ApiKey::ExpireDelegationTokenKey => {
                    let expire_delegation_token = ExpireDelegationTokenResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::ExpireDelegationTokenResponse(expire_delegation_token));
                }
                ApiKey::DescribeDelegationTokenKey => {
                    let describe_delegation_token = DescribeDelegationTokenResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeDelegationTokenResponse(describe_delegation_token));
                }
                ApiKey::DeleteGroupsKey => {
                    let delete_groups = DeleteGroupsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DeleteGroupsResponse(delete_groups));
                }
                ApiKey::ElectLeadersKey => {
                    let elect_leaders = ElectLeadersResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::ElectLeadersResponse(elect_leaders));
                }
                ApiKey::IncrementalAlterConfigsKey => {
                    let incremental_alter_configs = IncrementalAlterConfigsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::IncrementalAlterConfigsResponse(incremental_alter_configs));
                }
                ApiKey::AlterPartitionReassignmentsKey => {
                    let alter_partition_reassignments = AlterPartitionReassignmentsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::AlterPartitionReassignmentsResponse(alter_partition_reassignments));
                }
                ApiKey::ListPartitionReassignmentsKey => {
                    let list_partition_reassignments = ListPartitionReassignmentsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::ListPartitionReassignmentsResponse(list_partition_reassignments));
                }
                ApiKey::OffsetDeleteKey => {
                    let offset_delete = OffsetDeleteResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::OffsetDeleteResponse(offset_delete));
                }
                ApiKey::DescribeClientQuotasKey => {
                    let describe_client_quotas = DescribeClientQuotasResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeClientQuotasResponse(describe_client_quotas));
                }
                ApiKey::AlterClientQuotasKey => {
                    let alter_client_quotas = AlterClientQuotasResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::AlterClientQuotasResponse(alter_client_quotas));
                }
                ApiKey::DescribeUserScramCredentialsKey => {
                    let describe_user_scram_credentials = DescribeUserScramCredentialsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeUserScramCredentialsResponse(describe_user_scram_credentials));
                }
                ApiKey::AlterUserScramCredentialsKey => {
                    let alter_user_scram_credentials = AlterUserScramCredentialsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::AlterUserScramCredentialsResponse(alter_user_scram_credentials));
                }
                ApiKey::VoteKey => {
                    let vote = VoteResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::VoteResponse(vote));
                }
                ApiKey::BeginQuorumEpochKey => {
                    let begin_quorum_epoch = BeginQuorumEpochResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::BeginQuorumEpochResponse(begin_quorum_epoch));
                }
                ApiKey::EndQuorumEpochKey => {
                    let end_quorum_epoch = EndQuorumEpochResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::EndQuorumEpochResponse(end_quorum_epoch));
                }
                ApiKey::DescribeQuorumKey => {
                    let describe_quorum = DescribeQuorumResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeQuorumResponse(describe_quorum));
                }
                ApiKey::AlterPartitionKey => {
                    let alter_partition = AlterPartitionResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::AlterPartitionResponse(alter_partition));
                }
                ApiKey::UpdateFeaturesKey => {
                    let update_features = UpdateFeaturesResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::UpdateFeaturesResponse(update_features));
                }
                ApiKey::EnvelopeKey => {
                    let envelope = EnvelopeResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::EnvelopeResponse(envelope));
                }
                ApiKey::FetchSnapshotKey => {
                    let fetch_snapshot = FetchSnapshotResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::FetchSnapshotResponse(fetch_snapshot));
                }
                ApiKey::DescribeClusterKey => {
                    let describe_cluster = DescribeClusterResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeClusterResponse(describe_cluster));
                }
                ApiKey::DescribeProducersKey => {
                    let describe_producers = DescribeProducersResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeProducersResponse(describe_producers));
                }
                ApiKey::BrokerRegistrationKey => {
                    let broker_registration = BrokerRegistrationResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::BrokerRegistrationResponse(broker_registration));
                }
                ApiKey::BrokerHeartbeatKey => {
                    let broker_heartbeat = BrokerHeartbeatResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::BrokerHeartbeatResponse(broker_heartbeat));
                }
                ApiKey::UnregisterBrokerKey => {
                    let unregister_broker = UnregisterBrokerResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::UnregisterBrokerResponse(unregister_broker));
                }
                ApiKey::DescribeTransactionsKey => {
                    let describe_transactions = DescribeTransactionsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::DescribeTransactionsResponse(describe_transactions));
                }
                ApiKey::ListTransactionsKey => {
                    let list_transactions = ListTransactionsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::ListTransactionsResponse(list_transactions));
                }
                ApiKey::AllocateProducerIdsKey => {
                    let allocate_producer_ids = AllocateProducerIdsResponse::decode(&mut self.raw_body.as_slice(), version)?;
                    self.body = Some(ResponseKind::AllocateProducerIdsResponse(allocate_producer_ids));
                }
            }
        }
        Ok(())
    }
}

pub enum Command {
    Request(KafkaRequest),
    Response(KafkaResponse),
}

impl Command {
    pub fn is_request(&self) -> bool {
        matches!(self, Command::Request(_))
    }

    pub fn correlation_id(&self) -> i32 {
        match self {
            Command::Request(req) => req.header.correlation_id,
            Command::Response(res) => res.header.correlation_id,
        }
    }

    pub fn api_key(&self) -> Option<i16> {
        match self {
            Command::Request(req) => Some(req.header.request_api_key),
            _ => None
        }
    }
}
