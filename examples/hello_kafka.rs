use std::sync::Arc;

use kafka_protocol::messages::{DescribeClusterRequest, RequestHeader, RequestKind, ResponseKind};
use kafka_protocol::protocol::Request;

use kafka_rs::connection_manager::{BrokerAddress, ConnectionManager, OperationRetryOptions};
use kafka_rs::error::ConnectionError;
use kafka_rs::executor::TokioExecutor;
use kafka_rs::protocol::KafkaRequest;

#[tokio::main]
async fn main() -> Result<(), Box<ConnectionError>> {
    let executor = Arc::new(TokioExecutor);
    let manager = ConnectionManager::new(
        "kafka://192.168.1.5:9092".into(),
        None,
        OperationRetryOptions::default(),
        executor,
    )
    .await?;
    let mut header = RequestHeader::default();
    header.request_api_key = DescribeClusterRequest::KEY;
    let request = KafkaRequest {
        header,
        body: RequestKind::DescribeClusterRequest(DescribeClusterRequest::default()),
    };
    let addr = BrokerAddress {
        url: manager.url.clone(),
        broker_url: "localhost:9092".into(),
    };
    let response = manager.invoke(&addr, request).await?;
    println!("{}", response.header.correlation_id);
    if let ResponseKind::DescribeClusterResponse(res) = response.body.unwrap() {
        for (k, v) in res.brokers.iter() {
            println!("key: {:?}, value: {:?}", k, v.host);
        }
        println!("{:?}", res);
    }
    Ok(())
}
