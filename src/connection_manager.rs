use crate::connection::Connection;
use crate::error::ConnectionError;
use crate::executor::Executor;
use crate::protocol::{Command, KafkaResponse};
use futures::channel::oneshot;
use futures::lock::Mutex;
use futures::StreamExt;
use log::warn;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, trace};
use url::Url;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BrokerAddress {
    pub url: Url,
    pub broker_url: String,
}

/// configuration for reconnection exponential back off
#[derive(Debug, Clone)]
pub struct ConnectionRetryOptions {
    /// minimum delay between connection retries
    pub min_backoff: Duration,
    /// maximum delay between rconnection etries
    pub max_backoff: Duration,
    /// maximum number of connection retries
    pub max_retries: u32,
    /// time limit to establish a connection
    pub connection_timeout: Duration,
    /// keep-alive interval for each broker connection
    pub keep_alive: Duration,
}

impl std::default::Default for ConnectionRetryOptions {
    fn default() -> Self {
        ConnectionRetryOptions {
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(30),
            max_retries: 12u32,
            connection_timeout: Duration::from_secs(10),
            keep_alive: Duration::from_secs(60),
        }
    }
}

/// configuration for Pulsar operation retries
#[derive(Debug, Clone)]
pub struct OperationRetryOptions {
    /// time limit to receive an answer to a Pulsar operation
    pub operation_timeout: Duration,
    /// delay between operation retries after a ServiceNotReady error
    pub retry_delay: Duration,
    /// maximum number of operation retries. None indicates infinite retries
    pub max_retries: Option<u32>,
}

impl std::default::Default for OperationRetryOptions {
    fn default() -> Self {
        OperationRetryOptions {
            operation_timeout: Duration::from_secs(30),
            retry_delay: Duration::from_millis(500),
            max_retries: None,
        }
    }
}

enum ConnectionStatus<Exe: Executor> {
    Connected(Arc<Connection<Exe>>),
    Connecting(Vec<oneshot::Sender<Result<Arc<Connection<Exe>>, ConnectionError>>>),
}

/// Look up broker addresses for topics and partitioned topics
///
/// The ConnectionManager object provides a single interface to start
/// interacting with a cluster. It will automatically follow redirects
/// or use a proxy, and aggregate broker connections
#[derive(Clone)]
pub struct ConnectionManager<Exe: Executor> {
    pub url: Url,
    pub(crate) executor: Arc<Exe>,
    connections: Arc<Mutex<HashMap<BrokerAddress, ConnectionStatus<Exe>>>>,
    connection_retry_options: ConnectionRetryOptions,
    pub(crate) operation_retry_options: OperationRetryOptions,
}

impl<Exe: Executor> ConnectionManager<Exe> {
    pub async fn new(
        url: String,
        connection_retry: Option<ConnectionRetryOptions>,
        operation_retry_options: OperationRetryOptions,
        executor: Arc<Exe>,
    ) -> Result<Self, ConnectionError> {
        let connection_retry_options = connection_retry.unwrap_or_default();
        let url = Url::parse(&url)
            .map_err(|e| {
                error!("error parsing URL: {:?}", e);
                ConnectionError::NotFound
            })
            .and_then(|url| {
                url.host_str().ok_or_else(|| {
                    error!("missing host for URL: {:?}", url);
                    ConnectionError::NotFound
                })?;
                Ok(url)
            })?;

        let manager = ConnectionManager {
            url: url.clone(),
            executor,
            connections: Arc::new(Mutex::new(HashMap::new())),
            connection_retry_options,
            operation_retry_options,
        };

        let broker_address = BrokerAddress {
            url: url.clone(),
            broker_url: format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(6650)),
        };

        manager.connect(broker_address).await?;
        Ok(manager)
    }

    pub async fn invoke(
        &self,
        addr: &BrokerAddress,
        cmd: Command,
    ) -> Result<KafkaResponse, ConnectionError> {
        let conn = self.get_connection(addr).await?;
        let sender = conn.sender();
        sender.send(cmd, 1).await
    }

    pub async fn invoke_oneway(
        &self,
        addr: &BrokerAddress,
        cmd: Command,
    ) -> Result<(), ConnectionError> {
        let conn = self.get_connection(addr).await?;
        let sender = conn.sender();
        sender.send_oneway(cmd).await
    }

    pub fn get_base_address(&self) -> BrokerAddress {
        BrokerAddress {
            url: self.url.clone(),
            broker_url: format!(
                "{}:{}",
                self.url.host_str().unwrap(),
                self.url.port().unwrap_or(6650)
            ),
        }
    }

    pub async fn get_base_connection(
        &self,
        broker: &BrokerAddress,
    ) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        let broker_address = BrokerAddress {
            url: self.url.clone(),
            broker_url: format!(
                "{}:{}",
                broker.url.host_str().unwrap(),
                broker.url.port().unwrap_or(6650)
            ),
        };
        self.get_connection(&broker_address).await
    }

    pub async fn get_connection(
        &self,
        broker: &BrokerAddress,
    ) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        let rx = {
            let mut conns = self.connections.lock().await;
            match conns.get_mut(broker) {
                None => None,
                Some(ConnectionStatus::Connected(conn)) => {
                    if conn.is_valid() {
                        return Ok(conn.clone());
                    } else {
                        None
                    }
                }
                Some(ConnectionStatus::Connecting(ref mut v)) => {
                    let (tx, rx) = oneshot::channel();
                    v.push(tx);
                    Some(rx)
                }
            }
        };

        match rx {
            None => self.connect(broker.clone()).await,
            Some(rx) => match rx.await {
                Ok(res) => res,
                Err(_) => Err(ConnectionError::Canceled),
            },
        }
    }

    async fn connect_inner(
        &self,
        broker: &BrokerAddress,
    ) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        let rx = {
            match self
                .connections
                .lock()
                .await
                .entry(broker.clone())
                .or_insert_with(|| ConnectionStatus::Connecting(Vec::new()))
            {
                ConnectionStatus::Connecting(ref mut v) => {
                    if v.is_empty() {
                        None
                    } else {
                        let (tx, rx) = oneshot::channel();
                        v.push(tx);
                        Some(rx)
                    }
                }
                ConnectionStatus::Connected(_) => None,
            }
        };

        if let Some(rx) = rx {
            return match rx.await {
                Ok(res) => res,
                Err(_) => Err(ConnectionError::Canceled),
            };
        }

        let mut current_backoff;
        let mut current_retries = 0u32;

        let start = std::time::Instant::now();
        let conn = loop {
            match Connection::new(
                broker.url.clone(),
                self.connection_retry_options.connection_timeout,
                self.operation_retry_options.operation_timeout,
                self.executor.clone(),
            )
            .await
            {
                Ok(c) => break c,
                Err(ConnectionError::Io(e)) => {
                    if e.kind() != std::io::ErrorKind::ConnectionRefused
                        || e.kind() != std::io::ErrorKind::TimedOut
                    {
                        return Err(ConnectionError::Io(e));
                    }

                    if current_retries == self.connection_retry_options.max_retries {
                        return Err(ConnectionError::Io(e));
                    }

                    let jitter = rand::thread_rng().gen_range(0..10);
                    current_backoff = std::cmp::min(
                        self.connection_retry_options.min_backoff
                            * 2u32.saturating_pow(current_retries),
                        self.connection_retry_options.max_backoff,
                    ) + self.connection_retry_options.min_backoff * jitter;
                    current_retries += 1;

                    trace!(
                        "current retries: {}, current_backoff(pow = {}): {}ms",
                        current_retries,
                        2u32.pow(current_retries - 1),
                        current_backoff.as_millis()
                    );
                    error!(
                        "connection error, retrying connection to {} after {}ms",
                        broker.url,
                        current_backoff.as_millis()
                    );

                    self.executor.delay(current_backoff).await;
                }
                Err(e) => return Err(e),
            }
        };

        let connection_id = conn.id();
        info!(
            "Connected n°{} to {} in {}ms",
            connection_id,
            broker.url,
            (std::time::Instant::now() - start).as_millis()
        );
        let c = Arc::new(conn);
        Ok(c)
    }

    async fn connect(
        &self,
        broker: BrokerAddress,
    ) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        let conn = match self.connect_inner(&broker).await {
            Ok(c) => c,
            Err(e) => {
                if let Some(ConnectionStatus::Connecting(mut v)) =
                    self.connections.lock().await.remove(&broker)
                {
                    for tx in v.drain(..) {
                        let _ = tx.send(Err(ConnectionError::Canceled));
                    }
                }
                return Err(e);
            }
        };

        let _connection_id = conn.id();
        let _weak_conn = Arc::downgrade(&conn);
        let mut interval = self
            .executor
            .interval(self.connection_retry_options.keep_alive);
        let broker_url = broker.url.clone();
        let res = self.executor.spawn(Box::pin(async move {
            while let Some(()) = interval.next().await {
                // trace!("will ping connection {} to {}", connection_id, broker_url);
                // if let Some(strong_conn) = weak_conn.upgrade() {
                //     if let Err(e) = strong_conn.sender().
                // }
            }
        }));

        if res.is_err() {
            error!("the executor could not spawn the heartbeat future");
            return Err(ConnectionError::Shutdown);
        }

        let old = self
            .connections
            .lock()
            .await
            .insert(broker, ConnectionStatus::Connected(conn.clone()));
        match old {
            Some(ConnectionStatus::Connecting(mut v)) => {
                for tx in v.drain(..) {
                    let _ = tx.send(Ok(conn.clone()));
                }
            }
            Some(ConnectionStatus::Connected(_)) => {
                warn!("the connection to {} was already connected", broker_url);
            }
            None => {}
        };

        Ok(conn)
    }

    async fn check_connections(&self) {
        trace!("cleaning invalid or unused connections");
        self.connections
            .lock()
            .await
            .retain(|_, ref mut connection| match connection {
                ConnectionStatus::Connecting(_) => true,
                ConnectionStatus::Connected(conn) => {
                    // if the manager holds the only reference to that
                    // connection, we can remove it from the manager
                    // no need for special synchronization here: we're already
                    // in a mutex, and a case appears where the Arc is cloned
                    // somewhere at the same time, that just means the manager
                    // will create a new connection the next time it is asked
                    conn.is_valid() && Arc::strong_count(conn) > 1
                }
            })
    }
}
