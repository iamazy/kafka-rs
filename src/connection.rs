use crate::error::{ConnectionError, SharedError};
use crate::executor::{Executor, ExecutorKind};
use crate::protocol::{Command, KafkaCodec, KafkaRequest, KafkaResponse};
use futures::channel::{mpsc, oneshot};
use futures::future::{select, Either};
use futures::{pin_mut, Future, FutureExt, Sink, SinkExt, Stream, StreamExt};
use rand::{thread_rng, Rng};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tracing::{debug, error, trace};
use url::Url;

pub(crate) struct RegisterPair {
    correlation_id: i32,
    resolver: oneshot::Sender<Command>,
}

#[derive(Clone)]
pub struct SerialId(Arc<AtomicUsize>);

impl Default for SerialId {
    fn default() -> Self {
        SerialId(Arc::new(AtomicUsize::new(0)))
    }
}

impl SerialId {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn get(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) as u64
    }
}

pub struct ConnectionSender<Exe: Executor> {
    tx: mpsc::UnboundedSender<Command>,
    registrations: mpsc::UnboundedSender<RegisterPair>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
    correlation_id: SerialId,
    error: SharedError,
    executor: Arc<Exe>,
    operation_timeout: Duration,
}

impl<Exe: Executor> ConnectionSender<Exe> {
    pub(crate) fn new(
        tx: mpsc::UnboundedSender<Command>,
        registrations: mpsc::UnboundedSender<RegisterPair>,
        receiver_shutdown: oneshot::Sender<()>,
        error: SharedError,
        executor: Arc<Exe>,
        operation_timeout: Duration,
    ) -> Self {
        Self {
            tx,
            registrations,
            receiver_shutdown: Some(receiver_shutdown),
            correlation_id: SerialId::new(),
            error,
            executor,
            operation_timeout,
        }
    }

    #[tracing::instrument(skip(self, request))]
    pub async fn send(
        &self,
        request: KafkaRequest,
        version: i16,
    ) -> Result<KafkaResponse, ConnectionError> {
        let (resolver, response) = oneshot::channel();
        let response = async {
            response.await.map_err(|oneshot::Canceled| {
                self.error.set(ConnectionError::Disconnected);
                ConnectionError::Disconnected
            })
        };
        let api_key = request.api_key();
        match (
            self.registrations.unbounded_send(RegisterPair {
                correlation_id: request.correlation_id(),
                resolver,
            }),
            self.tx.unbounded_send(Command::Request(request)),
        ) {
            (Ok(_), Ok(_)) => {
                let delay_f = self.executor.delay(self.operation_timeout);
                pin_mut!(response);
                pin_mut!(delay_f);

                match select(response, delay_f).await {
                    Either::Left((Ok(Command::Response(mut res)), _)) => {
                        let _ = res.fill_body(api_key, version);
                        Ok(res)
                    }
                    Either::Left((Err(e), _)) => Err(e),
                    Either::Left((_, _)) => Err(ConnectionError::UnexpectedResponse(
                        "receive an invalid request".into(),
                    )),
                    Either::Right(_) => Err(ConnectionError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timeout sending message to the kafka server",
                    ))),
                }
            }
            _ => Err(ConnectionError::Disconnected),
        }
    }

    #[tracing::instrument(skip(self, request))]
    pub async fn send_oneway(&self, request: KafkaRequest) -> Result<(), ConnectionError> {
        self.tx
            .unbounded_send(Command::Request(request))
            .map_err(|_| ConnectionError::Disconnected)?;
        Ok(())
    }
}

struct Receiver<S: Stream<Item = Result<Command, ConnectionError>>> {
    inbound: Pin<Box<S>>,
    outbound: mpsc::UnboundedSender<Command>,
    error: SharedError,
    pending_requests: BTreeMap<i32, oneshot::Sender<Command>>,
    received_commands: BTreeMap<i32, Command>,
    registrations: Pin<Box<mpsc::UnboundedReceiver<RegisterPair>>>,
    shutdown: Pin<Box<oneshot::Receiver<()>>>,
}

impl<S: Stream<Item = Result<Command, ConnectionError>>> Receiver<S> {
    pub fn new(
        inbound: S,
        outbound: mpsc::UnboundedSender<Command>,
        error: SharedError,
        registrations: mpsc::UnboundedReceiver<RegisterPair>,
        shutdown: oneshot::Receiver<()>,
    ) -> Self {
        Receiver {
            inbound: Box::pin(inbound),
            outbound,
            error,
            pending_requests: BTreeMap::new(),
            received_commands: BTreeMap::new(),
            registrations: Box::pin(registrations),
            shutdown: Box::pin(shutdown),
        }
    }
}

impl<S: Stream<Item = Result<Command, ConnectionError>>> Future for Receiver<S> {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shutdown.as_mut().poll(cx) {
            Poll::Ready(Ok(())) | Poll::Ready(Err(futures::channel::oneshot::Canceled)) => {
                return Poll::Ready(Err(()));
            }
            Poll::Pending => {}
        }

        loop {
            match self.registrations.as_mut().poll_next(cx) {
                Poll::Ready(Some(RegisterPair {
                    correlation_id,
                    resolver,
                })) => match self.received_commands.remove(&correlation_id) {
                    Some(command) => {
                        let _ = resolver.send(command);
                    }
                    None => {
                        self.pending_requests.insert(correlation_id, resolver);
                    }
                },
                Poll::Ready(None) => {
                    self.error.set(ConnectionError::Disconnected);
                    return Poll::Ready(Err(()));
                }
                Poll::Pending => break,
            }
        }

        loop {
            match self.inbound.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(cmd))) => {
                    if let Command::Response(ref res) = cmd {
                        if let Some(resolver) =
                            self.pending_requests.remove(&res.header.correlation_id)
                        {
                            let _ = resolver.send(cmd);
                        }
                    }
                }
                Poll::Ready(None) => {
                    self.error.set(ConnectionError::Disconnected);
                    return Poll::Ready(Err(()));
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(e))) => {
                    self.error.set(e);
                    return Poll::Ready(Err(()));
                }
            }
        }
    }
}

pub struct Connection<Exe: Executor> {
    id: i64,
    url: Url,
    sender: ConnectionSender<Exe>,
}

impl<Exe: Executor> Connection<Exe> {
    pub async fn new(
        url: Url,
        connection_timeout: Duration,
        operation_timeout: Duration,
        executor: Arc<Exe>,
    ) -> Result<Connection<Exe>, ConnectionError> {
        if url.scheme() != "kafka" {
            return Err(ConnectionError::NotFound);
        }

        let u = url.clone();
        let address: SocketAddr = match executor
            .spawn_blocking(move || {
                u.socket_addrs(|| match u.scheme() {
                    "kafka" => Some(9092),
                    _ => None,
                })
                .map_err(|e| {
                    error!("could not look up address: {:?}", e);
                    e
                })
                .ok()
                .and_then(|v| {
                    let mut rng = thread_rng();
                    let index: usize = rng.gen_range(0..v.len());
                    v.get(index).copied()
                })
            })
            .await
        {
            Some(Some(addr)) => addr,
            _ => return Err(ConnectionError::NotFound),
        };
        debug!("connecting to {}:{}", url, address);
        let sender_prepare =
            Connection::prepare_stream(address, executor.clone(), operation_timeout);
        let delay_f = executor.delay(connection_timeout);

        pin_mut!(sender_prepare);
        pin_mut!(delay_f);

        let sender = match select(sender_prepare, delay_f).await {
            Either::Left((s, _)) => s?,
            Either::Right(_) => {
                return Err(ConnectionError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timeout connecting to the kafka server.",
                )))
            }
        };

        let id = rand::random();
        Ok(Connection { id, url, sender })
    }

    async fn prepare_stream(
        address: SocketAddr,
        executor: Arc<Exe>,
        operation_timeout: Duration,
    ) -> Result<ConnectionSender<Exe>, ConnectionError> {
        match executor.kind() {
            #[cfg(feature = "tokio-runtime")]
            ExecutorKind::Tokio => {
                let stream = tokio::net::TcpStream::connect(&address)
                    .await
                    .map(|stream| tokio_util::codec::Framed::new(stream, KafkaCodec))?;
                Connection::connect(stream, executor, operation_timeout).await
            }
            #[cfg(feature = "async-std-runtime")]
            ExecutorKind::AsyncStd => {
                let stream = async_std::net::TcpStream::connect(&address)
                    .await
                    .map(|stream| asynchronous_codec::Framed::new(stream, KafkaCodec))?;
                Connection::connect(stream, executor, operation_timeout).await
            }
            #[cfg(not(feature = "tokio-runtime"))]
            ExecutorKind::Tokio => {
                unimplemented!("the tokio-runtime cargo feature is not active.");
            }
            #[cfg(not(feature = "async-std-runtime"))]
            ExecutorKind::AsyncStd => {
                unimplemented!("the async-std-runtime cargo feature is not active.");
            }
        }
    }

    pub async fn connect<S>(
        stream: S,
        executor: Arc<Exe>,
        operation_timeout: Duration,
    ) -> Result<ConnectionSender<Exe>, ConnectionError>
    where
        S: Stream<Item = Result<Command, ConnectionError>>,
        S: Sink<Command, Error = ConnectionError>,
        S: Send + std::marker::Unpin + 'static,
    {
        let (mut sink, stream) = stream.split();
        let (tx, mut rx) = mpsc::unbounded();
        let (registrations_tx, registrations_rx) = mpsc::unbounded();
        let error = SharedError::new();
        let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();

        if executor
            .spawn(Box::pin(
                Receiver::new(
                    stream,
                    tx.clone(),
                    error.clone(),
                    registrations_rx,
                    receiver_shutdown_rx,
                )
                .map(|_| ()),
            ))
            .is_err()
        {
            error!("the executor could not spawn the receiver future");
            return Err(ConnectionError::Shutdown);
        }

        let err = error.clone();
        let res = executor.spawn(Box::pin(async move {
            while let Some(cmd) = rx.next().await {
                if let Err(e) = sink.send(cmd).await {
                    err.set(e);
                    break;
                }
            }
        }));
        if res.is_err() {
            error!("the executor could not spawn the receiver future");
            return Err(ConnectionError::Shutdown);
        }

        let sender = ConnectionSender::new(
            tx,
            registrations_tx,
            receiver_shutdown_tx,
            error,
            executor,
            operation_timeout,
        );
        Ok(sender)
    }

    pub fn sender(&self) -> &ConnectionSender<Exe> {
        &self.sender
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn is_valid(&self) -> bool {
        !self.sender.error.is_set()
    }
}

impl<Exe: Executor> Drop for Connection<Exe> {
    fn drop(&mut self) {
        trace!("dropping connection {} for {}", self.id, self.url);
        if let Some(shutdown) = self.sender.receiver_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}
