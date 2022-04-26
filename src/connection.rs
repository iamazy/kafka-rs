use crate::error::{ConnectionError, Error, SharedError};
use crate::executor::Executor;
use crate::message::Command;
use futures::channel::{mpsc, oneshot};
use futures::{Future, FutureExt, Stream};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tracing::trace;
use url::Url;

pub struct ConnectionSender<Exe: Executor> {
    tx: mpsc::UnboundedSender<Command>,
    registrations: mpsc::UnboundedSender<(i32, oneshot::Sender<Command>)>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
    correlation_id: AtomicI32,
    error: SharedError,
    executor: Arc<Exe>,
    operation_timeout: Duration,
}

impl<Exe: Executor> ConnectionSender<Exe> {
    pub fn new(
        tx: mpsc::UnboundedSender<Command>,
        registrations: mpsc::UnboundedSender<(i32, oneshot::Sender<Command>)>,
        receiver_shutdown: Option<oneshot::Sender<()>>,
        error: SharedError,
        executor: Arc<Exe>,
        operation_timeout: Duration,
    ) -> Self {
        Self {
            tx,
            registrations,
            receiver_shutdown,
            correlation_id: AtomicI32::new(1),
            error,
            executor,
            operation_timeout,
        }
    }

    #[tracing::instrument(skip(self, cmd))]
    pub async fn send(&self, cmd: Command) -> Result<Command, Error> {
        // let (sender, receiver) = oneshot::channel();
        // trace!("sending command: {:?}", cmd);
        let mut cmd = cmd;
        todo!()
    }
}

struct Receiver<S: Stream<Item = Result<Command, ConnectionError>>> {
    inbound: Pin<Box<S>>,
    outbound: mpsc::UnboundedSender<Command>,
    error: SharedError,
    pending_requests: BTreeMap<i32, oneshot::Sender<Command>>,
    received_commands: BTreeMap<i32, Command>,
    registrations: Pin<Box<mpsc::UnboundedReceiver<(i32, oneshot::Sender<Command>)>>>,
    shutdown: Pin<Box<oneshot::Receiver<()>>>,
}

impl<S: Stream<Item = Result<Command, ConnectionError>>> Receiver<S> {
    pub fn new(
        inbound: S,
        outbound: mpsc::UnboundedSender<Command>,
        error: SharedError,
        registrations: mpsc::UnboundedReceiver<(i32, oneshot::Sender<Command>)>,
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
                Poll::Ready(Some((correlation_id, resolver))) => {
                    match self.received_commands.remove(&correlation_id) {
                        Some(command) => {
                            let _ = resolver.send(command);
                        }
                        None => {
                            self.pending_requests.insert(correlation_id, resolver);
                        }
                    }
                }
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
                        if let Some(resolver) = self.pending_requests.remove(&res.header.correlation_id)
                        {
                            let _ = resolver.send(cmd);
                        }
                    }
                }
                Poll::Ready(None) => {
                    self.error.set(ConnectionError::Disconnected);
                    return Poll::Ready(Err(()));
                },
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
