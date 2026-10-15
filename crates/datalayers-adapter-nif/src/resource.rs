use arrow_flight::sql::client::PreparedStatement;
use rustler::ResourceArc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tonic::transport::Channel;

use crate::client::Client;

/// A resource that holds a client reference for executing SQL queries.
pub struct ClientResource {
    pub inner: Mutex<Option<Client>>,
    /// Whole-request deadline shared by every operation on this connection.
    pub request_timeout: Duration,
    /// Signalled when the socket is stopped, so the in-flight operation gives up
    /// instead of running until its deadline while `stop` waits for the client
    /// lock it is holding.
    pub cancel: watch::Sender<bool>,
}

impl rustler::Resource for ClientResource {}

impl ClientResource {
    pub fn new(
        client: Client,
        request_timeout: Duration,
        cancel: watch::Sender<bool>,
    ) -> ResourceArc<Self> {
        ResourceArc::new(Self {
            inner: Mutex::new(Some(client)),
            request_timeout,
            cancel,
        })
    }

    /// Ask the in-flight operation, if any, to give up.
    pub fn cancel_in_flight(&self) {
        self.cancel.send_replace(true);
    }
}

/// A cancellation signal that exists before a connection does.
///
/// A connect can be stopped while the handshake is still in progress, i.e. before
/// any `ClientResource` exists. The socket therefore holds one of these per
/// socket, hands it to `async_connect`, and the resulting connection then reuses
/// the very same channel for every operation on it.
pub struct CancelToken {
    sender: watch::Sender<bool>,
}

impl rustler::Resource for CancelToken {}

// The token is a pure signalling handle: a panic while a request is in progress
// cannot leave it in a state another request could trip over, which is what the
// unwind-safety markers are about. They are needed because the token crosses the
// NIF boundary as a resource.
impl std::panic::UnwindSafe for CancelToken {}
impl std::panic::RefUnwindSafe for CancelToken {}

impl CancelToken {
    pub fn new() -> ResourceArc<Self> {
        let (sender, _) = watch::channel(false);
        ResourceArc::new(Self { sender })
    }

    /// A handle the connect and, later, its connection can be cancelled with.
    pub fn sender(&self) -> watch::Sender<bool> {
        self.sender.clone()
    }

    /// Ask the connect (or the connection it produced) to give up.
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }
}

/// A resource that holds a prepared statement for executing SQL queries.
pub struct PreparedStatementResource {
    pub inner: Mutex<Option<PreparedStatement<Channel>>>,
    pub sql: String,
    pub auto_rebuild: bool,
}

impl rustler::Resource for PreparedStatementResource {}

impl PreparedStatementResource {
    pub fn new(
        statement: PreparedStatement<Channel>,
        sql: String,
        auto_rebuild: bool,
    ) -> ResourceArc<Self> {
        ResourceArc::new(Self {
            inner: Mutex::new(Some(statement)),
            sql,
            auto_rebuild,
        })
    }
}
