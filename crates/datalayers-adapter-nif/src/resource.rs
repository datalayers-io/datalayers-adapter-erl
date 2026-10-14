use arrow_flight::sql::client::PreparedStatement;
use rustler::ResourceArc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast};
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
    pub cancel: broadcast::Sender<()>,
}

impl rustler::Resource for ClientResource {}

impl ClientResource {
    pub fn new(client: Client, request_timeout: Duration) -> ResourceArc<Self> {
        let (cancel, _) = broadcast::channel(1);
        ResourceArc::new(Self {
            inner: Mutex::new(Some(client)),
            request_timeout,
            cancel,
        })
    }

    /// Ask the in-flight operation, if any, to give up.
    pub fn cancel_in_flight(&self) {
        let _ = self.cancel.send(());
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
