use arrow_flight::sql::client::PreparedStatement;
use rustler::ResourceArc;
use std::sync::Mutex;
use tonic::transport::Channel;

use crate::client::Client;

/// A resource that holds a client reference for executing SQL queries.
pub struct ClientResource {
    pub inner: Mutex<Option<Client>>,
}

impl rustler::Resource for ClientResource {}

impl ClientResource {
    pub fn new(client: Client) -> ResourceArc<Self> {
        ResourceArc::new(Self {
            inner: Mutex::new(Some(client)),
        })
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
