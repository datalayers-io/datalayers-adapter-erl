use std::{str::FromStr, time::Duration};

use crate::client_opts::ClientOpts;
use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_flight::{
    Ticket,
    sql::client::{FlightSqlServiceClient, PreparedStatement},
};

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

pub struct Client {
    /// The Arrow Flight SQL client.
    inner: FlightSqlServiceClient<Channel>,
}

impl Client {
    pub async fn try_new(opts: &ClientOpts) -> Result<Self> {
        let uri = opts.format_uri();
        let mut endpoint = Endpoint::from_str(&uri)
            .context(format!("Failed to create an endpoint with uri {uri}"))?
            .connect_timeout(Duration::from_secs(5))
            .keep_alive_while_idle(true);

        // Configures TLS if a certificate is provided.
        if let Some(tls_cert) = &opts.tls_cert {
            let cert = std::fs::read_to_string(tls_cert)
                .context(format!("Failed to read the TLS cert file {tls_cert}"))?;
            let cert = Certificate::from_pem(cert);
            let tls_config = ClientTlsConfig::new()
                .domain_name(opts.host.clone().unwrap_or_default())
                .ca_certificate(cert);
            endpoint = endpoint
                .tls_config(tls_config)
                .context("failed to configure TLS")?;
        }

        let channel = endpoint
            .connect()
            .await
            .context(format!("Failed to connect to server with uri {uri}"))?;
        let mut flight_sql_client = FlightSqlServiceClient::new(channel);

        // Performs authorization with the Datalayers server.
        let _ = flight_sql_client
            .handshake(
                &opts.username.clone().unwrap_or_default(),
                &opts.password.clone().unwrap_or_default(),
            )
            .await?;

        Ok(Self {
            inner: flight_sql_client,
        })
    }

    pub fn use_database(&mut self, database: &str) {
        self.inner.set_header("database", database);
    }

    pub async fn execute(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let flight_info = self.inner.execute(sql.to_string(), None).await?;
        let ticket = flight_info
            .endpoint
            .first()
            .context("No endpoint in flight info")?
            .ticket
            .clone()
            .context("No ticket in endpoint")?;
        let batches = self.do_get(ticket).await?;
        Ok(batches)
    }

    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement<Channel>> {
        let prepared_stmt = self.inner.prepare(sql.to_string(), None).await?;
        Ok(prepared_stmt)
    }

    pub async fn execute_prepared(
        &mut self,
        prepared_stmt: &mut PreparedStatement<Channel>,
        binding: RecordBatch,
    ) -> Result<Vec<RecordBatch>> {
        prepared_stmt
            .set_parameters(binding)
            .context("Failed to bind a record batch to the prepared statement")?;
        let flight_info = prepared_stmt.execute().await?;
        let ticket = flight_info
            .endpoint
            .first()
            .context("No endpoint in flight info")?
            .ticket
            .clone()
            .context("No ticket in endpoint")?;
        let batches = self.do_get(ticket).await?;
        Ok(batches)
    }

    pub async fn close_prepared(&self, prepared_stmt: PreparedStatement<Channel>) -> Result<()> {
        prepared_stmt
            .close()
            .await
            .context("Failed to close a prepared statement")
    }

    pub async fn stop(self) {
        // By taking ownership of self, this method consumes the Client.
        // When the method returns, self is dropped, and the underlying
        // gRPC channel is closed.
    }

    async fn do_get(&mut self, ticket: Ticket) -> Result<Vec<RecordBatch>> {
        use futures::TryStreamExt;
        let stream = self.inner.do_get(ticket).await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(batches)
    }
}
