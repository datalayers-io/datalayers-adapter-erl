use std::{str::FromStr, time::Duration};

use crate::client_opts::ClientOpts;
use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_flight::{
    Ticket,
    sql::client::{FlightSqlServiceClient, PreparedStatement},
};

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

/// Per-stage timeout applied to every blocking flight operation.
///
/// Without this, a half-open TCP connection makes `RT.block_on(...)` hang
/// forever: the calling dirty-io scheduler thread is blocked, the Erlang async
/// callback never fires and the buffer worker's inflight window is never
/// reclaimed.
///
/// Defaults to 15s. Override with `DL_NIF_TIMEOUT_SECS`; `0` or an invalid
/// value falls back to the default.
fn timeout_from_str(value: Option<&str>) -> Duration {
    value
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(15))
}

fn op_timeout() -> Duration {
    timeout_from_str(std::env::var("DL_NIF_TIMEOUT_SECS").ok().as_deref())
}

async fn timed<T, E, F>(stage: &str, fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    timed_with(op_timeout(), stage, fut).await
}

async fn timed_with<T, E, F>(limit: Duration, stage: &str, fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match tokio::time::timeout(limit, fut).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(anyhow::anyhow!("[{stage}] {err}")),
        Err(_) => Err(anyhow::anyhow!("[{stage}] timed out after {limit:?}")),
    }
}

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

        let channel = timed("connect:tcp", endpoint.connect())
            .await
            .context(format!("Failed to connect to server with uri {uri}"))?;
        let mut flight_sql_client = FlightSqlServiceClient::new(channel);

        // Performs authorization with the Datalayers server.
        let _ = timed(
            "connect:handshake",
            flight_sql_client.handshake(
                &opts.username.clone().unwrap_or_default(),
                &opts.password.clone().unwrap_or_default(),
            ),
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
        let flight_info =
            timed("execute:flight", self.inner.execute(sql.to_string(), None)).await?;
        let ticket = flight_info
            .endpoint
            .first()
            .context("No endpoint in flight info")?
            .ticket
            .clone()
            .context("No ticket in endpoint")?;
        let batches = timed("execute:do_get", self.do_get(ticket)).await?;
        Ok(batches)
    }

    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement<Channel>> {
        let prepared_stmt = timed("prepare", self.inner.prepare(sql.to_string(), None)).await?;
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
        let flight_info = timed("execute_prepared:flight", prepared_stmt.execute()).await?;
        let ticket = flight_info
            .endpoint
            .first()
            .context("No endpoint in flight info")?
            .ticket
            .clone()
            .context("No ticket in endpoint")?;
        let batches = timed("execute_prepared:do_get", self.do_get(ticket)).await?;
        Ok(batches)
    }

    pub async fn close_prepared(&self, prepared_stmt: PreparedStatement<Channel>) -> Result<()> {
        timed("close_prepared", prepared_stmt.close())
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

#[cfg(test)]
mod tests {
    use super::{timed_with, timeout_from_str};
    use std::time::Duration;

    #[test]
    fn timeout_defaults_to_15s() {
        assert_eq!(timeout_from_str(None), Duration::from_secs(15));
        assert_eq!(timeout_from_str(Some("")), Duration::from_secs(15));
        assert_eq!(timeout_from_str(Some("0")), Duration::from_secs(15));
        assert_eq!(timeout_from_str(Some("nope")), Duration::from_secs(15));
    }

    #[test]
    fn timeout_reads_env_value() {
        assert_eq!(timeout_from_str(Some("30")), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn timed_returns_value() {
        let out = timed_with(Duration::from_secs(5), "t", async {
            Ok::<_, std::io::Error>(42)
        })
        .await;
        assert_eq!(out.unwrap(), 42);
    }

    #[tokio::test]
    async fn timed_propagates_error() {
        let out = timed_with(Duration::from_secs(5), "t", async {
            Err::<i32, _>(std::io::Error::other("boom"))
        })
        .await;
        let msg = out.unwrap_err().to_string();
        assert!(msg.contains("[t]") && msg.contains("boom"), "{msg}");
    }

    #[tokio::test]
    async fn timed_returns_error_on_timeout() {
        let out = timed_with(
            Duration::from_millis(10),
            "execute:flight",
            std::future::pending::<Result<i32, std::io::Error>>(),
        )
        .await;
        let msg = out.unwrap_err().to_string();
        assert!(msg.contains("timed out after"), "{msg}");
    }
}
