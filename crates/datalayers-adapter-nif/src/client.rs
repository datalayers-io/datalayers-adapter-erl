use std::{str::FromStr, time::Duration};

use crate::client_opts::ClientOpts;
use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_flight::{
    Ticket,
    sql::client::{FlightSqlServiceClient, PreparedStatement},
};

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

/// Run `fut`, bounded by `limit`, tagging any error with the flight stage it came
/// from.
///
/// Without a bound, a half-open TCP connection makes `RT.block_on(...)` hang
/// forever: the calling thread is blocked, the Erlang async callback never fires
/// and the buffer worker's inflight window is never reclaimed.
///
/// The limit is always supplied by the caller (`Client` carries the value it was
/// created with) — there is deliberately no hidden global or environment knob.
///
/// The original error is attached as the *source* of the returned error (via
/// `anyhow::Context`) instead of being formatted into a fresh message: callers
/// classify failures by inspecting the error chain, e.g.
/// `is_prepared_statement_lost` looks for a `tonic::Status` with code `NotFound`
/// to decide whether a prepared statement has to be rebuilt. Turning the error
/// into a message-only error here would silently disable that classification.
///
/// Timeouts have no source, so the two paths differ only in the message.
async fn timed<T, E, F>(limit: Duration, stage: &str, fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: Into<anyhow::Error>,
{
    match tokio::time::timeout(limit, fut).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(err.into().context(format!("[{stage}]"))),
        Err(_) => Err(anyhow::anyhow!("[{stage}] timed out after {limit:?}")),
    }
}

pub struct Client {
    /// The Arrow Flight SQL client.
    inner: FlightSqlServiceClient<Channel>,
    /// Per-stage flight timeout, taken from the connect options.
    timeout: Duration,
}

impl Client {
    pub async fn try_new(opts: &ClientOpts) -> Result<Self> {
        let uri = opts.format_uri();
        let timeout = opts.timeout();
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

        let channel = timed(timeout, "connect:tcp", endpoint.connect())
            .await
            .context(format!("Failed to connect to server with uri {uri}"))?;
        let mut flight_sql_client = FlightSqlServiceClient::new(channel);

        // Performs authorization with the Datalayers server.
        let _ = timed(
            timeout,
            "connect:handshake",
            flight_sql_client.handshake(
                &opts.username.clone().unwrap_or_default(),
                &opts.password.clone().unwrap_or_default(),
            ),
        )
        .await?;

        Ok(Self {
            inner: flight_sql_client,
            timeout,
        })
    }

    pub fn use_database(&mut self, database: &str) {
        self.inner.set_header("database", database);
    }

    pub async fn execute(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let flight_info = timed(
            self.timeout,
            "execute:flight",
            self.inner.execute(sql.to_string(), None),
        )
        .await?;
        let ticket = flight_info
            .endpoint
            .first()
            .context("No endpoint in flight info")?
            .ticket
            .clone()
            .context("No ticket in endpoint")?;
        let batches = timed(self.timeout, "execute:do_get", self.do_get(ticket)).await?;
        Ok(batches)
    }

    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement<Channel>> {
        let prepared_stmt = timed(
            self.timeout,
            "prepare",
            self.inner.prepare(sql.to_string(), None),
        )
        .await?;
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
        let flight_info = timed(
            self.timeout,
            "execute_prepared:flight",
            prepared_stmt.execute(),
        )
        .await?;
        let ticket = flight_info
            .endpoint
            .first()
            .context("No endpoint in flight info")?
            .ticket
            .clone()
            .context("No ticket in endpoint")?;
        let batches = timed(self.timeout, "execute_prepared:do_get", self.do_get(ticket)).await?;
        Ok(batches)
    }

    pub async fn close_prepared(&self, prepared_stmt: PreparedStatement<Channel>) -> Result<()> {
        timed(self.timeout, "close_prepared", prepared_stmt.close())
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
    use super::timed;
    use std::time::Duration;

    #[tokio::test]
    async fn timed_returns_value() {
        let out = timed(Duration::from_secs(5), "t", async {
            Ok::<_, std::io::Error>(42)
        })
        .await;
        assert_eq!(out.unwrap(), 42);
    }

    #[tokio::test]
    async fn timed_propagates_error() {
        let out = timed(Duration::from_secs(5), "t", async {
            Err::<i32, _>(std::io::Error::other("boom"))
        })
        .await;
        let err = out.unwrap_err();
        // The stage is the context (`{}`), the cause lives in the chain (`{:#}`).
        assert_eq!(err.to_string(), "[t]");
        assert!(format!("{err:#}").contains("boom"), "{err:#}");
    }

    #[tokio::test]
    async fn timed_returns_error_on_timeout() {
        let out = timed(
            Duration::from_millis(10),
            "execute:flight",
            std::future::pending::<Result<i32, std::io::Error>>(),
        )
        .await;
        // The timeout text stays stable (other components match on it).
        assert_eq!(
            out.unwrap_err().to_string(),
            "[execute:flight] timed out after 10ms"
        );
    }

    /// `lib.rs::is_prepared_statement_lost/1` classifies a failure by walking the
    /// error chain, so the stage wrapper must keep the original error as the
    /// source instead of formatting it into a fresh message.
    #[tokio::test]
    async fn timed_preserves_the_source_for_error_classification() {
        let err = timed(Duration::from_secs(1), "execute_prepared:do_get", async {
            Err::<(), _>(tonic::Status::not_found("gone"))
        })
        .await
        .unwrap_err();

        assert!(
            crate::is_prepared_statement_lost(&err),
            "the wrapper dropped the tonic::Status from the chain: {err:?}"
        );
        // A contextual error prints only the context with `{}`; the cause is in the
        // chain, which is why the NIF entry points encode errors with `{:#}`.
        assert_eq!(err.to_string(), "[execute_prepared:do_get]");
        assert!(format!("{err:#}").contains("gone"), "{err:#}");
    }
}
