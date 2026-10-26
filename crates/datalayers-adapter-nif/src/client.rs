use std::sync::{Arc, OnceLock};
use std::{str::FromStr, time::Duration};

use crate::client_opts::ClientOpts;
use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_flight::{
    Ticket,
    sql::client::{FlightSqlServiceClient, PreparedStatement},
};

use tokio::sync::Semaphore;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

/// Capacity for certificate reads.
///
/// A blocking read can not be cancelled: when the request deadline fires it is
/// abandoned, but it keeps running on the blocking pool. Holding a slot here for
/// the whole read keeps the number of those abandoned reads bounded, which the
/// request's own in-flight slot can not do because the timeout releases it.
static FILE_READ_LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();

fn file_read_limiter() -> Arc<Semaphore> {
    FILE_READ_LIMITER
        .get_or_init(|| Arc::new(Semaphore::new(4)))
        .clone()
}

/// Read `path` on the blocking pool, holding a read slot until the read returns.
async fn read_file(path: &str) -> Result<String> {
    let permit = file_read_limiter()
        .acquire_owned()
        .await
        .map_err(|_| anyhow::anyhow!("the file read limiter is closed"))?;
    let path_owned = path.to_string();
    tokio::task::spawn_blocking(move || {
        // Moved in on purpose: the slot is released when the read really ends,
        // not when the request that asked for it gave up.
        let _permit = permit;
        std::fs::read_to_string(&path_owned)
    })
    .await
    .map_err(|e| anyhow::anyhow!("the certificate read task failed: {e}"))?
    .context(format!("Failed to read the TLS cert file {path}"))
}

/// Run `fut`, bounded by `limit`, tagging any error with the flight stage it came
/// from.
///
/// Without a bound, a half-open TCP connection makes the future never complete:
/// the task would occupy a shared runtime worker forever, the socket would never
/// receive a completion and the buffer worker's inflight window would never be
/// reclaimed.
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
    pub async fn try_new(opts: &ClientOpts, on_network: impl FnOnce()) -> Result<Self> {
        let uri = opts.format_uri();
        let timeout = opts.timeout();
        // Deliberately no endpoint-level connect timeout: the `timed` wrapper
        // around `endpoint.connect()` bounds this stage, so the caller's `timeout`
        // (or the request budget handed down by the socket) is authoritative and
        // the failure is reported as a stage timeout, not a transport error.
        let mut endpoint = Endpoint::from_str(&uri)
            .context(format!("Failed to create an endpoint with uri {uri}"))?
            .keep_alive_while_idle(true);

        // Configures TLS if a certificate is provided.
        if let Some(tls_cert) = &opts.tls_cert {
            // Read on the blocking pool, holding a read slot for the whole read:
            // a synchronous read here would stall a runtime worker, and a read
            // that outlives its request must not be able to pile up unbounded.
            let cert = read_file(tls_cert).await?;
            let cert = Certificate::from_pem(cert);
            let tls_config = ClientTlsConfig::new()
                .domain_name(opts.host.clone().unwrap_or_default())
                .ca_certificate(cert);
            endpoint = endpoint
                .tls_config(tls_config)
                .context("failed to configure TLS")?;
        }

        on_network();
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

    async fn do_get(&mut self, ticket: Ticket) -> Result<Vec<RecordBatch>> {
        use futures::TryStreamExt;
        let stream = self.inner.do_get(ticket).await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, timed};
    use crate::client_opts::ClientOpts;
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

    /// A peer that accepts TCP but never speaks HTTP/2 is the exact half-open
    /// symptom seen in the field. Establishing the connection must fail within
    /// the per-stage timeout rather than hanging forever.
    #[tokio::test]
    async fn connecting_to_a_silent_server_is_bounded_by_the_stage_timeout() {
        use std::time::Instant;

        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind a local TCP listener");
        let port = listener.local_addr().expect("listener address").port();
        // Accept connections and hold them open without ever writing a byte.
        std::thread::spawn(move || {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept() {
                held.push(stream);
            }
        });

        let opts = ClientOpts {
            host: Some("127.0.0.1".to_string()),
            port: Some(port),
            timeout_ms: Some(300),
            ..Default::default()
        };

        let started = Instant::now();
        let err = match Client::try_new(&opts, || {}).await {
            Ok(_) => panic!("connecting to a silent server must fail, not hang"),
            Err(err) => err,
        };
        let elapsed = started.elapsed();

        assert!(
            format!("{err:#}").contains("timed out after"),
            "expected a bounded timeout, got: {err:#}"
        );
        assert!(elapsed < Duration::from_secs(3), "took {elapsed:?} to fail");
    }

    /// The certificate is read before the endpoint is built, and the read must not
    /// be able to outlive the request deadline. A writer that holds a FIFO open
    /// without writing keeps the read blocked; a synchronous read would stall the
    /// runtime worker (and the deadline with it) until the writer let go.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_stalled_certificate_read_is_bounded_by_the_deadline() {
        use crate::AsyncPayload;
        use std::time::Instant;
        use tokio::sync::Semaphore;

        let dir = std::env::temp_dir().join(format!("datalayers-nif-fifo-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let fifo = dir.join("ca.pem");
        let _ = std::fs::remove_file(&fifo);
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if !made {
            // Without mkfifo there is no portable way to stall a read here.
            return;
        }
        // Hold the FIFO open without writing, then release it. Releasing it lets a
        // reader that is still blocked in the background finish, so a failing run
        // can not leave the runtime waiting on it forever.
        let held = fifo.clone();
        std::thread::spawn(move || {
            if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&held) {
                std::thread::sleep(Duration::from_secs(1));
                drop(file);
            }
        });

        let opts = ClientOpts {
            host: Some("127.0.0.1".to_string()),
            port: Some(1),
            tls_cert: Some(fifo.to_string_lossy().to_string()),
            timeout_ms: Some(60000),
            request_timeout_ms: Some(200),
            ..Default::default()
        };

        let started = Instant::now();
        let deadline = crate::RequestDeadline::from_remaining(
            Duration::from_millis(200),
            Duration::from_millis(200),
        );
        let dispatched = std::sync::atomic::AtomicBool::new(false);
        let permits = Semaphore::new(4);
        let out = crate::run_bounded(deadline, &permits, &dispatched, async move {
            match Client::try_new(&opts, || {}).await {
                Ok(_) => AsyncPayload::Closed,
                Err(e) => AsyncPayload::Error(format!("{e:#}")),
            }
        })
        .await;
        let elapsed = started.elapsed();
        let _ = std::fs::remove_file(&fifo);
        let _ = std::fs::remove_dir(&dir);

        match out.payload {
            // The certificate is read before the endpoint can receive a request.
            AsyncPayload::NotDispatched(msg) => {
                assert!(msg.contains("timed out after"), "unexpected error: {msg}");
            }
            _ => panic!("a stalled certificate read must not let the request complete"),
        }
        assert!(
            elapsed < Duration::from_millis(1500),
            "the deadline did not bound the read: {elapsed:?}"
        );
    }

    /// A blocking read can not be cancelled, so a request that times out leaves it
    /// running. The read must keep holding a slot for as long as it really runs,
    /// otherwise repeated attempts pile up blocked reads without bound.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoned_certificate_reads_stay_bounded() {
        use crate::AsyncPayload;
        use tokio::sync::Semaphore;

        let dir = std::env::temp_dir().join(format!("datalayers-read-cap-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let fifo = dir.join("ca.pem");
        let _ = std::fs::remove_file(&fifo);
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if !made {
            return;
        }
        let path = fifo.to_string_lossy().to_string();

        let before = thread_count();
        for _ in 0..16 {
            // Each attempt is expected to time out while its read is stuck on a
            // FIFO nobody writes to.
            let deadline = crate::RequestDeadline::from_remaining(
                Duration::from_millis(50),
                Duration::from_millis(50),
            );
            let dispatched = std::sync::atomic::AtomicBool::new(false);
            let permits = Semaphore::new(16);
            let _ = crate::run_bounded(deadline, &permits, &dispatched, async {
                match super::read_file(&path).await {
                    Ok(_) => AsyncPayload::Closed,
                    Err(e) => AsyncPayload::Error(format!("{e:#}")),
                }
            })
            .await;
        }
        let grown = thread_count().saturating_sub(before);
        let held = super::file_read_limiter().available_permits();

        // Writers let the abandoned readers finish, so the runtime can shut down.
        // The FIFO path is left in place on purpose: removing it while a reader is
        // still blocked in open() would make those writers fail and the readers
        // block forever, which would hang the runtime shutdown.
        for _ in 0..8 {
            let unblock = fifo.clone();
            std::thread::spawn(move || {
                let _ = std::fs::OpenOptions::new().write(true).open(&unblock);
            });
        }

        assert!(
            held == 0,
            "the abandoned reads did not hold the read slots: {held} still free"
        );
        assert!(
            grown <= 8,
            "16 abandoned reads left {grown} extra threads behind"
        );
    }

    #[cfg(target_os = "linux")]
    fn thread_count() -> usize {
        std::fs::read_to_string("/proc/self/status")
            .unwrap_or_default()
            .lines()
            .find_map(|line| {
                line.strip_prefix("Threads:")
                    .and_then(|value| value.trim().parse().ok())
            })
            .unwrap_or(0)
    }
}
