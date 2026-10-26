extern crate core;
extern crate rustler;

mod atoms;
mod client;
mod client_opts;
mod resource;
mod types;
mod util;

use crate::resource::CancelToken;
use crate::resource::{ClientResource, PreparedStatementResource};
use arrow_array::RecordBatch;
use arrow_flight::sql::client::PreparedStatement;
use arrow_schema::Schema;
use atoms::*;
use client::Client;
use client_opts::{ClientOpts, MAX_REQUEST_TIMEOUT_MS};
use futures::FutureExt;
use lazy_static::lazy_static;
use rustler::env::{OwnedEnv, SavedTerm};
use rustler::types::LocalPid;
use rustler::{Encoder, Env, NifResult, Reference, ResourceArc, Term};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::{Semaphore, SemaphorePermit, watch};
use tonic::transport::Channel;

fn is_prepared_statement_lost(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            return status.code() == tonic::Code::NotFound;
        }
    }
    false
}

rustler::init!("datalayers_nif", load = on_load);

lazy_static! {
    // This unwrap is acceptable because if the runtime fails to build, the NIF cannot run.
    static ref RT: Runtime = Runtime::new().unwrap();
}

pub fn on_load(env: Env, _load_info: Term) -> bool {
    let _ = env.register::<ClientResource>();
    let _ = env.register::<PreparedStatementResource>();
    let _ = env.register::<CancelToken>();
    true
}

/// Create the cancellation signal a socket holds for its connection attempt.
#[rustler::nif]
fn new_cancel_token() -> ResourceArc<CancelToken> {
    CancelToken::new()
}

/// Cancel whatever the token was handed to: an in-progress connect, or the
/// connection it produced.
#[rustler::nif]
fn cancel(token_ref: Reference) -> rustler::Atom {
    match token_ref.decode::<ResourceArc<CancelToken>>() {
        Ok(token) => {
            token.cancel();
            ok()
        }
        Err(_) => error(),
    }
}

// =============================================================================
// Asynchronous NIFs
//
// Every NIF below is submit-and-return: the NIF body only decodes its arguments
// and hands the actual flight round-trip to the shared tokio runtime. It never
// blocks the calling BEAM process, so it can never pin a normal *or* a dirty IO
// scheduler thread -- this is what keeps a stalled Datalayers server from
// starving file and log I/O across the whole VM (emqx/emqx#19342).
//
// When the task finishes (or a per-stage timeout inside `Client` fires) the
// result is delivered to the socket process as:
//
//     {datalayers_async_result, Id, Result}
//
// where `Result` is `{ok, Value}` or `{error, Reason}`. `datalayers_sock` keeps
// at most one operation in flight per connection and matches on `Id`.
// =============================================================================

/// The value computed by an asynchronous task and encoded back into the socket
/// process's environment once the task finishes.
enum AsyncPayload {
    Rows(Vec<Vec<String>>),
    Prepared(ResourceArc<PreparedStatementResource>),
    Connected(ResourceArc<ClientResource>),
    UseDatabase,
    Closed,
    Error(String),
    /// A failure that happened before anything was sent to the server. The adapter
    /// stays neutral about what the caller does with that: it only reports that no
    /// request was dispatched, and the caller decides whether that is retryable.
    NotDispatched(String),
}

impl AsyncPayload {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            AsyncPayload::Rows(rows) => (ok(), rows).encode(env),
            AsyncPayload::Prepared(resource) => (ok(), resource).encode(env),
            AsyncPayload::Connected(resource) => (ok(), resource).encode(env),
            AsyncPayload::UseDatabase => (ok(), "database_changed").encode(env),
            AsyncPayload::Closed => (ok(), prepare_closed()).encode(env),
            AsyncPayload::Error(reason) => (error(), reason).encode(env),
            AsyncPayload::NotDispatched(reason) => {
                (error(), (not_dispatched(), reason)).encode(env)
            }
        }
    }
}

struct CompletionMessage {
    id: i64,
    payload: AsyncPayload,
}

impl Encoder for CompletionMessage {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        (datalayers_async_result(), self.id, self.payload.encode(env)).encode(env)
    }
}

struct EncodedCompletion {
    env: OwnedEnv,
    message: SavedTerm,
}

/// Cap used when the application environment does not configure a usable one.
///
/// It is kept in sync with the `max_in_flight` default in
/// `src/datalayers.app.src`.
const DEFAULT_MAX_IN_FLIGHT: usize = 1024;
/// Accepted range for the configured cap. Below the minimum no request could ever
/// run; above the maximum the value is meaningless, and it would approach what a
/// semaphore can hold.
const MIN_MAX_IN_FLIGHT: i64 = 1;
const MAX_MAX_IN_FLIGHT: i64 = 1_000_000;

/// Process-wide cap on the number of requests running at the same time.
///
/// It bounds the amount of pending work a stalled server can accumulate: at most
/// this many requests run at once, and anything over the cap waits for a slot
/// under its own request deadline (see [`run_bounded`]).
///
/// One native library serves every connection of every datalayers connector in
/// the node, so the bound is process-wide and can not be a per-connection option.
/// It is set once when the NIF is loaded, from the `datalayers` application
/// environment, and can not be changed at runtime.
static IN_FLIGHT_CAPACITY: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_IN_FLIGHT);
static IN_FLIGHT_LIMITER: OnceLock<Semaphore> = OnceLock::new();
/// CPU-heavy parameter and result conversions share one process-wide bound.
static CONVERSION_LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();

/// Decide the process-wide cap from a configured value.
///
/// Anything outside the accepted range, and anything that is not an integer, is
/// replaced by the default: a nonsensical setting must be able to neither disable
/// the limiter nor throttle the node.
fn resolve_capacity(configured: Option<i64>) -> usize {
    match configured {
        Some(value) if (MIN_MAX_IN_FLIGHT..=MAX_MAX_IN_FLIGHT).contains(&value) => value as usize,
        _ => DEFAULT_MAX_IN_FLIGHT,
    }
}

/// Clamp a capacity to what a semaphore can represent.
///
/// A backstop for [`resolve_capacity`]: `Semaphore::new` panics past
/// [`Semaphore::MAX_PERMITS`], and a panic inside a NIF takes the whole VM down.
fn clamp_permits(capacity: usize) -> usize {
    capacity.min(Semaphore::MAX_PERMITS)
}

/// Resolve the process-wide limiter: created on first use, after the load-time
/// configuration, and reused for the life of the VM.
fn in_flight_limiter() -> &'static Semaphore {
    IN_FLIGHT_LIMITER
        .get_or_init(|| Semaphore::new(clamp_permits(IN_FLIGHT_CAPACITY.load(Ordering::Relaxed))))
}

fn conversion_capacity() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
        .clamp(1, 16)
}

/// A bounded blocking pool for CPU conversion work. A permit is moved into the
/// blocking closure, so abandoned requests can not pile up conversions without
/// bound after their runtime futures are dropped.
fn conversion_limiter() -> &'static Arc<Semaphore> {
    CONVERSION_LIMITER.get_or_init(|| Arc::new(Semaphore::new(conversion_capacity())))
}

/// Configure the process-wide in-flight cap. Called once, when the module is
/// loaded, with the value of the `datalayers` application environment.
///
/// Returns `error` if the limiter is already in use, so a late call can not
/// change the cap of a node that is already serving requests.
#[rustler::nif]
fn set_max_in_flight<'a>(value: Term<'a>) -> rustler::Atom {
    if IN_FLIGHT_LIMITER.get().is_some() {
        return error();
    }
    IN_FLIGHT_CAPACITY.store(
        resolve_capacity(value.decode::<i64>().ok()),
        Ordering::Relaxed,
    );
    // Materialise the limiter now: that fixes the capacity for the life of the VM
    // and makes the guard above reject any later attempt to change it. Without it
    // the cap would stay mutable until the first request materialised it.
    let _ = in_flight_limiter();
    ok()
}

/// A request deadline on the same monotonic clock as Erlang.
#[derive(Clone, Copy, Debug)]
struct RequestDeadline {
    at: Instant,
    budget: Duration,
}

impl RequestDeadline {
    fn new((at_ms, budget_ms): (i64, u64), default: Duration) -> Self {
        let budget = request_budget(budget_ms, default);
        let remaining = erlang_monotonic_ms()
            .and_then(|now_ms| remaining_at(at_ms, now_ms))
            .unwrap_or_default();
        Self::from_remaining(remaining, budget)
    }

    fn from_remaining(remaining: Duration, budget: Duration) -> Self {
        Self {
            at: Instant::now()
                .checked_add(remaining)
                .unwrap_or_else(Instant::now),
            budget,
        }
    }

    fn remaining(self) -> Option<Duration> {
        let remaining = self.at.saturating_duration_since(Instant::now());
        (!remaining.is_zero()).then_some(remaining)
    }
}

fn request_budget(budget_ms: u64, default: Duration) -> Duration {
    match budget_ms {
        ms if ms > 0 && ms <= MAX_REQUEST_TIMEOUT_MS => Duration::from_millis(ms),
        _ => default,
    }
}

#[cfg(not(test))]
fn erlang_monotonic_ms() -> Option<i64> {
    use rustler::sys::{ERL_NIF_TIME_ERROR, ErlNifTimeUnit, enif_monotonic_time};

    let now = unsafe { enif_monotonic_time(ErlNifTimeUnit::ERL_NIF_MSEC) };
    (now != ERL_NIF_TIME_ERROR).then_some(now)
}

#[cfg(test)]
fn erlang_monotonic_ms() -> Option<i64> {
    Some(0)
}

fn remaining_at(deadline_ms: i64, now_ms: i64) -> Option<Duration> {
    let remaining = deadline_ms.saturating_sub(now_ms);
    (remaining > 0).then(|| Duration::from_millis(remaining as u64))
}

fn deadline_failure(deadline: RequestDeadline, dispatched: &AtomicBool) -> Option<AsyncPayload> {
    deadline.remaining().is_none().then(|| {
        if dispatched.load(Ordering::Relaxed) {
            AsyncPayload::Error(format!("[request] timed out after {:?}", deadline.budget))
        } else {
            AsyncPayload::NotDispatched(format!(
                "[request] timed out after {:?} before dispatch",
                deadline.budget
            ))
        }
    })
}

fn pre_dispatch_timeout(deadline: RequestDeadline) -> AsyncPayload {
    AsyncPayload::NotDispatched(format!(
        "[request] timed out after {:?} before dispatch",
        deadline.budget
    ))
}

/// A payload together with the in-flight permit that must cover final encoding.
struct BoundedPayload<'a> {
    payload: AsyncPayload,
    /// Keeping this guard in the result holds the request slot through final
    /// result encoding.
    _permit: Option<SemaphorePermit<'a>>,
}

/// Run `work` under the process-wide in-flight cap and the whole-request deadline.
///
/// The deadline bounds the *entire* request, including the wait for an in-flight
/// slot, so a request can never hold -- or wait for -- a slot for longer than
/// that. The per-stage timeouts inside `Client` (`timed`) still bound each
/// individual await.
async fn run_bounded<'a, F>(
    deadline: RequestDeadline,
    permits: &'a Semaphore,
    dispatched: &AtomicBool,
    work: F,
) -> BoundedPayload<'a>
where
    F: std::future::Future<Output = AsyncPayload>,
{
    let Some(remaining) = deadline.remaining() else {
        return BoundedPayload {
            payload: pre_dispatch_timeout(deadline),
            _permit: None,
        };
    };

    // Waiting for a slot is not dispatched work: report it as such so the caller
    // can tell it apart from a failure that may have reached the server.
    let permit = match tokio::time::timeout(remaining, permits.acquire()).await {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => {
            return BoundedPayload {
                payload: AsyncPayload::Error("[request] in-flight limiter closed".to_string()),
                _permit: None,
            };
        }
        Err(_) => {
            return BoundedPayload {
                payload: AsyncPayload::NotDispatched(format!(
                    "[request] timed out after {:?} waiting for an in-flight slot",
                    deadline.budget
                )),
                _permit: None,
            };
        }
    };

    let Some(remaining) = deadline.remaining() else {
        return BoundedPayload {
            payload: pre_dispatch_timeout(deadline),
            _permit: Some(permit),
        };
    };
    let payload = match tokio::time::timeout(remaining, work).await {
        Ok(payload) => payload,
        Err(_) if dispatched.load(Ordering::Relaxed) => {
            AsyncPayload::Error(format!("[request] timed out after {:?}", deadline.budget))
        }
        Err(_) => AsyncPayload::NotDispatched(format!(
            "[request] timed out after {:?} before dispatch",
            deadline.budget
        )),
    };
    BoundedPayload {
        payload,
        _permit: Some(permit),
    }
}

/// Run `work` under the request limits, turning a panic into a delivered error.
///
/// The task is detached, so a panic would terminate it before any completion was
/// sent. The socket would keep its single in-flight slot occupied forever and the
/// connection would look hung, so panics must never escape this point.
async fn run_guarded<'a, F>(
    deadline: RequestDeadline,
    permits: &'a Semaphore,
    dispatched: &AtomicBool,
    work: F,
) -> BoundedPayload<'a>
where
    F: std::future::Future<Output = AsyncPayload>,
{
    match AssertUnwindSafe(run_bounded(deadline, permits, dispatched, work))
        .catch_unwind()
        .await
    {
        Ok(payload) => payload,
        Err(panic) => BoundedPayload {
            payload: AsyncPayload::Error(format!("[request] panicked: {}", panic_message(panic))),
            _permit: None,
        },
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Race `work` against the connection's cancellation signal.
///
/// Cancelling cannot undo a request the server has already accepted, so the
/// failure is deliberately an ordinary one: the caller must not treat it as safe
/// to retry.
async fn cancellable<F, T>(mut cancelled: watch::Receiver<bool>, work: F) -> Result<T, AsyncPayload>
where
    F: std::future::Future<Output = T>,
{
    if *cancelled.borrow() {
        return Err(AsyncPayload::Error(
            "[request] cancelled: the socket was stopped".to_string(),
        ));
    }
    tokio::select! {
        biased;
        _ = cancelled.changed() => {
            Err(AsyncPayload::Error(
                "[request] cancelled: the socket was stopped".to_string()
            ))
        }
        payload = work => Ok(payload),
    }
}

async fn encode_completion(
    id: i64,
    payload: AsyncPayload,
) -> Result<EncodedCompletion, AsyncPayload> {
    let permit = conversion_limiter()
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AsyncPayload::Error("completion encoder closed".to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let env = OwnedEnv::new();
        let message = env.save(CompletionMessage { id, payload });
        EncodedCompletion { env, message }
    })
    .await
    .map_err(|e| AsyncPayload::Error(format!("completion encoding failed: {e}")))
}

const INLINE_RESULT_CELLS: usize = 256;
const INLINE_RESULT_BYTES: usize = 8192;
const INLINE_PAYLOAD_ROWS: usize = 32;

fn result_batch_is_small(batches: &[RecordBatch]) -> bool {
    batches
        .iter()
        .map(|batch| batch.num_rows().saturating_mul(batch.num_columns()))
        .sum::<usize>()
        <= INLINE_RESULT_CELLS
}

fn payload_is_small(payload: &AsyncPayload) -> bool {
    match payload {
        AsyncPayload::Rows(rows) => {
            let bytes = rows
                .iter()
                .flat_map(|row| row.iter())
                .map(String::len)
                .sum::<usize>();
            rows.len() <= INLINE_PAYLOAD_ROWS && bytes <= INLINE_RESULT_BYTES
        }
        _ => true,
    }
}

fn encode_completion_inline(
    id: i64,
    payload: AsyncPayload,
) -> Result<EncodedCompletion, AsyncPayload> {
    let env = OwnedEnv::new();
    let message = env.save(CompletionMessage { id, payload });
    Ok(EncodedCompletion { env, message })
}

/// Run one request and encode its completion while the in-flight slot is still
/// held. A late encode never sends its success message: only a fully encoded
/// result may leave here before the request deadline.
async fn run_and_encode<F>(
    reply_id: i64,
    deadline: RequestDeadline,
    permits: &Semaphore,
    dispatched: &AtomicBool,
    work: F,
) -> Result<EncodedCompletion, AsyncPayload>
where
    F: std::future::Future<Output = AsyncPayload>,
{
    let bounded = run_guarded(deadline, permits, dispatched, work).await;
    let BoundedPayload { payload, _permit } = bounded;
    let Some(remaining) = deadline.remaining() else {
        return Err(deadline_failure(deadline, dispatched).expect("the deadline expired"));
    };
    let encoded = if payload_is_small(&payload) {
        encode_completion_inline(reply_id, payload)?
    } else {
        match tokio::time::timeout(remaining, encode_completion(reply_id, payload)).await {
            Ok(encoded) => encoded?,
            Err(_) => {
                return Err(deadline_failure(deadline, dispatched).expect("the deadline expired"));
            }
        }
    };
    if let Some(err) = deadline_failure(deadline, dispatched) {
        return Err(err);
    }
    Ok(encoded)
}

fn send_encoded(reply_pid: &LocalPid, encoded: EncodedCompletion) {
    let EncodedCompletion { mut env, message } = encoded;
    let _ = env.send_and_clear(reply_pid, |env| message.load(env));
}

fn send_payload(reply_pid: &LocalPid, id: i64, payload: AsyncPayload) {
    let mut env = OwnedEnv::new();
    let _ = env.send_and_clear(reply_pid, |env| {
        CompletionMessage { id, payload }.encode(env)
    });
}

/// Submit `work` to the shared runtime and deliver its result to `reply_pid`.
///
/// The NIF entry point returns immediately, so no BEAM scheduler is ever blocked
/// by the flight round-trip. The task is bounded by the in-flight cap and the
/// whole-request deadline before it runs.
fn submit<F>(
    reply_pid: LocalPid,
    id: i64,
    deadline: RequestDeadline,
    dispatched: Arc<AtomicBool>,
    cancel: Option<watch::Receiver<bool>>,
    work: F,
) where
    F: std::future::Future<Output = AsyncPayload> + Send + 'static,
{
    RT.spawn(async move {
        let operation = run_and_encode(id, deadline, in_flight_limiter(), &dispatched, work);
        let result = match cancel {
            Some(cancelled) => match cancellable(cancelled, operation).await {
                Ok(result) => result,
                Err(payload) => Err(payload),
            },
            None => operation.await,
        };
        match result {
            Ok(encoded) => send_encoded(&reply_pid, encoded),
            Err(payload) => send_payload(&reply_pid, id, payload),
        }
    });
}

// Decoding connect options copies host, credentials, and TLS paths out of the
// caller's Erlang terms. Keep that work off normal BEAM schedulers.
#[rustler::nif(schedule = "DirtyCpu")]
fn async_connect<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    request: (i64, u64),
    token_ref: Reference<'a>,
    opts: ClientOpts,
) -> NifResult<Term<'a>> {
    let deadline = RequestDeadline::new(request, opts.request_timeout());
    // The connection's default is the *configured* budget, not what is left of this
    // connect attempt's budget: a connect that used 10s of a 15s budget must not
    // leave every later operation with 5s.
    let default_timeout = opts.request_timeout();
    let token: ResourceArc<CancelToken> = match token_ref.decode() {
        Ok(token) => token,
        Err(_) => return Ok((error(), "invalid_cancel_token").encode(env)),
    };
    // Same channel for the connect attempt and, once it succeeds, for every
    // operation on the connection it produced.
    let cancel = token.sender().subscribe();
    let dispatched = Arc::new(AtomicBool::new(false));
    let work_dispatched = Arc::clone(&dispatched);
    submit(
        reply_pid,
        id,
        deadline,
        dispatched,
        Some(cancel),
        async move {
            match Client::try_new(&opts, || work_dispatched.store(true, Ordering::Relaxed)).await {
                Ok(client) => AsyncPayload::Connected(ClientResource::new(
                    client,
                    default_timeout,
                    token.sender(),
                )),
                Err(e) => AsyncPayload::Error(format!("{e:#}")),
            }
        },
    );
    Ok(ok().encode(env))
}

// `Database` is copied out of the caller's binary during NIF decoding.
#[rustler::nif(schedule = "DirtyCpu")]
fn async_use_database<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    request: (i64, u64),
    client_resource_ref: Reference<'a>,
    database: String,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let deadline = RequestDeadline::new(request, client_resource.request_timeout);

    let cancel = client_resource.cancel.subscribe();
    let dispatched = Arc::new(AtomicBool::new(false));
    let work_dispatched = Arc::clone(&dispatched);
    submit(
        reply_pid,
        id,
        deadline,
        dispatched,
        Some(cancel),
        async move {
            let mut client_guard = client_resource.inner.lock().await;
            match &mut *client_guard {
                Some(client) => {
                    work_dispatched.store(true, Ordering::Relaxed);
                    client.use_database(&database);
                    AsyncPayload::UseDatabase
                }
                None => AsyncPayload::Error("client_stopped".to_string()),
            }
        },
    );

    Ok(ok().encode(env))
}

/// execute a SQL query using the client resource.
/// Return the result as a term.
/// example:
/// ```erlang
/// 0> datalayers:execute(Client, <<"SHOW DATABASES">>).
/// {ok,[[<<"information_schema">>, <<"2025-07-08T17:37:16+08:00">>],
///      [<<"rust">>,               <<"2025-06-20T11:15:32+08:00">>]
///     ]} | {error, Reason}
/// ```
// The `String` argument copies the SQL binary before the NIF body runs. Keep that
// copy off normal BEAM schedulers; the Erlang socket also rejects oversized SQL
// before reaching this boundary.
#[rustler::nif(schedule = "DirtyCpu")]
fn async_execute<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    request: (i64, u64),
    client_resource_ref: Reference<'a>,
    sql: String,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let deadline = RequestDeadline::new(request, client_resource.request_timeout);

    let cancel = client_resource.cancel.subscribe();
    let dispatched = Arc::new(AtomicBool::new(false));
    let work_dispatched = Arc::clone(&dispatched);
    submit(
        reply_pid,
        id,
        deadline,
        dispatched,
        Some(cancel),
        async move {
            let mut client_guard = client_resource.inner.lock().await;
            match &mut *client_guard {
                Some(client) => {
                    work_dispatched.store(true, Ordering::Relaxed);
                    match client.execute(&sql).await {
                        Ok(batches) => {
                            convert_result_batches(batches, deadline, &work_dispatched).await
                        }
                        Err(e) => AsyncPayload::Error(format!("{e:#}")),
                    }
                }
                None => AsyncPayload::Error("client_stopped".to_string()),
            }
        },
    );

    Ok(ok().encode(env))
}

// Like `async_execute`, decoding copies the SQL statement. It is bounded by the
// Erlang-side size check and runs on a dirty CPU scheduler.
#[rustler::nif(schedule = "DirtyCpu")]
fn async_prepare<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    request: (i64, u64),
    client_resource_ref: Reference<'a>,
    sql: String,
    auto_rebuild: bool,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let deadline = RequestDeadline::new(request, client_resource.request_timeout);

    let cancel = client_resource.cancel.subscribe();
    let dispatched = Arc::new(AtomicBool::new(false));
    let work_dispatched = Arc::clone(&dispatched);
    submit(
        reply_pid,
        id,
        deadline,
        dispatched,
        Some(cancel),
        async move {
            let mut client_guard = client_resource.inner.lock().await;
            match &mut *client_guard {
                Some(client) => {
                    work_dispatched.store(true, Ordering::Relaxed);
                    match client.prepare(&sql).await {
                        Ok(statement) => match parameter_schema(&statement) {
                            Ok(schema) => AsyncPayload::Prepared(PreparedStatementResource::new(
                                statement,
                                schema,
                                sql,
                                auto_rebuild,
                            )),
                            Err(err) => err,
                        },
                        Err(e) => AsyncPayload::Error(format!("{e:#}")),
                    }
                }
                None => AsyncPayload::Error("client_stopped".to_string()),
            }
        },
    );

    Ok(ok().encode(env))
}

// The only async NIF whose body does work that scales with the caller's input:
// `OwnedEnv::save` copies the whole parameter term and a NIF body can not be
// preempted. Dirty CPU scheduling keeps that copy off the normal schedulers,
// while (unlike dirty IO) not competing with file and log I/O.
#[rustler::nif(schedule = "DirtyCpu")]
fn async_execute_prepare<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    request: (i64, u64),
    client_resource_ref: Reference<'a>,
    statement_resource_ref: Reference<'a>,
    params: Term<'a>,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let statement_resource: ResourceArc<PreparedStatementResource> =
        match statement_resource_ref.decode() {
            Ok(r) => r,
            Err(_) => return Ok((error(), "invalid_statement_resource").encode(env)),
        };
    let deadline = RequestDeadline::new(request, client_resource.request_timeout);
    if deadline.remaining().is_none() {
        return Ok((
            error(),
            (
                not_dispatched(),
                format!(
                    "[request] timed out after {:?} before dispatch",
                    deadline.budget
                ),
            ),
        )
            .encode(env));
    }
    let binding =
        match util::params_to_record_batch(statement_resource.parameter_schema.clone(), params) {
            Ok(binding) => binding,
            Err(rustler::Error::BadArg) => return Ok((error(), "badarg").encode(env)),
            Err(e) => {
                return Ok((error(), format!("invalid_params: {e:?}")).encode(env));
            }
        };
    if deadline.remaining().is_none() {
        return Ok((
            error(),
            (
                not_dispatched(),
                format!(
                    "[request] timed out after {:?} before dispatch",
                    deadline.budget
                ),
            ),
        )
            .encode(env));
    }

    let cancel = client_resource.cancel.subscribe();
    let dispatched = Arc::new(AtomicBool::new(false));
    let work_dispatched = Arc::clone(&dispatched);
    submit(
        reply_pid,
        id,
        deadline,
        dispatched,
        Some(cancel),
        async move {
            let mut client_guard = client_resource.inner.lock().await;
            let Some(client) = &mut *client_guard else {
                return AsyncPayload::Error("client_or_statement_stopped".to_string());
            };
            let mut statement_guard = statement_resource.inner.lock().await;

            // `close_prepared/1` takes the statement out of the resource; rebuild it when
            // the caller asked for auto_rebuild, exactly like the synchronous path does.
            if statement_guard.is_none() {
                if !statement_resource.auto_rebuild {
                    return AsyncPayload::Error("client_or_statement_stopped".to_string());
                }
                // The rebuild sends a Prepare RPC. Once it may have reached the
                // server, this request is no longer safely "not dispatched".
                work_dispatched.store(true, Ordering::Relaxed);
                return match client.prepare(&statement_resource.sql).await {
                    Ok(mut new_statement) => {
                        if let Some(err) = deadline_failure(deadline, &work_dispatched) {
                            return err;
                        }
                        match client.execute_prepared(&mut new_statement, binding).await {
                            Ok(batches) => {
                                *statement_guard = Some(new_statement);
                                convert_result_batches(batches, deadline, &work_dispatched).await
                            }
                            Err(e2) => AsyncPayload::Error(format!("{e2:#}")),
                        }
                    }
                    Err(rebuild_err) => AsyncPayload::Error(format!("{rebuild_err:#}")),
                };
            }
            let statement = match &mut *statement_guard {
                Some(statement) => statement,
                None => return AsyncPayload::Error("client_or_statement_stopped".to_string()),
            };
            if let Some(err) = deadline_failure(deadline, &work_dispatched) {
                return err;
            }
            work_dispatched.store(true, Ordering::Relaxed);
            let retry_binding = statement_resource.auto_rebuild.then(|| binding.clone());

            match client.execute_prepared(statement, binding).await {
                Ok(batches) => convert_result_batches(batches, deadline, &work_dispatched).await,
                Err(e) => {
                    if statement_resource.auto_rebuild && is_prepared_statement_lost(&e) {
                        if let Some(err) = deadline_failure(deadline, &work_dispatched) {
                            return err;
                        }
                        match client.prepare(&statement_resource.sql).await {
                            Ok(mut new_statement) => {
                                if let Some(err) = deadline_failure(deadline, &work_dispatched) {
                                    return err;
                                }
                                let retry_binding =
                                    retry_binding.expect("auto_rebuild has a retry binding");
                                match client
                                    .execute_prepared(&mut new_statement, retry_binding)
                                    .await
                                {
                                    Ok(batches) => {
                                        *statement = new_statement;
                                        convert_result_batches(batches, deadline, &work_dispatched)
                                            .await
                                    }
                                    Err(e2) => AsyncPayload::Error(format!("{e2:#}")),
                                }
                            }
                            Err(rebuild_err) => AsyncPayload::Error(format!("{rebuild_err:#}")),
                        }
                    } else {
                        AsyncPayload::Error(format!("{e:#}"))
                    }
                }
            }
        },
    );

    Ok(ok().encode(env))
}

#[rustler::nif]
fn async_close_prepared<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    request: (i64, u64),
    client_resource_ref: Reference<'a>,
    statement_resource_ref: Reference<'a>,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let statement_resource: ResourceArc<PreparedStatementResource> =
        match statement_resource_ref.decode() {
            Ok(r) => r,
            Err(_) => return Ok((error(), "invalid_statement_resource").encode(env)),
        };
    let deadline = RequestDeadline::new(request, client_resource.request_timeout);

    let cancel = client_resource.cancel.subscribe();
    let dispatched = Arc::new(AtomicBool::new(false));
    let work_dispatched = Arc::clone(&dispatched);
    submit(
        reply_pid,
        id,
        deadline,
        dispatched,
        Some(cancel),
        async move {
            let client_guard = client_resource.inner.lock().await;
            let mut statement_guard = statement_resource.inner.lock().await;
            if let (Some(client), Some(statement)) = (&*client_guard, statement_guard.take()) {
                work_dispatched.store(true, Ordering::Relaxed);
                match client.close_prepared(statement).await {
                    Ok(_) => AsyncPayload::Closed,
                    Err(e) => AsyncPayload::Error(format!("{e:#}")),
                }
            } else {
                AsyncPayload::Error("client_or_statement_stopped".to_string())
            }
        },
    );

    Ok(ok().encode(env))
}

/// Drop the client (closing the underlying channel) without blocking the caller.
///
/// The caller is not waiting for a reply: the value is only used to release the
/// gRPC channel when a socket is stopped.
#[rustler::nif]
fn stop(client_resource_ref: Reference) -> rustler::Atom {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return error(),
    };

    // Let the in-flight operation give up first: it releases the client lock, so
    // the task below does not have to wait for it to run into its own deadline.
    client_resource.cancel_in_flight();
    RT.spawn(async move {
        let _ = client_resource.inner.lock().await.take();
    });

    ok()
}

fn parameter_schema(statement: &PreparedStatement<Channel>) -> Result<Arc<Schema>, AsyncPayload> {
    statement
        .parameter_schema()
        .map(|schema| Arc::new((*schema).clone()))
        .map_err(|e| AsyncPayload::Error(format!("invalid_params: {e}")))
}

/// Convert returned Arrow batches on a bounded blocking worker. The conversion
/// itself is not preemptible, but the runtime future remains cancellable and the
/// permit stays held until the blocking closure actually finishes.
async fn convert_result_batches(
    batches: Vec<RecordBatch>,
    deadline: RequestDeadline,
    dispatched: &AtomicBool,
) -> AsyncPayload {
    if let Some(err) = deadline_failure(deadline, dispatched) {
        return err;
    }
    if result_batch_is_small(&batches) {
        let rows = util::record_batch_to_term(&batches);
        return deadline_failure(deadline, dispatched).unwrap_or_else(|| AsyncPayload::Rows(rows));
    }
    let permit = match conversion_limiter().clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => {
            return AsyncPayload::Error("result conversion limiter closed".to_string());
        }
    };
    let converted = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        util::record_batch_to_term(&batches)
    })
    .await;
    match converted {
        Ok(rows) => {
            deadline_failure(deadline, dispatched).unwrap_or_else(|| AsyncPayload::Rows(rows))
        }
        Err(e) => AsyncPayload::Error(format!("result conversion failed: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AsyncPayload, RequestDeadline, cancellable, clamp_permits, convert_result_batches,
        in_flight_limiter, is_prepared_statement_lost, remaining_at, request_budget,
        resolve_capacity, run_bounded, run_guarded,
    };
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    use tokio::sync::Semaphore;

    fn deadline_after(budget: Duration) -> RequestDeadline {
        RequestDeadline::from_remaining(budget, budget)
    }

    #[test]
    fn not_found_is_lost() {
        let err = anyhow::Error::new(tonic::Status::not_found("gone"));
        assert!(is_prepared_statement_lost(&err));
    }

    #[test]
    fn other_status_is_not_lost() {
        let err = anyhow::Error::new(tonic::Status::internal("boom"));
        assert!(!is_prepared_statement_lost(&err));
    }

    #[test]
    fn plain_error_is_not_lost() {
        let err = anyhow::anyhow!("plain");
        assert!(!is_prepared_statement_lost(&err));
    }

    #[test]
    fn wrapped_not_found_is_lost() {
        let err = anyhow::Error::new(tonic::Status::not_found("gone")).context("outer");
        assert!(is_prepared_statement_lost(&err));
    }

    #[tokio::test]
    async fn the_request_deadline_bounds_a_hanging_operation() {
        let permits = Semaphore::new(4);
        let dispatched = Arc::new(AtomicBool::new(false));
        let work_dispatched = Arc::clone(&dispatched);
        let out = run_bounded(
            deadline_after(Duration::from_millis(50)),
            &permits,
            &dispatched,
            async move {
                work_dispatched.store(true, Ordering::Relaxed);
                std::future::pending::<AsyncPayload>().await
            },
        )
        .await;
        match out.payload {
            AsyncPayload::Error(msg) => {
                assert!(msg.contains("timed out after"), "unexpected error: {msg}");
            }
            _ => panic!("a hanging request must be bounded by the request deadline"),
        }
    }

    #[tokio::test]
    async fn a_fast_operation_is_not_affected_by_the_deadline() {
        let permits = Semaphore::new(4);
        let dispatched = AtomicBool::new(false);
        let out = run_bounded(
            deadline_after(Duration::from_secs(30)),
            &permits,
            &dispatched,
            async { AsyncPayload::Rows(vec![vec!["ok".to_string()]]) },
        )
        .await;
        match out.payload {
            AsyncPayload::Rows(rows) => assert_eq!(rows, vec![vec!["ok".to_string()]]),
            _ => panic!("a fast request must run to completion"),
        }
    }

    /// The cap is what bounds pending work: a request that cannot get a slot keeps
    /// waiting until its own deadline, then fails instead of piling up forever.
    #[tokio::test]
    async fn requests_over_the_in_flight_cap_wait_for_their_deadline() {
        let permits = Semaphore::new(1);
        let dispatched = AtomicBool::new(false);
        // Hold the only permit, as if another request were in flight.
        let held = permits.acquire().await.expect("acquire the only permit");

        let blocked = run_bounded(
            deadline_after(Duration::from_millis(50)),
            &permits,
            &dispatched,
            async { AsyncPayload::Closed },
        )
        .await;
        match blocked.payload {
            // It never ran, so the caller may safely retry it.
            AsyncPayload::NotDispatched(msg) => {
                assert!(msg.contains("in-flight slot"), "unexpected error: {msg}");
            }
            _ => panic!("a request over the cap must not run while the permit is held"),
        }

        drop(held);
        let allowed = run_bounded(
            deadline_after(Duration::from_millis(50)),
            &permits,
            &dispatched,
            async { AsyncPayload::Closed },
        )
        .await;
        assert!(matches!(allowed.payload, AsyncPayload::Closed));
    }

    /// A panic inside the detached task must not swallow the completion: the
    /// socket would keep its single in-flight slot occupied forever.
    #[tokio::test]
    async fn a_panicking_operation_is_delivered_as_an_error() {
        let permits = Semaphore::new(4);
        let dispatched = AtomicBool::new(false);
        let out = run_guarded(
            deadline_after(Duration::from_secs(5)),
            &permits,
            &dispatched,
            async {
                panic!("boom");
            },
        )
        .await;
        match out.payload {
            AsyncPayload::Error(msg) => {
                assert!(
                    msg.contains("panicked") && msg.contains("boom"),
                    "unexpected error: {msg}"
                );
            }
            _ => panic!("a panic must be converted into a delivered error"),
        }
    }

    #[test]
    fn the_request_budget_falls_back_to_the_connection_default() {
        assert_eq!(
            request_budget(0, Duration::from_secs(15)),
            Duration::from_secs(15)
        );
        assert_eq!(
            request_budget(250, Duration::from_secs(15)),
            Duration::from_millis(250)
        );
        assert_eq!(
            request_budget(u64::MAX, Duration::from_secs(15)),
            Duration::from_secs(15)
        );
    }

    #[test]
    fn remaining_budget_uses_the_absolute_deadline() {
        assert_eq!(remaining_at(1_250, 1_000), Some(Duration::from_millis(250)));
        assert_eq!(remaining_at(1_000, 1_000), None);
        assert_eq!(remaining_at(999, 1_000), None);
    }

    /// Time spent copying or waiting to enter the runtime counts against the
    /// socket's original deadline. Expired work must not run at all.
    #[tokio::test]
    async fn a_deadline_that_expired_before_polling_rejects_work() {
        let permits = Semaphore::new(1);
        let dispatched = AtomicBool::new(false);
        let ran = Arc::new(AtomicBool::new(false));
        let work_ran = Arc::clone(&ran);
        let deadline = deadline_after(Duration::from_millis(20));

        tokio::time::sleep(Duration::from_millis(40)).await;
        let out = run_bounded(deadline, &permits, &dispatched, async move {
            work_ran.store(true, Ordering::Relaxed);
            AsyncPayload::Closed
        })
        .await;

        match out.payload {
            AsyncPayload::NotDispatched(message) => {
                assert!(message.contains("[request] timed out after"), "{message}");
                assert!(message.contains("before dispatch"), "{message}");
            }
            _ => panic!("expired work must be rejected before dispatch"),
        }
        assert!(!ran.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn result_conversion_cannot_complete_after_the_deadline() {
        let rows = 200_000;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let values = Arc::new(Int64Array::from_iter_values(0..rows));
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        let permits = Semaphore::new(1);
        let dispatched = Arc::new(AtomicBool::new(true));
        let work_dispatched = Arc::clone(&dispatched);
        let deadline = deadline_after(Duration::from_millis(1));

        let out = run_bounded(deadline, &permits, &dispatched, async move {
            convert_result_batches(vec![batch], deadline, &work_dispatched).await
        })
        .await;

        assert!(
            matches!(out.payload, AsyncPayload::Error(message) if message.contains("timed out after")),
            "result conversion completed after the request deadline"
        );
    }

    /// The cap is a node-level setting: a nonsensical value falls back to the
    /// default instead of disabling the limiter or throttling the node.
    #[test]
    fn the_in_flight_cap_falls_back_to_the_default_when_unusable() {
        assert_eq!(resolve_capacity(None), 1024);
        assert_eq!(resolve_capacity(Some(0)), 1024);
        assert_eq!(resolve_capacity(Some(-5)), 1024);
        assert_eq!(resolve_capacity(Some(2_000_000)), 1024);
        assert_eq!(resolve_capacity(Some(4)), 4);
    }

    /// The limiter is process-wide and resolved once, for the life of the VM.
    #[test]
    fn the_in_flight_limiter_is_resolved_once() {
        assert!(std::ptr::eq(in_flight_limiter(), in_flight_limiter()));
    }

    /// Stopping the socket must make the in-flight operation give up promptly
    /// instead of letting it run until its deadline.
    #[tokio::test]
    async fn a_cancelled_operation_gives_up_promptly() {
        let (cancel, _keep) = tokio::sync::watch::channel(false);
        let sender = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sender.send_replace(true);
        });

        let started = Instant::now();
        let out = cancellable(cancel.subscribe(), std::future::pending::<AsyncPayload>()).await;
        let elapsed = started.elapsed();

        match out {
            Err(AsyncPayload::Error(msg)) => {
                assert!(msg.contains("cancelled"), "unexpected error: {msg}");
            }
            _ => panic!("a cancelled operation must not run to completion"),
        }
        assert!(elapsed < Duration::from_secs(1), "took {elapsed:?}");
    }

    /// Cancellation state must survive the gap between the socket death signal
    /// and the NIF subscribing to the token.
    #[tokio::test]
    async fn cancellation_before_subscribe_is_not_lost() {
        let (cancel, _keep) = tokio::sync::watch::channel(false);
        cancel.send_replace(true);
        let out = tokio::time::timeout(
            Duration::from_millis(100),
            cancellable(cancel.subscribe(), std::future::pending::<AsyncPayload>()),
        )
        .await
        .expect("cancellation was lost before subscription");
        assert!(matches!(out, Err(AsyncPayload::Error(message)) if message.contains("cancelled")));
    }

    /// `max_in_flight` is an arbitrary decoded integer and `Semaphore::new` panics
    /// past `MAX_PERMITS`; a panic inside a NIF takes the VM down, so an
    /// out-of-range configuration must be clamped before it reaches the
    /// constructor.
    #[test]
    fn an_out_of_range_in_flight_capacity_is_clamped_and_does_not_panic() {
        assert_eq!(clamp_permits(7), 7);
        assert_eq!(clamp_permits(usize::MAX), Semaphore::MAX_PERMITS);
        assert_eq!(
            Semaphore::new(clamp_permits(usize::MAX)).available_permits(),
            Semaphore::MAX_PERMITS
        );
    }
}
