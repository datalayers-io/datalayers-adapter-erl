extern crate core;
extern crate rustler;

mod atoms;
mod client;
mod client_opts;
mod resource;
mod types;
mod util;

use crate::resource::{ClientResource, PreparedStatementResource};
use arrow_array::RecordBatch;
use arrow_flight::sql::client::PreparedStatement;
use atoms::*;
use client::Client;
use client_opts::ClientOpts;
use futures::FutureExt;
use lazy_static::lazy_static;
use rustler::env::{OwnedEnv, SavedTerm};
use rustler::types::LocalPid;
use rustler::{Encoder, Env, NifResult, Reference, ResourceArc, Term};
use std::panic::AssertUnwindSafe;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::{Semaphore, broadcast};
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
    true
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
    fn encode<'a>(self, env: Env<'a>) -> Term<'a> {
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

/// Run `work` under the process-wide in-flight cap and the whole-request deadline.
///
/// The deadline bounds the *entire* request, including the wait for an in-flight
/// slot, so a request can never hold -- or wait for -- a slot for longer than
/// that. The per-stage timeouts inside `Client` (`timed`) still bound each
/// individual await.
async fn run_bounded<F>(deadline: Duration, permits: &Semaphore, work: F) -> AsyncPayload
where
    F: std::future::Future<Output = AsyncPayload>,
{
    let started = Instant::now();

    // Waiting for a slot is not dispatched work: report it as such so the caller
    // can tell it apart from a failure that may have reached the server.
    let permit = match tokio::time::timeout(deadline, permits.acquire()).await {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => {
            return AsyncPayload::Error("[request] in-flight limiter closed".to_string());
        }
        Err(_) => {
            return AsyncPayload::NotDispatched(format!(
                "[request] timed out after {deadline:?} waiting for an in-flight slot"
            ));
        }
    };

    // Once the work has started the server may already have accepted the write,
    // so a timeout here is an ordinary, non-retriable failure.
    let remaining = deadline.saturating_sub(started.elapsed());
    let payload = match tokio::time::timeout(remaining, work).await {
        Ok(payload) => payload,
        Err(_) => AsyncPayload::Error(format!("[request] timed out after {deadline:?}")),
    };
    drop(permit);
    payload
}

/// Run `work` under the request limits, turning a panic into a delivered error.
///
/// The task is detached, so a panic would terminate it before any completion was
/// sent. The socket would keep its single in-flight slot occupied forever and the
/// connection would look hung, so panics must never escape this point.
async fn run_guarded<F>(deadline: Duration, permits: &Semaphore, work: F) -> AsyncPayload
where
    F: std::future::Future<Output = AsyncPayload>,
{
    match AssertUnwindSafe(run_bounded(deadline, permits, work))
        .catch_unwind()
        .await
    {
        Ok(payload) => payload,
        Err(panic) => AsyncPayload::Error(format!("[request] panicked: {}", panic_message(panic))),
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
async fn cancellable<F>(mut cancelled: broadcast::Receiver<()>, work: F) -> AsyncPayload
where
    F: std::future::Future<Output = AsyncPayload>,
{
    tokio::select! {
        biased;
        _ = cancelled.recv() => {
            AsyncPayload::Error("[request] cancelled: the socket was stopped".to_string())
        }
        payload = work => payload,
    }
}

/// The deadline a request must finish by: the per-request budget handed down by
/// the socket, or the connection's own default when the caller passes `0`.
fn request_deadline(timeout_ms: u64, default: Duration) -> Duration {
    match timeout_ms {
        0 => default,
        ms => Duration::from_millis(ms),
    }
}

/// Submit `work` to the shared runtime and deliver its result to `reply_pid`.
///
/// The NIF entry point returns immediately, so no BEAM scheduler is ever blocked
/// by the flight round-trip. The task is bounded by the in-flight cap and the
/// whole-request deadline before it runs.
fn submit<F>(
    reply_pid: LocalPid,
    id: i64,
    deadline: Duration,
    cancel: Option<broadcast::Receiver<()>>,
    work: F,
) where
    F: std::future::Future<Output = AsyncPayload> + Send + 'static,
{
    RT.spawn(async move {
        let work = run_guarded(deadline, in_flight_limiter(), work);
        let payload = match cancel {
            Some(cancelled) => cancellable(cancelled, work).await,
            None => work.await,
        };
        let mut env = OwnedEnv::new();
        let _ = env.send_and_clear(&reply_pid, |env| {
            (datalayers_async_result(), id, payload.encode(env)).encode(env)
        });
    });
}

#[rustler::nif]
fn async_connect<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    timeout_ms: u64,
    opts: ClientOpts,
) -> NifResult<Term<'a>> {
    let deadline = request_deadline(timeout_ms, opts.request_timeout());
    // The connection's default is the *configured* budget, not what is left of this
    // connect attempt's budget: a connect that used 10s of a 15s budget must not
    // leave every later operation with 5s.
    let default_timeout = opts.request_timeout();
    // A connection that is still being set up has no resource to cancel yet; its
    // own budget bounds it.
    submit(reply_pid, id, deadline, None, async move {
        match Client::try_new(&opts).await {
            Ok(client) => AsyncPayload::Connected(ClientResource::new(client, default_timeout)),
            Err(e) => AsyncPayload::Error(format!("{e:#}")),
        }
    });
    Ok(ok().encode(env))
}

#[rustler::nif]
fn async_use_database<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    timeout_ms: u64,
    client_resource_ref: Reference<'a>,
    database: String,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let request_timeout = request_deadline(timeout_ms, client_resource.request_timeout);

    let cancel = client_resource.cancel.subscribe();
    submit(reply_pid, id, request_timeout, Some(cancel), async move {
        let mut client_guard = client_resource.inner.lock().await;
        match &mut *client_guard {
            Some(client) => {
                client.use_database(&database);
                AsyncPayload::UseDatabase
            }
            None => AsyncPayload::Error("client_stopped".to_string()),
        }
    });

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
#[rustler::nif]
fn async_execute<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    timeout_ms: u64,
    client_resource_ref: Reference<'a>,
    sql: String,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let request_timeout = request_deadline(timeout_ms, client_resource.request_timeout);

    let cancel = client_resource.cancel.subscribe();
    submit(reply_pid, id, request_timeout, Some(cancel), async move {
        let mut client_guard = client_resource.inner.lock().await;
        match &mut *client_guard {
            Some(client) => match client.execute(&sql).await {
                Ok(batches) => AsyncPayload::Rows(util::record_batch_to_term(&batches[..])),
                Err(e) => AsyncPayload::Error(format!("{e:#}")),
            },
            None => AsyncPayload::Error("client_stopped".to_string()),
        }
    });

    Ok(ok().encode(env))
}

#[rustler::nif]
fn async_prepare<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    timeout_ms: u64,
    client_resource_ref: Reference<'a>,
    sql: String,
    auto_rebuild: bool,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let request_timeout = request_deadline(timeout_ms, client_resource.request_timeout);

    let cancel = client_resource.cancel.subscribe();
    submit(reply_pid, id, request_timeout, Some(cancel), async move {
        let mut client_guard = client_resource.inner.lock().await;
        match &mut *client_guard {
            Some(client) => match client.prepare(&sql).await {
                Ok(statement) => AsyncPayload::Prepared(PreparedStatementResource::new(
                    statement,
                    sql,
                    auto_rebuild,
                )),
                Err(e) => AsyncPayload::Error(format!("{e:#}")),
            },
            None => AsyncPayload::Error("client_stopped".to_string()),
        }
    });

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
    timeout_ms: u64,
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
    let request_timeout = request_deadline(timeout_ms, client_resource.request_timeout);

    // Only the term copy happens on the BEAM scheduler. Locking the resources and
    // converting the parameter matrix into an Arrow `RecordBatch` both scale with
    // the input size, so they happen on the task thread; the parameters travel
    // there through an owned environment.
    let owned_env = OwnedEnv::new();
    let saved_params = owned_env.save(params);

    let cancel = client_resource.cancel.subscribe();
    submit(reply_pid, id, request_timeout, Some(cancel), async move {
        let mut client_guard = client_resource.inner.lock().await;
        let Some(client) = &mut *client_guard else {
            return AsyncPayload::Error("client_or_statement_stopped".to_string());
        };
        let mut statement_guard = statement_resource.inner.lock().await;

        // `close_prepared/1` takes the statement out of the resource; rebuild it when
        // the caller asked for auto_rebuild, exactly like the synchronous path does.
        //
        // `&OwnedEnv` is `Send` but not `Sync`, so a borrow of it must never be held
        // across an await or the whole task future stops being `Send`. The parameter
        // binding is therefore built in a synchronous scope, between the awaits.
        if statement_guard.is_none() {
            if !statement_resource.auto_rebuild {
                return AsyncPayload::Error("client_or_statement_stopped".to_string());
            }
            return match client.prepare(&statement_resource.sql).await {
                Ok(mut new_statement) => {
                    let binding =
                        match binding_from_saved(&owned_env, &saved_params, &new_statement) {
                            Ok(binding) => binding,
                            Err(err) => return err,
                        };
                    match client.execute_prepared(&mut new_statement, binding).await {
                        Ok(batches) => {
                            *statement_guard = Some(new_statement);
                            AsyncPayload::Rows(util::record_batch_to_term(&batches[..]))
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

        let binding = match binding_from_saved(&owned_env, &saved_params, statement) {
            Ok(binding) => binding,
            Err(err) => return err,
        };

        match client.execute_prepared(statement, binding).await {
            Ok(batches) => AsyncPayload::Rows(util::record_batch_to_term(&batches[..])),
            Err(e) => {
                if statement_resource.auto_rebuild && is_prepared_statement_lost(&e) {
                    match client.prepare(&statement_resource.sql).await {
                        Ok(mut new_statement) => {
                            let new_binding =
                                match binding_from_saved(&owned_env, &saved_params, &new_statement)
                                {
                                    Ok(binding) => binding,
                                    Err(err) => return err,
                                };
                            match client
                                .execute_prepared(&mut new_statement, new_binding)
                                .await
                            {
                                Ok(batches) => {
                                    *statement = new_statement;
                                    AsyncPayload::Rows(util::record_batch_to_term(&batches[..]))
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
    });

    Ok(ok().encode(env))
}

#[rustler::nif]
fn async_close_prepared<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    timeout_ms: u64,
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
    let request_timeout = request_deadline(timeout_ms, client_resource.request_timeout);

    let cancel = client_resource.cancel.subscribe();
    submit(reply_pid, id, request_timeout, Some(cancel), async move {
        let client_guard = client_resource.inner.lock().await;
        let mut statement_guard = statement_resource.inner.lock().await;
        if let (Some(client), Some(statement)) = (&*client_guard, statement_guard.take()) {
            match client.close_prepared(statement).await {
                Ok(_) => AsyncPayload::Closed,
                Err(e) => AsyncPayload::Error(format!("{e:#}")),
            }
        } else {
            AsyncPayload::Error("client_or_statement_stopped".to_string())
        }
    });

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

/// Build the Arrow binding for `statement` from parameters saved in `owned_env`.
///
/// This runs on the task thread: converting the parameter matrix scales with the
/// input size, so it must not happen on a BEAM scheduler.
fn binding_from_saved(
    owned_env: &OwnedEnv,
    saved_params: &SavedTerm,
    statement: &PreparedStatement<Channel>,
) -> Result<RecordBatch, AsyncPayload> {
    owned_env.run(|env| {
        let params = saved_params.load(env);
        match util::params_to_record_batch(statement, params) {
            Ok(binding) => Ok(binding),
            Err(rustler::Error::BadArg) => Err(AsyncPayload::Error("badarg".to_string())),
            Err(e) => Err(AsyncPayload::Error(format!("invalid_params: {e:?}"))),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AsyncPayload, cancellable, clamp_permits, in_flight_limiter, is_prepared_statement_lost,
        request_deadline, resolve_capacity, run_bounded, run_guarded,
    };
    use std::time::{Duration, Instant};
    use tokio::sync::Semaphore;

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
        let out = run_bounded(
            Duration::from_millis(50),
            &permits,
            std::future::pending::<AsyncPayload>(),
        )
        .await;
        match out {
            AsyncPayload::Error(msg) => {
                assert!(msg.contains("timed out after"), "unexpected error: {msg}");
            }
            _ => panic!("a hanging request must be bounded by the request deadline"),
        }
    }

    #[tokio::test]
    async fn a_fast_operation_is_not_affected_by_the_deadline() {
        let permits = Semaphore::new(4);
        let out = run_bounded(Duration::from_secs(30), &permits, async {
            AsyncPayload::Rows(vec![vec!["ok".to_string()]])
        })
        .await;
        match out {
            AsyncPayload::Rows(rows) => assert_eq!(rows, vec![vec!["ok".to_string()]]),
            _ => panic!("a fast request must run to completion"),
        }
    }

    /// The cap is what bounds pending work: a request that cannot get a slot keeps
    /// waiting until its own deadline, then fails instead of piling up forever.
    #[tokio::test]
    async fn requests_over_the_in_flight_cap_wait_for_their_deadline() {
        let permits = Semaphore::new(1);
        // Hold the only permit, as if another request were in flight.
        let held = permits.acquire().await.expect("acquire the only permit");

        let blocked = run_bounded(Duration::from_millis(50), &permits, async {
            AsyncPayload::Closed
        })
        .await;
        match blocked {
            // It never ran, so the caller may safely retry it.
            AsyncPayload::NotDispatched(msg) => {
                assert!(msg.contains("in-flight slot"), "unexpected error: {msg}");
            }
            _ => panic!("a request over the cap must not run while the permit is held"),
        }

        drop(held);
        let allowed = run_bounded(Duration::from_millis(50), &permits, async {
            AsyncPayload::Closed
        })
        .await;
        assert!(matches!(allowed, AsyncPayload::Closed));
    }

    /// A panic inside the detached task must not swallow the completion: the
    /// socket would keep its single in-flight slot occupied forever.
    #[tokio::test]
    async fn a_panicking_operation_is_delivered_as_an_error() {
        let permits = Semaphore::new(4);
        let out = run_guarded(Duration::from_secs(5), &permits, async {
            panic!("boom");
        })
        .await;
        match out {
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
            request_deadline(0, Duration::from_secs(15)),
            Duration::from_secs(15)
        );
        assert_eq!(
            request_deadline(250, Duration::from_secs(15)),
            Duration::from_millis(250)
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
        let (cancel, _keep) = tokio::sync::broadcast::channel(1);
        let sender = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = sender.send(());
        });

        let started = Instant::now();
        let out = cancellable(cancel.subscribe(), std::future::pending::<AsyncPayload>()).await;
        let elapsed = started.elapsed();

        match out {
            AsyncPayload::Error(msg) => {
                assert!(msg.contains("cancelled"), "unexpected error: {msg}");
            }
            _ => panic!("a cancelled operation must not run to completion"),
        }
        assert!(elapsed < Duration::from_secs(1), "took {elapsed:?}");
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
