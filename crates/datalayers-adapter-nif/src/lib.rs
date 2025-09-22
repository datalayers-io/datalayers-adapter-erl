extern crate core;
extern crate rustler;

mod atoms;
mod client;
mod client_opts;
mod resource;
mod types;
mod util;

use crate::resource::{ClientResource, PreparedStatementResource};
use atoms::*;
use client::Client;
use client_opts::ClientOpts;
use lazy_static::lazy_static;
use rustler::{Encoder, Env, NifResult, Reference, ResourceArc, Term};
use tokio::runtime::Runtime;

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

#[rustler::nif(schedule = "DirtyIo")]
fn connect(env: Env, opts: ClientOpts) -> NifResult<Term> {
    let result_term = match RT.block_on(Client::try_new(&opts)) {
        Ok(client) => (ok(), ClientResource::new(client)).encode(env),
        Err(e) => (error(), e.to_string()).encode(env),
    };
    Ok(result_term)
}

#[rustler::nif(schedule = "DirtyIo")]
fn use_database<'a>(
    env: Env<'a>,
    resource_ref: Reference<'a>,
    database: String,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };

    let mut client_guard = match client_resource.0.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            return Ok((error(), format!("lock poisoned: {poisoned}")).encode(env));
        }
    };

    let result_term = if let Some(client) = &mut *client_guard {
        client.use_database(&database);
        (ok(), "database_changed").encode(env)
    } else {
        (error(), "client_stopped".to_string()).encode(env)
    };
    Ok(result_term)
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
#[rustler::nif(schedule = "DirtyIo")]
fn execute<'a>(env: Env<'a>, resource_ref: Reference<'a>, sql: String) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };

    let mut client_guard = match client_resource.0.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            return Ok((error(), format!("lock poisoned: {poisoned}")).encode(env));
        }
    };

    let result_term = if let Some(client) = &mut *client_guard {
        match RT.block_on(client.execute(&sql)) {
            Ok(result) => (ok(), util::record_batch_to_term(&result[..])).encode(env),
            Err(e) => (error(), e.to_string()).encode(env),
        }
    } else {
        (error(), "client_stopped".to_string()).encode(env)
    };
    Ok(result_term)
}

#[rustler::nif(schedule = "DirtyIo")]
fn prepare<'a>(
    env: Env<'a>,
    client_resource_ref: Reference<'a>,
    sql: String,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };

    let mut client_guard = match client_resource.0.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            return Ok((error(), format!("lock poisoned: {poisoned}")).encode(env));
        }
    };
    let result_term = if let Some(client) = &mut *client_guard {
        match RT.block_on(client.prepare(&sql)) {
            Ok(statement) => (ok(), PreparedStatementResource::new(statement)).encode(env),
            Err(e) => (error(), e.to_string()).encode(env),
        }
    } else {
        (error(), "client_stopped".to_string()).encode(env)
    };
    Ok(result_term)
}

#[rustler::nif(schedule = "DirtyIo")]
fn execute_prepare<'a>(
    env: Env<'a>,
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

    let mut client_guard = match client_resource.0.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            return Ok((error(), format!("client lock poisoned: {poisoned}")).encode(env));
        }
    };
    let mut statement_guard = match statement_resource.0.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            return Ok((error(), format!("statement lock poisoned: {poisoned}")).encode(env));
        }
    };

    let result_term =
        if let (Some(client), Some(statement)) = (&mut *client_guard, &mut *statement_guard) {
            let binding = match util::params_to_record_batch(statement, params) {
                Ok(rb) => rb,
                Err(rustler::Error::BadArg) => return Ok((error(), "badarg").encode(env)),
                Err(e) => return Ok((error(), format!("invalid_params: {e:?}")).encode(env)),
            };

            match RT.block_on(client.execute_prepared(statement, binding)) {
                Ok(result) => (ok(), util::record_batch_to_term(&result[..])).encode(env),
                Err(e) => (error(), e.to_string()).encode(env),
            }
        } else {
            (error(), "client_or_statement_stopped".to_string()).encode(env)
        };
    Ok(result_term)
}

#[rustler::nif(schedule = "DirtyIo")]
fn close_prepared<'a>(
    env: Env<'a>,
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

    let client_guard = match client_resource.0.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            return Ok((error(), format!("client lock poisoned: {poisoned}")).encode(env));
        }
    };
    let mut statement_guard = match statement_resource.0.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            return Ok((error(), format!("statement lock poisoned: {poisoned}")).encode(env));
        }
    };

    let result_term =
        if let (Some(client), Some(statement)) = (&*client_guard, statement_guard.take()) {
            match RT.block_on(client.close_prepared(statement)) {
                Ok(_) => (ok(), prepare_closed()).encode(env),
                Err(e) => (error(), e.to_string()).encode(env),
            }
        } else {
            (error(), "client_or_statement_stopped".to_string()).encode(env)
        };
    Ok(result_term)
}

#[rustler::nif(schedule = "DirtyIo")]
fn stop(client_resource_ref: Reference) -> rustler::Atom {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return error(),
    };

    let mut client_guard = match client_resource.0.lock() {
        Ok(guard) => guard,
        Err(_) => return error(),
    };

    if let Some(client) = client_guard.take() {
        RT.block_on(client.stop());
    }

    ok()
}
