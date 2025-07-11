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
use rustler::{Env, Reference, ResourceArc, Term};
use tokio::runtime::Runtime;

rustler::init!("datalayers_nif", load = on_load);

lazy_static! {
    static ref RT: Runtime = Runtime::new().unwrap();
}

pub fn on_load(env: Env, _load_info: Term) -> bool {
    let _ = env.register::<ClientResource>();
    let _ = env.register::<PreparedStatementResource>();
    true
}

#[rustler::nif(schedule = "DirtyIo")]
fn connect(opts: ClientOpts) -> Result<ResourceArc<ClientResource>, String> {
    match RT.block_on(Client::try_new(&opts)) {
        Ok(client) => Ok(ClientResource::new(client)),
        Err(e) => Err(e.to_string()),
    }
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
fn execute(resource: Reference, sql: String) -> Result<Vec<Vec<String>>, String> {
    let client_resource: rustler::ResourceArc<ClientResource> = match resource.decode() {
        Ok(r) => r,
        Err(_) => return Err("invalid_client_resource".to_string()),
    };

    let mut client_guard = client_resource.0.lock().unwrap();

    if let Some(client) = &mut *client_guard {
        match RT.block_on(client.execute(&sql)) {
            Ok(result) => Ok(util::record_batch_to_term(&result[..])),
            Err(e) => Err(e.to_string()),
        }
    } else {
        Err("client_stopped".to_string())
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn prepare(
    client_resource: ResourceArc<ClientResource>,
    sql: String,
) -> Result<ResourceArc<PreparedStatementResource>, String> {
    let mut client_guard = client_resource.0.lock().unwrap();
    if let Some(client) = &mut *client_guard {
        match RT.block_on(client.prepare(&sql)) {
            Ok(statement) => Ok(PreparedStatementResource::new(statement)),
            Err(e) => Err(e.to_string()),
        }
    } else {
        Err("client_stopped".to_string())
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn execute_prepare(
    client_resource: ResourceArc<ClientResource>,
    statement_resource: ResourceArc<PreparedStatementResource>,
    // the parameters for the prepared statement. is list(list(term())) in Erlnag
    params: Term,
) -> Result<Vec<Vec<String>>, String> {
    let mut client_guard = client_resource.0.lock().unwrap();
    let mut statement_guard = statement_resource.0.lock().unwrap();

    if let (Some(client), Some(statement)) = (&mut *client_guard, &mut *statement_guard) {
        let binding = match util::params_to_record_batch(statement, params) {
            Ok(rb) => rb,
            Err(e) => return Err(format!("invalid_params: {:?}", e)),
        };

        match RT.block_on(client.execute_prepared(statement, binding)) {
            Ok(result) => Ok(util::record_batch_to_term(&result[..])),
            Err(e) => Err(e.to_string()),
        }
    } else {
        Err("client_or_statement_stopped".to_string())
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn close_prepared(
    client_resource: ResourceArc<ClientResource>,
    statement_resource: ResourceArc<PreparedStatementResource>,
) -> Result<rustler::Atom, String> {
    let mut client_guard = client_resource.0.lock().unwrap();
    let mut statement_guard = statement_resource.0.lock().unwrap();

    if let (Some(client), Some(statement)) = (client_guard.take(), statement_guard.take()) {
        match RT.block_on(client.close_prepared(statement)) {
            Ok(_) => Ok(ok()),
            Err(e) => Err(e.to_string()),
        }
    } else {
        Err("client_or_statement_stopped".to_string())
    }
}

#[rustler::nif]
fn stop(resource: ResourceArc<ClientResource>) -> rustler::Atom {
    let mut client_guard = resource.0.lock().unwrap();
    if let Some(client) = client_guard.take() {
        RT.block_on(client.stop());
    }

    ok()
}
