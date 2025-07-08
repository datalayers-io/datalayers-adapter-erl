extern crate core;
extern crate rustler;

mod atoms;
mod client;
mod resource;
mod util;

use crate::resource::ClientResource;
use atoms::*;
use client::{Client, ClientConfig};
use lazy_static::lazy_static;
use rustler::{Env, Reference, ResourceArc, Term};
use tokio::runtime::Runtime;

rustler::init!("libdatalayers", load = on_load);

lazy_static! {
    static ref RT: Runtime = Runtime::new().unwrap();
}

pub fn on_load(env: Env, _load_info: Term) -> bool {
    let _ = env.register::<ClientResource>();
    true
}

#[rustler::nif(schedule = "DirtyIo")]
fn connect_nif(config: ClientConfig) -> Result<ResourceArc<ClientResource>, String> {
    match RT.block_on(Client::try_new(&config)) {
        Ok(client) => return Ok(ClientResource::new(client)),
        Err(e) => return Err(e.to_string()),
    }
}

#[rustler::nif]
fn execute_nif<'a>(env: Env<'a>, resource: Reference, sql: String) -> Result<Term<'a>, String> {
    let client_resource: rustler::ResourceArc<ClientResource> = match resource.decode() {
        Ok(r) => r,
        Err(_) => return Err("invalid_client_resource".to_string()),
    };

    let mut client_guard = client_resource.0.lock().unwrap();

    if let Some(client) = &mut *client_guard {
        match RT.block_on(client.execute(&sql)) {
            Ok(result) => Ok(util::record_batch_to_term(&result[..], env)),
            Err(e) => Err(e.to_string()),
        }
    } else {
        Err("client_stopped".to_string())
    }
}

#[rustler::nif]
fn stop_nif(resource: Reference) -> rustler::Atom {
    let client_resource: rustler::ResourceArc<ClientResource> = match resource.decode() {
        Ok(r) => r,
        Err(_) => return error(),
    };

    let mut client_guard = client_resource.0.lock().unwrap();
    if let Some(client) = client_guard.take() {
        RT.block_on(client.stop());
    }

    ok()
}
