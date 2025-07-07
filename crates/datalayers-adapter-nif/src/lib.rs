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
use rustler::{Encoder, Env, Term, ResourceArc};
use tokio::runtime::Runtime;

rustler::init!("libdatalayers", load = on_load);

lazy_static! {
    static ref RT: Runtime = Runtime::new().unwrap();
}

pub fn on_load(env: Env, _load_info: Term) -> bool {
    let _ = env.register::<ClientResource>();
    true
}

#[rustler::nif]
fn connect(env: Env, config: ClientConfig) -> Result<ResourceArc<ClientResource>, String> {
    let client = match RT.block_on(Client::try_new(&config)) {
        Ok(client) => client,
        Err(e) => return Err(e.to_string()),
    };

    Ok(ClientResource::new(client))
}

#[rustler::nif]
fn execute<'a>(env: Env<'a>, resource: Term<'a>, sql: String) -> Term<'a> {
    let client_resource: rustler::ResourceArc<ClientResource> = match resource.decode() {
        Ok(r) => r,
        Err(_) => return (error(), "invalid_client_resource").encode(env),
    };

    let mut client_guard = client_resource.0.lock().unwrap();

    if let Some(client) = &mut *client_guard {
        match RT.block_on(client.execute(&sql)) {
            Ok(result) => (ok(), util::to_term(&result[..], env)).encode(env),
            Err(e) => (error(), e.to_string()).encode(env),
        }
    } else {
        (error(), client_stopped()).encode(env)
    }
}

#[rustler::nif]
fn stop(resource: Term) -> rustler::Atom {
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
