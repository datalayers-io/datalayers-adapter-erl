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
use rustler::{Encoder, Env, MapIterator, Term};
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
fn connect<'a>(env: Env<'a>, term: Term<'a>) -> Term<'a> {
    let iter = match MapIterator::new(term) {
        Some(iter) => iter,
        None => return (error(), "invalid_map").encode(env),
    };

    let mut host = "127.0.0.1".to_string();
    let mut port = 8360;
    let mut username = "admin".to_string();
    let mut password = "public".to_string();

    for (key, value) in iter {
        let key_str = match key.atom_to_string() {
            Ok(key) => key,
            Err(_) => return (error(), "invalid_key").encode(env),
        };
        match key_str.as_str() {
            "host" => match value.decode::<String>() {
                Ok(h) => host = h,
                Err(_) => return (error(), "invalid_host").encode(env),
            },
            "port" => match value.decode::<u32>() {
                Ok(p) => port = p,
                Err(_) => return (error(), "invalid_port").encode(env),
            },
            "username" => match value.decode::<String>() {
                Ok(u) => username = u,
                Err(_) => return (error(), "invalid_username").encode(env),
            },
            "password" => match value.decode::<String>() {
                Ok(p) => password = p,
                Err(_) => return (error(), "invalid_password").encode(env),
            },
            _ => (),
        }
    }

    let tls_cert = std::env::var("TLS_CERT").ok();
    let config = ClientConfig {
        host,
        port,
        username,
        password,
        tls_cert,
    };

    let client = match RT.block_on(Client::try_new(&config)) {
        Ok(client) => client,
        Err(e) => return (error(), e.to_string()).encode(env),
    };

    (ok(), ClientResource::new(client)).encode(env)
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
