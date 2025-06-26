extern crate core;
extern crate rustler;

mod atoms;

use atoms::ok;
use rustler::{Env, Term};

rustler::init!("libdatalayers", load = on_load);

#[rustler::nif]

fn hello() -> rustler::Atom {
    println!("Hello, world!");
    ok()
}

pub fn on_load(_env: Env, _load_info: Term) -> bool {
    true
}
