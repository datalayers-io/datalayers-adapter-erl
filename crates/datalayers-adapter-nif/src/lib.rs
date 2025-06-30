extern crate core;
extern crate rustler;

mod atoms;
mod client;
mod util;

use atoms::ok;
use client::{Client, ClientConfig};
use util::*;

use rustler::{Env, Term};
use anyhow::Result;
use tokio::runtime::Runtime;
use lazy_static::lazy_static;

rustler::init!("libdatalayers", load = on_load);

lazy_static! {
    static ref RT: Runtime = Runtime::new().unwrap();
}

#[rustler::nif]
fn hello() -> rustler::Atom {
    println!("Hello, world!");
    let _res = RT.block_on(do_hello());
    ok()
}

pub fn on_load(_env: Env, _load_info: Term) -> bool {
    true
}

async fn do_hello() -> Result<()> {
    // Sets the TLS_CERT env var to the path of the certificate file if you want to use TLS.
    let tls_cert = std::env::var("TLS_CERT").ok();

    // Creates a client configured for Datalayers.
    let config = ClientConfig {
        host: "172.19.0.30".to_string(),
        port: 8360,
        username: "admin".to_string(),
        password: "public".to_string(),
        tls_cert,
    };
    let mut client = Client::try_new(&config).await?;

    // Creates a database `rust`.
    let mut sql = "CREATE DATABASE rust";
    let mut result = client.execute(sql).await?;
    // The result should be:
    // Affected rows: 0
    print_affected_rows(&result);

    // Optional: sets the database header for each outgoing request to `rust`.
    // The Datalayers server uses this header to identify the associated table of a request.
    //
    // This setting is optional since the following SQLs contain the database context
    // and the server could parse the database context from SQLs.
    client.use_database("rust");

    // Creates a table `demo` within the database `rust`.
    sql = r#"
        CREATE TABLE rust.demo (
            ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            sid INT32,
            value REAL,
            flag INT8,
            timestamp key(ts)
        )
        PARTITION BY HASH(sid) PARTITIONS 8
        ENGINE=TimeSeries;
    "#;
    result = client.execute(sql).await?;
    // The result should be:
    // Affected rows: 0
    print_affected_rows(&result);

    // Inserts some data.
    sql = r#"
        INSERT INTO rust.demo (ts, sid, value, flag) VALUES
            ('2024-09-01T10:00:00+08:00', 1, 12.5, 0),
            ('2024-09-01T10:05:00+08:00', 2, 15.3, 1),
            ('2024-09-01T10:10:00+08:00', 3, 9.8, 0),
            ('2024-09-01T10:15:00+08:00', 4, 22.1, 1),
            ('2024-09-01T10:20:00+08:00', 5, 30.0, 0);
    "#;
    result = client.execute(sql).await?;
    // The result should be:
    // Affected rows: 5
    print_affected_rows(&result);

    // Queries the inserted data
    sql = "SELECT * FROM rust.demo";
    result = client.execute(sql).await?;
    // The result should be:
    // +---------------------------+-----+-------+------+
    // | ts                        | sid | value | flag |
    // +---------------------------+-----+-------+------+
    // | 2024-09-01T10:15:00+08:00 | 4   | 22.1  | 1    |
    // | 2024-09-01T10:10:00+08:00 | 3   | 9.8   | 0    |
    // | 2024-09-01T10:05:00+08:00 | 2   | 15.3  | 1    |
    // | 2024-09-01T10:20:00+08:00 | 5   | 30.0  | 0    |
    // | 2024-09-01T10:00:00+08:00 | 1   | 12.5  | 0    |
    // +---------------------------+-----+-------+------+
    print_batches(&result);

    // Inserts some data with prepared statement.
    sql = "INSERT INTO rust.demo (ts, sid, value, flag) VALUES (?, ?, ?, ?);";
    let mut prepared_stmt = client.prepare(sql).await?;
    let mut binding = make_insert_binding();
    result = client.execute_prepared(&mut prepared_stmt, binding).await?;
    // The result should be:
    // Affected rows: 5
    print_affected_rows(&result);

    // Queries the inserted data with prepared statement.
    sql = "SELECT * FROM rust.demo WHERE sid = ?";
    prepared_stmt = client.prepare(sql).await?;

    // Retrieves all rows with `sid` = 1.
    binding = make_query_binding(1);
    result = client.execute_prepared(&mut prepared_stmt, binding).await?;
    // The result should be:
    // +---------------------------+-----+-------+------+
    // | ts                        | sid | value | flag |
    // +---------------------------+-----+-------+------+
    // | 2024-09-01T10:00:00+08:00 | 1   | 12.5  | 0    |
    // | 2024-09-02T10:00:00+08:00 | 1   | 12.5  | 0    |
    // +---------------------------+-----+-------+------+
    print_batches(&result);

    // Retrieves all rows with `sid` = 2.
    binding = make_query_binding(2);
    result = client.execute_prepared(&mut prepared_stmt, binding).await?;
    // The result should be:
    // +---------------------------+-----+-------+------+
    // | ts                        | sid | value | flag |
    // +---------------------------+-----+-------+------+
    // | 2024-09-01T10:05:00+08:00 | 2   | 15.3  | 1    |
    // | 2024-09-02T10:05:00+08:00 | 2   | 15.3  | 1    |
    // +---------------------------+-----+-------+------+
    print_batches(&result);

    // Closes the prepared statement to notify releasing resources on server side.
    client.close_prepared(prepared_stmt).await?;

    // There provides a dedicated interface `execute_update` for executing DMLs, including Insert, Delete.
    // This interface directly returns the affected rows which might be convenient for some use cases.
    //
    // Note, Datalayers does not support Update and the development for Delete is in progress.
    sql = r#"
        INSERT INTO rust.demo (ts, sid, value, flag) VALUES
            ('2024-09-03T10:00:00+08:00', 1, 4.5, 0),
            ('2024-09-03T10:05:00+08:00', 2, 11.6, 1);
    "#;
    let affected_rows = client.execute_update(sql).await?;
    // The output should be:
    // Affected rows: 2
    println!("Affected rows: {}", affected_rows);

    // Checks that the data are inserted successfully.
    sql = "SELECT * FROM rust.demo where ts >= '2024-09-03T10:00:00+08:00'";
    result = client.execute(sql).await?;
    // The result should be:
    // +---------------------------+-----+-------+------+
    // | ts                        | sid | value | flag |
    // +---------------------------+-----+-------+------+
    // | 2024-09-03T10:00:00+08:00 | 1   | 4.5   | 0    |
    // | 2024-09-03T10:05:00+08:00 | 2   | 11.6  | 1    |
    // +---------------------------+-----+-------+------+
    print_batches(&result);

    Ok(())
}
