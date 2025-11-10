# Datalayers Adapter for Erlang

[![Erlang/OTP](https://img.shields.io/badge/Erlang/OTP-27-red.svg)](https://www.erlang.org)
[![Rust](https://img.shields.io/badge/Rust-1.88-orange.svg)](https://www.rust-lang.org)

`datalayers-adapter-erl` is an Erlang library for connecting to and interacting with [Datalayers](https://datalayers.cn/) databases. It uses a Rust NIF (Native Implemented Function) for high-performance communication with the Datalayers server via the Arrow Flight SQL protocol.

## Features

- Connect to Datalayers with or without TLS.
- Execute SQL queries.
- Use prepared statements for optimized query execution.
- Asynchronous APIs for non-blocking operations.
- Automatic type conversion between Erlang terms and Datalayers types.

## Building

To build the library, you need to have `rebar3` and the Rust toolchain installed.

```bash
$ rebar3 compile
```

## Usage

### Add as a Dependency

Add the following to your `rebar.config` file:

```erlang
{deps, [
    {datalayers, {git, "https://github.com/datalayers-io/datalayers-adapter-erl.git", {tag, "0.1.0"}}}
]}.
```

### Connecting to Datalayers

To connect to a Datalayers instance, use the `datalayers:connect/1` function with a map of options.

**Without TLS:**

```erlang
Opts = #{
    host => <<"localhost">>,
    port => 8360,
    username => <<"admin">>,
    password => <<"public">>
},
{ok, Client} = datalayers:connect(Opts).
```

**With TLS:**

```erlang
Opts = #{
    host => <<"localhost">>,
    port => 8360,
    username => <<"admin">>,
    password => <<"public">>,
    tls_cert => <<"/path/to/ca.crt">>
},
{ok, Client} = datalayers:connect(Opts).
```

### Executing Queries

Use `datalayers:execute/2` to run SQL queries.

```erlang
{ok, Client} = datalayers:connect(Opts),
{ok, Result} = datalayers:execute(Client, <<"SELECT version()">>).
%% Result will be [[<<"2.3.8">>]]
```

### Using a Specific Database

After connecting, you can switch to a specific database using `datalayers:use_database/2`.

```erlang
ok = datalayers:use_database(Client, <<"my_database">>).
```

### Prepared Statements

For queries that are executed multiple times, prepared statements can improve performance.

```erlang
{ok, Client} = datalayers:connect(Opts),
ok = datalayers:use_database(Client, <<"my_database">>),

Sql = <<"INSERT INTO my_table (ts, sid, value, flag) VALUES (?, ?, ?, ?)">>,
{ok, PreparedStatement} = datalayers:prepare(Client, Sql),

Timestamp = erlang:system_time(millisecond),
Params = [
    [Timestamp, 1, 42.0, 1],
    [Timestamp, 2, 43.0, 0]
],
{ok, _} = datalayers:execute_prepare(Client, PreparedStatement, Params),

ok = datalayers:close_prepared(Client, PreparedStatement).
```

### Asynchronous Operations

The library also provides asynchronous versions of `execute`, `prepare`, and `execute_prepare`. These functions take a callback function that will be executed with the result.

```erlang
Callback = {fun(Result) -> io:format("Result: ~p~n", [Result]) end, []},
ok = datalayers:async_execute(Client, <<"SELECT * FROM my_table">>, Callback).
```

### Stopping the Client

To close the connection and clean up resources, use `datalayers:stop/1`.

```erlang
ok = datalayers:stop(Client).
```

## Running Tests

To run the test suite, use `rebar3 eunit`. The tests require a running Datalayers instance. You can configure the connection details using environment variables:

-   `DATALAYERS_TCP_ADDR`: The address for TCP tests (defaults to `localhost`).
-   `DATALAYERS_TLS_ADDR`: The address for TLS tests (defaults to `localhost`).

```bash
$ rebar3 eunit
```
