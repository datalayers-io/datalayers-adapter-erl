# Datalayers Adapter for Erlang

[![Erlang/OTP 27](https://img.shields.io/badge/Erlang/OTP-27-red.svg?logo=erlang&logoColor=white)](https://www.erlang.org)
[![Rust 1.98.1](https://img.shields.io/badge/Rust-1.98.1-orange.svg?logo=rust&logoColor=white)](https://www.rust-lang.org)

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
    tls_enable => true,
    tls_cacertfile => <<"/path/to/ca-bundle.pem">>,
    verify => verify_peer,
    tls_server_name => <<"datalayers.example.com">>
},
{ok, Client} = datalayers:connect(Opts).
```

TLS never falls back to a built-in Mozilla, webpki, or operating-system trust
store. The caller must provide an absolute path to the PEM CA bundle in
`tls_cacertfile`. The bundle may contain multiple CA certificates.

- `tls_enable` explicitly selects TLS. A CA path without this option still
  enables TLS for compatibility with older callers.
- `verify` must be `verify_peer`. `verify_none` is rejected because the Arrow
  Flight client always verifies the server certificate chain and hostname.
- `tls_server_name` overrides the name used for SNI and hostname verification.
  It defaults to `host`.

> [!WARNING]
> `tls_cert` is no longer accepted as a CA alias. Rename existing configurations
> to use `tls_cacertfile`. The `tls_cert` name is reserved for a possible future
> client certificate option and must not be used for a CA bundle.

Mutual TLS is not supported by the Datalayers server. Client certificate
options (`certfile` and `keyfile`) must not be supplied to this driver.

**Optional settings:**

-   `timeout` — per-stage flight timeout in milliseconds. Every network stage (connect, handshake, execute, fetch) is bounded by it. Defaults to `15000`; `0` means "not set".
-   `request_timeout` — whole-request deadline in milliseconds, including queueing, runtime scheduling, and parameter conversion. Accepted values are `1..86400000`; defaults and invalid or out-of-range values use `15000`. `0` means "not set".

Both are per connection. The cap on how many requests may run at the same time is
per node instead (see [In-flight limit](#in-flight-limit)).

### In-flight limit

One native library serves every connection in the node, so the number of requests
that may run at the same time is capped per node rather than per connection:

```erlang
%% sys.config
{datalayers, [{max_in_flight, 1024}]}
```

The value is read once, when the library is loaded, and can not be changed at
runtime. It defaults to `1024`; a value outside `1..1000000`, or one that is not
an integer, is replaced by that default.

### Executing Queries

Use `datalayers:execute/2` to run SQL queries.

```erlang
{ok, Client} = datalayers:connect(Opts),
{ok, Result} = datalayers:execute(Client, <<"SELECT version()">>).
%% Result will be [[<<"2.3.8">>]]
```

SQL binaries must not exceed **8 MiB**. This is a hard limit because the statement
is copied across the NIF boundary. Keep SQL substantially smaller in normal use;
for large batches, use prepared statements and `execute_prepare/3` instead of
rendering a very large batch statement. Oversized synchronous calls return
`{error, {invalid_sql, {too_large, 8388608}}}` and asynchronous calls deliver the
same error to their callback.

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

The library also provides asynchronous versions of `execute`, `prepare`, and `execute_prepare`. They return immediately and deliver the result to the callback instead.

The callback is invoked as `CallbackFun(CallbackArgs ++ [Result])`, where `Result` is `{ok, Value}` or `{error, Reason}` (including `{error, {not_dispatched, Reason}}`, see [Errors](#errors)).

```erlang
Callback = {fun(Result) -> io:format("Result: ~p~n", [Result]) end, []},
ok = datalayers:async_execute(Client, <<"SELECT * FROM my_table">>, Callback).
```

### Stopping the Client

To close the connection and clean up resources, use `datalayers:stop/1`.

```erlang
ok = datalayers:stop(Client).
```

### Errors

A failed call returns `{error, Reason}`. `Reason` is usually a text description.
`{not_dispatched, Reason}` means the request never reached the server (nothing
was sent), so retrying it is safe if the caller chooses to. Invalid SQL inputs
return `{invalid_sql, {too_large, MaxBytes}}` or `{invalid_sql, expected_binary}`.

## Running Tests

To run the test suite, use `rebar3 eunit`. The tests require a running Datalayers instance. You can configure the connection details using environment variables:

-   `DATALAYERS_TCP_ADDR`: The address for TCP tests (defaults to `localhost`).
-   `DATALAYERS_TLS_ADDR`: The address for TLS tests (defaults to `localhost`).

```bash
$ rebar3 eunit
```
