-type opts() :: #{
    host := binary(),
    port := integer(),
    username := binary(),
    password := binary(),
    %% Explicit TLS switch. When omitted, a CA path still enables TLS for
    %% backward compatibility.
    tls_enable => boolean(),
    %% PEM CA bundle used to verify the Datalayers server certificate.
    tls_cacertfile => binary(),
    %% Peer certificate and hostname verification are mandatory.
    verify => verify_peer,
    %% Name used for SNI and certificate hostname verification. Defaults to host.
    tls_server_name => binary(),
    %% Per-stage flight timeout, in milliseconds.
    timeout => non_neg_integer(),
    %% Whole-request deadline, in milliseconds.
    request_timeout => non_neg_integer()
}.
-type client() :: pid().
-type client_ref() :: reference().
-type sql() :: binary().
-type result() :: list().
%% A failure is either a plain text reason, or `{not_dispatched, Reason}` when
%% nothing was sent to the server; whether that is worth retrying is the caller's
%% decision.
-type reason() ::
    binary()
    | {not_dispatched, binary()}
    | {invalid_sql, {too_large, non_neg_integer()}}
    | {invalid_sql, expected_binary}.

-type command() :: connect | use_database | execute | prepare | execute_prepare | close_prepared.
-type args() :: term().
-type callback() :: {function(), list()}.

-type prepared_statement() :: reference().
-type params() :: list().
-type prepare_opts() :: #{auto_rebuild => boolean()}.

-define(NIF_MODULE, datalayers_nif).

%% Whole-request deadline, in milliseconds, used when the connect options do not
%% set `request_timeout`. Keep in sync with `DEFAULT_REQUEST_TIMEOUT_MS` in
%% `client_opts.rs`.
-define(DEFAULT_REQUEST_TIMEOUT_MS, 15000).
%% Keep the absolute deadline, backstop timer, and NIF integer conversions in a
%% documented range. Larger values fall back to the default.
-define(MAX_REQUEST_TIMEOUT_MS, 86400000).
%% SQL is copied across the NIF boundary. Reject oversized statements before
%% submitting them, while passing normal batch SQL to a dirty CPU scheduler.
-define(MAX_SQL_BYTES, 8388608).

-define(REQ(Func, Args), {sync, Func, Args}).
-define(ASYNC_REQ(Func, Args, ResultCallback), {async, Func, Args, ResultCallback}).
