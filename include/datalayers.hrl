-type opts() :: #{
    host := binary(),
    port := integer(),
    username := binary(),
    password := binary(),
    tls_cert := binary()
}.
-type client() :: pid().
-type client_ref() :: reference().
-type sql() :: binary().
-type result() :: list().
-type reason() :: binary().

-type command() :: connect | execute | prepare | execute_prepare | close_prepared.
-type args() :: term().
-type callback() :: {function(), list()}.

-type prepared_statement() :: reference().
-type params() :: list().

-define(NIF_MODULE, datalayers_nif).

-define(REQ(Func, Args), {sync, Func, Args}).
-define(ASYNC_REQ(Func, Args, ResultCallback), {async, Func, Args, ResultCallback}).
