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
-type args() :: any().

-type prepared_statement() :: reference().
-type params() :: list().
