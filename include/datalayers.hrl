-type opts() :: #{
    host := binary(),
    port := integer(),
    username := binary(),
    password := binary(),
    tls_cert := binary()
}.
-type client() :: reference().
-type sql() :: binary().
-type result() :: list().
-type reason() :: binary().

-type prepared_statement() :: reference().
-type params() :: list().
