-module(libdatalayers).

-export([load/0, 
         connect/1, execute/2, stop/1]).
-on_load(load/0).

load() ->
    erlang:load_nif(filename:join(priv(), "libdatalayers"), none).

-spec connect(_Opts :: map()) -> {ok, _Client :: reference()} | {error, Reason :: binary()}.
connect(_Opts) ->
    %% maps:with([host, port, username, password, tls_cert], Opts)
    not_loaded(?LINE).

execute(_Client, _Sql) ->
    not_loaded(?LINE).

stop(_Client) ->
    not_loaded(?LINE).

not_loaded(Line) ->
    erlang:nif_error({error, {not_loaded, [{module, ?MODULE}, {line, Line}]}}).

priv() ->
    case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end.
