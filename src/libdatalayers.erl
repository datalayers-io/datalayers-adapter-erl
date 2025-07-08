-module(libdatalayers).

-export([
    connect/1,
    execute/2,
    stop/1
]).

-export([load/0]).
-on_load(load/0).

-type opts() :: #{
    host => binary(),
    port => integer(),
    username => binary(),
    password => binary(),
    tls_cert := binary() | undefined
}.
-type client() :: reference().
-type sql() :: binary().
-type result() :: list().
-type reason() :: binary().

load() ->
    erlang:load_nif(filename:join(priv(), "libdatalayers"), none).


%% ========================================
%% APIs

-spec connect(Opts :: opts()) -> {ok, client()} | {error, reason()}.
connect(Opts) ->
    Host = maps:get(host, Opts, <<"localhost">>),
    Port = maps:get(port, Opts, 8360),
    Username = maps:get(username, Opts, <<"admin">>),
    Password = maps:get(password, Opts, <<"public">>),
    TlsCert = maps:get(tls_cert, Opts, undefined),
    connect_nif(#{host => Host, port => Port, username => Username, password => Password, tls_cert => TlsCert}).


-spec execute(client(), sql()) -> {ok, result()} | {error, reason()}.
execute(Client, Sql) ->
    execute_nif(Client, Sql).

-spec stop(client()) -> ok | error.
stop(Client) ->
    stop_nif(Client).

%% ========================================
%% NIFs

connect_nif(_Opts) ->
    not_loaded(?LINE).

execute_nif(_Client, _Sql) ->
    not_loaded(?LINE).

stop_nif(_Client) ->
    not_loaded(?LINE).


%% ========================================
%% Helpers

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
