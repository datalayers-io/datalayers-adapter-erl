-module(datalayers).

-include("datalayers.hrl").

-export([
    connect/1,
    execute/2,
    stop/1
]).

%% =================================================================================================
%% APIs

-spec connect(Opts :: opts()) -> {ok, client()} | {error, reason()}.
connect(Opts) ->
    Host = maps:get(host, Opts, <<"localhost">>),
    Port = maps:get(port, Opts, 8360),
    Username = maps:get(username, Opts, <<"admin">>),
    Password = maps:get(password, Opts, <<"public">>),
    TlsCert = maps:get(tls_cert, Opts, undefined),
    NOpts = #{
        host => Host,
        port => Port,
        username => Username,
        password => Password,
        tls_cert => TlsCert
    },
    datalayers_nif:connect(NOpts).

-spec execute(client(), sql()) -> {ok, result()} | {error, reason()}.
execute(Client, Sql) ->
    datalayers_nif:execute(Client, Sql).

-spec stop(client()) -> ok | error.
stop(Client) ->
    datalayers_nif:stop(Client).
