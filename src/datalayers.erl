-module(datalayers).

-include("datalayers.hrl").

-export([
    connect/1,
    execute/2,

    prepare/2,
    execute_prepare/3,
    close_prepared/2,

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

-spec prepare(client(), sql()) -> {ok, prepared_statement()} | {error, reason()}.
prepare(Client, Sql) ->
    datalayers_nif:prepare(Client, Sql).

-spec execute_prepare(client(), prepared_statement(), params()) -> {ok, result()} | {error, reason()}.
execute_prepare(Client, Statement, Params) ->
    datalayers_nif:execute_prepare(Client, Statement, Params).

-spec close_prepared(client(), prepared_statement()) -> ok | {error, reason()}.
close_prepared(Client, Statement) ->
    datalayers_nif:close_prepared(Client, Statement).

-spec stop(client()) -> ok | error.
stop(Client) ->
    datalayers_nif:stop(Client).
