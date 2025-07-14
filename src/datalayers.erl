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
    datalayers_nif:connect(Opts).

-spec execute(client(), sql()) -> {ok, result()} | {error, reason()}.
execute(Client, Sql) ->
    datalayers_nif:execute(Client, Sql).

-spec prepare(client(), sql()) -> {ok, prepared_statement()} | {error, reason()}.
prepare(Client, Sql) ->
    datalayers_nif:prepare(Client, Sql).

-spec execute_prepare(client(), prepared_statement(), params()) ->
    {ok, result()} | {error, reason()}.
execute_prepare(Client, Statement, Params) ->
    datalayers_nif:execute_prepare(Client, Statement, Params).

-spec close_prepared(client(), prepared_statement()) -> {ok, prepare_closed} | {error, reason()}.
close_prepared(Client, Statement) ->
    datalayers_nif:close_prepared(Client, Statement).

-spec stop(client()) -> ok | error.
stop(Client) ->
    datalayers_nif:stop(Client).
