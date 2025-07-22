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

-define(call(Client, Args), call(Client, ?FUNCTION_NAME, Args)).

%% =================================================================================================
%% APIs

-spec connect(Opts :: opts()) -> {ok, client()} | {error, reason()}.
connect(Opts) ->
    Res = {ok, Pid} = datalayers_sock:start(),
    case ?call(Pid, Opts) of
        {ok, Ref} when is_reference(Ref) ->
            Res;
        {error, _} = Err ->
            _ = stop(Pid),
            Err
    end.

-spec execute(client(), sql()) -> {ok, result()} | {error, reason()}.
execute(Client, Sql) ->
    ?call(Client, Sql).

-spec prepare(client(), sql()) -> {ok, prepared_statement()} | {error, reason()}.
prepare(Client, Sql) ->
    ?call(Client, Sql).

-spec execute_prepare(client(), prepared_statement(), params()) ->
    {ok, result()} | {error, reason()}.
execute_prepare(Client, Statement, Params) ->
    ?call(Client, #{
        prepare_statement => Statement,
        parameters => Params
    }).

-spec close_prepared(client(), prepared_statement()) -> {ok, prepare_closed} | {error, reason()}.
close_prepared(Client, Statement) ->
    ?call(Client, #{prepare_statement => Statement}).

-spec stop(client()) -> ok | error.
stop(Client) ->
    datalayers_sock:stop(Client).

%% ================================================================================
%% Helpers

-spec call(client(), command(), args()) -> {ok, any()} | {error, any()}.
call(Client, _Fun = Cmd, Args) ->
    case datalayers_sock:sync_command(Client, Cmd, Args) of
        {ok, _Result} = Ok -> Ok;
        {error, _Reason} = Err -> Err
    end.
