-module(datalayers).

-include("datalayers.hrl").

-export([
    connect/1,

    use_database/2,

    execute/2,
    prepare/2,
    execute_prepare/3,

    close_prepared/2,
    stop/1,

    async_execute/3,
    async_prepare/3,
    async_execute_prepare/4
]).

-export_type([
    opts/0,
    client/0,
    sql/0,
    result/0,
    reason/0,
    args/0,
    prepared_statement/0,
    params/0
]).

-define(call(Client, Args), call(Client, ?FUNCTION_NAME, Args)).
-define(async(Client, Args, ResultCallback),
    async(Client, with_out_async(?FUNCTION_NAME), Args, ResultCallback)
).

%% =================================================================================================
%% APIs

-spec connect(Opts :: opts()) -> {ok, client()} | {error, reason()}.
connect(Opts) ->
    Res = {ok, Pid} = datalayers_sock:start(),
    case ?call(Pid, [Opts]) of
        {ok, Ref} when is_reference(Ref) ->
            Res;
        {error, _} = Err ->
            _ = stop(Pid),
            Err
    end.

-spec use_database(client(), binary()) -> ok.
use_database(Client, DbName) ->
    case datalayers:execute(Client, <<"SHOW DATABASES">>) of
        {ok, DBs} ->
            case lists:any(fun([DB, _Data]) -> DB =:= DbName end, DBs) of
                true ->
                    ?call(Client, [DbName]);
                false ->
                    {error,
                        iolist_to_binary(io_lib:format("Database `~s` does not exist", [DbName]))}
            end;
        {error, _Reason} ->
            {error, <<"Failed to fetch databases">>}
    end.

-spec execute(client(), sql()) -> {ok, result()} | {error, reason()}.
execute(Client, Sql) ->
    ?call(Client, [Sql]).

-spec prepare(client(), sql()) -> {ok, prepared_statement()} | {error, reason()}.
prepare(Client, Sql) ->
    ?call(Client, [Sql]).

-spec execute_prepare(client(), prepared_statement(), params()) ->
    {ok, result()} | {error, reason()}.
execute_prepare(Client, Statement, Params) ->
    ?call(Client, [Statement, Params]).

-spec close_prepared(client(), prepared_statement()) -> {ok, prepare_closed} | {error, reason()}.
close_prepared(Client, Statement) ->
    ?call(Client, [Statement]).

-spec stop(client()) -> ok | error.
stop(Client) ->
    datalayers_sock:stop(Client).

%% ================================================================================
%% Asynchronous APIs

-spec async_execute(client(), sql(), callback()) -> ok.
async_execute(Client, Sql, ResultCallback) ->
    ?async(Client, [Sql], ResultCallback).

-spec async_prepare(client(), sql(), callback()) -> ok.
async_prepare(Client, Sql, ResultCallback) ->
    ?async(Client, [Sql], ResultCallback).

-spec async_execute_prepare(client(), prepared_statement(), params(), callback()) -> ok.
async_execute_prepare(Client, Statement, Params, ResultCallback) ->
    ?async(Client, [Statement, Params], ResultCallback).

%% ================================================================================
%% Helpers

call(Client, _Fun = Cmd, Args) ->
    case datalayers_sock:sync_command(Client, Cmd, Args) of
        {ok, _Result} = Ok -> Ok;
        {error, _Reason} = Err -> Err
    end.

-spec async(client(), command(), list(), callback()) -> ok.
async(Client, _Fun = Cmd, Args, ResultCallback) ->
    _ = erlang:send(Client, ?ASYNC_REQ(Cmd, Args, ResultCallback)),
    ok.

with_out_async(Func) ->
    Raw = atom_to_binary(Func, utf8),
    <<"async_", Rest/binary>> = Raw,
    _Cmd = binary_to_existing_atom(Rest, utf8).
