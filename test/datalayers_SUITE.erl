-module(datalayers_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

-define(datalayers_version, <<"2.3.8">>).

-define(create_database, <<"CREATE DATABASE common_test">>).

-define(drop_database, <<"DROP DATABASE common_test">>).

%% erlfmt-ignore
-define(create_table, <<"
    CREATE TABLE common_test.demo (
        ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        sid INT32,
        value REAL,
        flag INT8,
        timestamp key(ts)
    )
    PARTITION BY HASH(sid) PARTITIONS 8
    ENGINE=TimeSeries;
">>).

-define(drop_table, <<"DROP TABLE IF EXISTS common_test.demo">>).

%% erlfmt-ignore
-define(create_nullable_table, <<"
    CREATE TABLE common_test.demo_nullable (
        ts TIMESTAMP NOT NULL,
        sid INT32,
        value REAL,
        flag INT8,
        note STRING NULL,
        timestamp key(ts)
    )
    PARTITION BY HASH(sid) PARTITIONS 8
    ENGINE=TimeSeries;
">>).

-define(drop_nullable_table, <<"DROP TABLE IF EXISTS common_test.demo_nullable">>).

-define(conn_opts(Config), ?config(conn_opts, Config)).

all() ->
    [
        {group, tcp},
        {group, tls}
    ].

groups() ->
    TCs = [
        t_connect_test,
        t_stop_test,
        t_prepare_test,
        t_invalid_ref_test,
        t_prepare_invalid_params_test,
        t_prepare_with_null_test
    ],
    [
        {tcp, TCs},
        {tls, TCs}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(tcp, Config) ->
    Host = get_host_addr("DATALAYERS_TCP_ADDR"),
    ConnOpts = #{
        host => Host,
        port => 8360,
        username => <<"admin">>,
        password => <<"public">>
    },
    NConfig = [{conn_opts, ConnOpts} | Config],
    ensure_database_and_table(NConfig),
    NConfig;
init_per_group(tls, Config) ->
    Host = get_host_addr("DATALAYERS_TLS_ADDR"),
    Dir = code:lib_dir(datalayers),
    Cacertfile = filename:join([Dir, <<"test/data/certs">>, <<"ca.crt">>]),
    ConnOpts = #{
        host => Host,
        port => 8360,
        username => <<"admin">>,
        password => <<"public">>,
        tls_cert => Cacertfile
    },
    NConfig = [{conn_opts, ConnOpts} | Config],
    ensure_database_and_table(NConfig),
    NConfig.

end_per_group(_GroupName, Config) ->
    drop_database_and_table(Config),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

t_connect_test(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    ?assert(is_pid(Client)),
    _ = datalayers:stop(Client),
    ok.

t_stop_test(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    ok = datalayers:stop(Client),
    %% stop is cast
    ct:sleep(100),
    try
        datalayers:execute(
            Client, ?create_database
        )
    catch
        exit:{noproc, _}:_ -> ok;
        _ -> ?assert(false, "Expected no process error")
    end.

t_prepare_test(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    {ok, PreparedStatement} = datalayers:prepare(
        Client,
        <<"INSERT INTO common_test.demo (ts, sid, value, flag) VALUES (?, ?, ?, ?);">>
    ),
    {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, [
        [erlang:system_time(millisecond), 1, 42.0, 1],
        [erlang:system_time(millisecond), 2, 43.0, 0]
    ]),
    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    ok = datalayers:stop(Client).

t_invalid_ref_test(Config) ->
    {ok, ClientRef} = datalayers_nif:connect(?conn_opts(Config)),
    InvalidRef = make_ref(),

    ?assertMatch(
        {error, <<"invalid_client_resource">>}, datalayers_nif:execute(InvalidRef, <<"SELECT 1">>)
    ),
    ?assertMatch(
        {error, <<"invalid_client_resource">>}, datalayers_nif:prepare(InvalidRef, <<"SELECT 1">>)
    ),
    ?assertMatch(
        {error, <<"invalid_client_resource">>},
        datalayers_nif:execute_prepare(InvalidRef, InvalidRef, [])
    ),
    ?assertMatch(
        {error, <<"invalid_statement_resource">>},
        datalayers_nif:execute_prepare(ClientRef, InvalidRef, [])
    ),
    ?assertMatch(
        {error, <<"invalid_client_resource">>},
        datalayers_nif:close_prepared(InvalidRef, InvalidRef)
    ),
    ?assertMatch(
        {error, <<"invalid_statement_resource">>},
        datalayers_nif:close_prepared(ClientRef, InvalidRef)
    ),

    ok = datalayers_nif:stop(ClientRef).

t_prepare_invalid_params_test(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    {ok, PreparedStatement} = datalayers:prepare(
        Client,
        <<"INSERT INTO common_test.demo (ts, sid, value, flag) VALUES (?, ?, ?, ?);">>
    ),

    %% Case 1: Too few parameters
    ParamsShort = [[erlang:system_time(millisecond), 1, 42.0]],
    ?assertMatch({error, _}, datalayers:execute_prepare(Client, PreparedStatement, ParamsShort)),

    %% Case 2: Too many parameters
    ParamsLong = [[erlang:system_time(millisecond), 1, 42.0, 1, <<"extra">>]],
    ?assertMatch({error, _}, datalayers:execute_prepare(Client, PreparedStatement, ParamsLong)),

    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    ok = datalayers:stop(Client).

t_prepare_with_null_test(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    {ok, PreparedStatement} = datalayers:prepare(
        Client,
        <<"INSERT INTO common_test.demo_nullable (ts, sid, value, flag, note) VALUES (?, ?, ?, ?, ?);">>
    ),

    Timestamp = erlang:system_time(millisecond),
    Params = [[Timestamp, 1, 42.0, null, null]],
    {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, Params),

    {ok, [[_, _, _, <<>>, <<>>]]} = datalayers:execute(
        Client,
        iolist_to_binary(
            io_lib:format("SELECT * FROM common_test.demo_nullable WHERE ts = ~p", [Timestamp])
        )
    ),

    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    ok = datalayers:stop(Client).

%% ================================================================================
%% Helpers
%% ================================================================================

get_host_addr(Env) ->
    case os:getenv(Env) of
        false -> <<"localhost">>;
        Host -> iolist_to_binary(Host)
    end.

ensure_database_and_table(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    {ok, _} = datalayers:execute(Client, ?create_database),
    {ok, _} = datalayers:execute(Client, ?create_table),
    {ok, _} = datalayers:execute(Client, ?create_nullable_table),
    {ok, [[?datalayers_version]]} = datalayers:execute(Client, <<"SELECT version()">>),
    ok = datalayers:stop(Client),
    Config.

drop_database_and_table(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    {ok, _} = datalayers:execute(Client, ?drop_table),
    {ok, _} = datalayers:execute(Client, ?drop_nullable_table),
    {ok, _} = datalayers:execute(Client, ?drop_database),
    ok = datalayers:stop(Client).
