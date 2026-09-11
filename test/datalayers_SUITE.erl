-module(datalayers_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

-define(conn_opts(Config), ?config(conn_opts, Config)).
-define(version(Config), ?config(version, Config)).
-define(database(Config), ?config(database, Config)).
-define(table(Config), ?config(table, Config)).

-define(create_database(Config), <<"CREATE DATABASE IF NOT EXISTS ", (?database(Config))/binary>>).
-define(drop_database(Config), <<"DROP DATABASE IF EXISTS ", (?database(Config))/binary>>).

%% erlfmt-ignore
-define(create_table(Config), <<"
    CREATE TABLE IF NOT EXISTS ", (?database(Config))/binary, ".", (?table(Config))/binary, " (
        ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        sid INT32,
        value REAL,
        flag INT8,
        timestamp key(ts)
    )
    PARTITION BY HASH(sid) PARTITIONS 8
    ENGINE=TimeSeries;
">>).
-define(drop_table(Config),
    <<"DROP TABLE IF EXISTS ", (?database(Config))/binary, ".", (?table(Config))/binary>>
).

-define(insert_prepare_sql_statement(Config),
    <<"INSERT INTO ", (?database(Config))/binary, ".", (?table(Config))/binary,
        " (ts, sid, value, flag) VALUES (?, ?, ?, ?);">>
).

-define(select_all_from_table_by_ts(Config, Timestamp),
    iolist_to_binary(
        io_lib:format("SELECT * FROM ~s.~s WHERE ts = ~p", [
            ?database(Config), ?table(Config), Timestamp
        ])
    )
).

all() ->
    [
        {group, tcp},
        {group, tls}
    ].

groups() ->
    TCs = [
        t_connect_test,
        t_stop_test,
        t_use_database,
        t_prepare_test,
        t_invalid_ref_test,
        t_prepare_invalid_params_test,
        t_prepare_with_null_test,
        t_empty_batch,
        t_prepare_auto_rebuild_test,
        t_prepare_no_rebuild_test,
        t_async_execute,
        t_async_prepare,
        t_async_execute_prepare,
        t_async_ordering,
        t_async_error_reply,
        t_async_prepare_auto_rebuild,
        t_sync_after_async_ordering
    ],
    [
        {tcp, TCs},
        {tls, TCs}
    ].

init_per_suite(Config) ->
    Host = get_host_addr("DATALAYERS_TCP_ADDR"),
    ConnOpts = #{
        host => Host,
        port => 8360,
        username => <<"admin">>,
        password => <<"public">>
    },
    {ok, Client} = datalayers:connect(ConnOpts),
    {ok, [[Vsn]]} = do_execute(Client, <<"SELECT version()">>),
    ct:pal("Datalayers version: ~s", [Vsn]),
    datalayers:stop(Client),
    [{version, Vsn} | Config].

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
    [{conn_opts, ConnOpts} | Config];
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
    [{conn_opts, ConnOpts} | Config].

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Database = <<(atom_to_binary(TestCase))/binary, "_db">>,
    Table = <<(atom_to_binary(TestCase))/binary, "_table">>,
    NConfig = [{database, Database}, {table, Table} | Config],
    ensure_database_and_table(NConfig),
    NConfig.

end_per_testcase(_TestCase, Config) ->
    drop_database_and_table(Config),
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
        do_execute(
            Client, <<"SELECT version()">>
        )
    catch
        exit:{noproc, _}:_ -> ok;
        _ -> ?assert(false, "Expected no process error")
    end.

t_use_database(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    ?assertMatch(
        {ok, <<"database_changed">>},
        datalayers:use_database(Client, ?database(Config))
    ),

    ?assertMatch(
        {error, <<"Database `non_existed_db` does not exist">>},
        datalayers:use_database(Client, <<"non_existed_db">>)
    ),

    {ok, [[Vsn]]} = do_execute(Client, <<"SELECT version()">>),
    ?assertEqual(?version(Config), Vsn),
    ok = datalayers:stop(Client).

t_prepare_test(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    {ok, PreparedStatement} = datalayers:prepare(
        Client,
        ?insert_prepare_sql_statement(Config)
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
        {error, <<"invalid_client_resource">>},
        datalayers_nif:prepare(InvalidRef, <<"SELECT 1">>, false)
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
        ?insert_prepare_sql_statement(Config)
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
        ?insert_prepare_sql_statement(Config)
    ),

    Timestamp = erlang:system_time(millisecond),
    Params = [[Timestamp, 1, 42.0, null]],
    {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, Params),

    {ok, [[_, _, _, <<>>]]} = do_execute(
        Client,
        ?select_all_from_table_by_ts(Config, Timestamp)
    ),

    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    ok = datalayers:stop(Client).

t_empty_batch(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    Timestamp = erlang:system_time(millisecond),
    %% Empty table
    ?assertEqual(
        {ok, []}, do_execute(Client, ?select_all_from_table_by_ts(Config, Timestamp))
    ),
    datalayers:stop(Client).

t_prepare_auto_rebuild_test(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    {ok, PreparedStatement} = datalayers:prepare(
        Client,
        ?insert_prepare_sql_statement(Config),
        #{auto_rebuild => true}
    ),
    Timestamp = erlang:system_time(millisecond),
    {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, [
        [Timestamp, 1, 42.0, 1]
    ]),
    %% Verify data was actually written
    ?assertMatch(
        {ok, [[_, <<"1">>, <<"42.0">>, <<"1">>]]},
        do_execute(Client, ?select_all_from_table_by_ts(Config, Timestamp))
    ),
    %% Close prepared on server side
    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    %% Should auto-rebuild and succeed
    Timestamp2 = erlang:system_time(millisecond),
    ?assertMatch(
        {ok, _},
        datalayers:execute_prepare(Client, PreparedStatement, [
            [Timestamp2, 2, 43.0, 0]
        ])
    ),
    %% Verify data from auto-rebuilt statement was actually written
    ?assertMatch(
        {ok, [[_, <<"2">>, <<"43.0">>, <<"0">>]]},
        do_execute(Client, ?select_all_from_table_by_ts(Config, Timestamp2))
    ),
    ok = datalayers:stop(Client).

t_prepare_no_rebuild_test(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    {ok, PreparedStatement} = datalayers:prepare(
        Client,
        ?insert_prepare_sql_statement(Config)
    ),
    Timestamp = erlang:system_time(millisecond),
    {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, [
        [Timestamp, 1, 42.0, 1]
    ]),
    %% Close prepared on server side
    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    %% Should return error without auto_rebuild
    Timestamp2 = erlang:system_time(millisecond),
    ?assertMatch(
        {error, _},
        datalayers:execute_prepare(Client, PreparedStatement, [
            [Timestamp2, 2, 43.0, 0]
        ])
    ),
    ok = datalayers:stop(Client).

t_async_execute(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    Parent = self(),
    {ok, _} = datalayers:async_execute(Client, <<"SELECT version()">>, async_reply_fun(Parent)),
    {ok, [[Vsn]]} = receive_async_result(5000),
    ?assertEqual(?version(Config), Vsn),
    ok = datalayers:stop(Client).

t_async_prepare(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    Parent = self(),
    {ok, _} = datalayers:async_prepare(
        Client, ?insert_prepare_sql_statement(Config), async_reply_fun(Parent)
    ),
    %% Exercises the ResourceArc -> Erlang term encoding on the NIF worker thread.
    {ok, PreparedStatement} = receive_async_result(5000),
    Timestamp = erlang:system_time(millisecond),
    {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, [[Timestamp, 1, 42.0, 1]]),
    ?assertMatch(
        {ok, [[_, <<"1">>, <<"42.0">>, <<"1">>]]},
        do_execute(Client, ?select_all_from_table_by_ts(Config, Timestamp))
    ),
    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    ok = datalayers:stop(Client).

t_async_execute_prepare(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    {ok, PreparedStatement} = datalayers:prepare(
        Client, ?insert_prepare_sql_statement(Config)
    ),
    Parent = self(),
    Timestamp = erlang:system_time(millisecond),
    {ok, _} = datalayers:async_execute_prepare(
        Client,
        PreparedStatement,
        [[Timestamp, 1, 42.0, 1]],
        async_reply_fun(Parent)
    ),
    {ok, _} = receive_async_result(5000),
    ?assertMatch(
        {ok, [[_, <<"1">>, <<"42.0">>, <<"1">>]]},
        do_execute(Client, ?select_all_from_table_by_ts(Config, Timestamp))
    ),
    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    ok = datalayers:stop(Client).

t_async_ordering(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    Parent = self(),
    Numbers = lists:seq(1, 20),
    Sqls = [integer_to_binary(N) || N <- Numbers],
    lists:foreach(
        fun(N) ->
            {ok, _} = datalayers:async_execute(
                Client, <<"SELECT ", N/binary>>, async_reply_fun(Parent)
            )
        end,
        Sqls
    ),
    %% single in-flight per connection must preserve request order
    Results = [receive_async_result(5000) || _ <- Numbers],
    Expected = [{ok, [[N]]} || N <- Sqls],
    ?assertEqual(Expected, Results),
    ok = datalayers:stop(Client).

t_async_error_reply(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    Parent = self(),
    {ok, _} = datalayers:async_execute(
        Client,
        <<"SELECT * FROM no_such_db.no_such_table">>,
        async_reply_fun(Parent)
    ),
    %% a failing request must still reply, never leave the caller hanging
    ?assertMatch({error, _}, receive_async_result(5000)),
    ok = datalayers:stop(Client).

t_async_prepare_auto_rebuild(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    Parent = self(),
    {ok, PreparedStatement} = datalayers:prepare(
        Client, ?insert_prepare_sql_statement(Config), #{auto_rebuild => true}
    ),
    Timestamp = erlang:system_time(millisecond),
    {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, [[Timestamp, 1, 42.0, 1]]),
    %% Closing the handle takes the statement out of the resource; the asynchronous
    %% path must rebuild it because auto_rebuild is set.
    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    Timestamp2 = erlang:system_time(millisecond),
    {ok, _} = datalayers:async_execute_prepare(
        Client,
        PreparedStatement,
        [[Timestamp2, 2, 43.0, 0]],
        async_reply_fun(Parent)
    ),
    ?assertMatch({ok, _}, receive_async_result(5000)),
    ?assertMatch(
        {ok, [[_, <<"2">>, <<"43.0">>, <<"0">>]]},
        do_execute(Client, ?select_all_from_table_by_ts(Config, Timestamp2))
    ),
    ok = datalayers:stop(Client).

t_sync_after_async_ordering(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    Parent = self(),
    Numbers = lists:seq(1, 10),
    Sqls = [integer_to_binary(N) || N <- Numbers],
    lists:foreach(
        fun(N) ->
            {ok, _} = datalayers:async_execute(
                Client, <<"SELECT ", N/binary>>, async_reply_fun(Parent)
            )
        end,
        Sqls
    ),
    %% A synchronous request shares the connection queue, so it must not overtake the
    %% asynchronous requests that are already queued.  The socket answers the async
    %% callbacks before it replies to this call, and messages from one sender to one
    %% receiver are ordered, so every async result is already in our mailbox once the
    %% synchronous call returns.
    {ok, [[<<"999">>]]} = datalayers:execute(Client, <<"SELECT 999">>),
    Results = [receive_async_result(0) || _ <- Numbers],
    ?assertEqual([{ok, [[N]]} || N <- Sqls], Results),
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
    {ok, [[Vsn]]} = do_execute(Client, <<"SELECT version()">>),
    ?assertEqual(?version(Config), Vsn),
    ct:pal("Expected Datalayers version: ~s, The Running version ~s", [?version(Config), Vsn]),

    {ok, _} = do_execute(Client, ?drop_table(Config)),
    {ok, _} = do_execute(Client, ?drop_database(Config)),

    {ok, _} = do_execute(Client, ?create_database(Config)),
    {ok, _} = do_execute(Client, ?create_table(Config)),

    ok = datalayers:stop(Client),
    Config.

drop_database_and_table(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    {ok, _} = do_execute(Client, ?drop_table(Config)),
    {ok, _} = do_execute(Client, ?drop_database(Config)),
    ok = datalayers:stop(Client).

do_execute(Client, SQL) ->
    {ok, _} = datalayers:execute(Client, SQL).

%% Callback handed to datalayers:async_*; the socket invokes it as
%% CallbackFun(CallbackArgs ++ [Result]), i.e. async_reply_fun(Parent) yields
%% fun(Parent, Result) -> Parent ! {async_result, Result} end.
async_reply_fun(Parent) ->
    {fun(To, Result) -> To ! {async_result, Result} end, [Parent]}.

receive_async_result(Timeout) ->
    receive
        {async_result, Result} -> Result
    after Timeout ->
        ct:fail("timed out waiting for async result")
    end.
