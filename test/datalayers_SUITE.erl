-module(datalayers_SUITE).
-include_lib("eunit/include/eunit.hrl").

-define(host, <<"172.18.0.2">>).

connect_test_() ->
    {
        "Connect and execute",
        fun() ->
            {ok, Client} = datalayers:connect(#{host => ?host}),
            {ok, _} = datalayers:execute(Client, <<"CREATE DATABASE eunit_test">>),
            {ok, [[<<"2.3.3">>]]} = datalayers:execute(Client, <<"SELECT version()">>),
            {ok, _} = datalayers:execute(Client, <<"DROP DATABASE eunit_test">>)
        end
    }.

stop_test_() ->
    {
        "Stop client",
        fun() ->
            {ok, Client} = datalayers:connect(#{host => ?host}),
            ok = datalayers:stop(Client),
            {error, <<"client_stopped">>} = datalayers:execute(
                Client, <<"CREATE DATABASE eunit_test">>
            )
        end
    }.

%% erlfmt-ignore
-define(create_database, <<"
    CREATE TABLE eunit_test.demo (
        ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        sid INT32,
        value REAL,
        flag INT8,
        timestamp key(ts)
    )
    PARTITION BY HASH(sid) PARTITIONS 8
    ENGINE=TimeSeries;
">>).

prepare_test_() ->
    {
        "Prepare and execute",
        fun() ->
            {ok, Client} = datalayers:connect(#{host => ?host}),
            {ok, _} = datalayers:execute(Client, <<"CREATE DATABASE eunit_test">>),
            {ok, _} = datalayers:execute(Client, ?create_database),
            {ok, PreparedStatement} = datalayers:prepare(
                Client,
                <<"INSERT INTO eunit_test.demo (ts, sid, value, flag) VALUES (?, ?, ?, ?);">>
            ),
            {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, [
                [erlang:system_time(millisecond), 1, 42.0, 1],
                [erlang:system_time(millisecond), 2, 43.0, 0]
            ]),
            {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
            {ok, [[<<"2.3.3">>]]} = datalayers:execute(Client, <<"SELECT version()">>),
            {ok, _} = datalayers:execute(Client, <<"DROP TABLE eunit_test.demo">>),
            {ok, _} = datalayers:execute(Client, <<"DROP DATABASE eunit_test">>),
            ok = datalayers:stop(Client)
        end
    }.
