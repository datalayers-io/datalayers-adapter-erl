-module(datalayers_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(host, get_host_addr()).

-compile([export_all, nowarn_export_all]).

suite() -> [].

all() -> [connect_test, stop_test, prepare_test].

groups() -> [].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

connect_test(_Config) ->
    {ok, Client} = datalayers:connect(#{host => ?host}),
    {ok, _} = datalayers:execute(Client, <<"CREATE DATABASE common_test">>),
    {ok, [[Version]]} = datalayers:execute(Client, <<"SELECT version()">>),
    ?assert(is_binary(Version)),
    {ok, _} = datalayers:execute(Client, <<"DROP DATABASE common_test">>).

stop_test(_Config) ->
    {ok, Client} = datalayers:connect(#{host => ?host}),
    ok = datalayers:stop(Client),
    %% stop is cast
    ct:sleep(100),
    try datalayers:execute(
        Client, <<"CREATE DATABASE common_test">>
    ) catch
          exit : {noproc, _} : _ -> ok;
          _ -> ?assert(false, "Expected no process error")
    end.

%% erlfmt-ignore
-define(create_database, <<"
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

prepare_test(_Config) ->
    {ok, Client} = datalayers:connect(#{host => ?host}),
    {ok, _} = datalayers:execute(Client, <<"CREATE DATABASE common_test">>),
    {ok, _} = datalayers:execute(Client, ?create_database),
    {ok, PreparedStatement} = datalayers:prepare(
        Client,
        <<"INSERT INTO common_test.demo (ts, sid, value, flag) VALUES (?, ?, ?, ?);">>
    ),
    {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, [
        [erlang:system_time(millisecond), 1, 42.0, 1],
        [erlang:system_time(millisecond), 2, 43.0, 0]
    ]),
    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    {ok, [[<<"2.3.3">>]]} = datalayers:execute(Client, <<"SELECT version()">>),
    {ok, _} = datalayers:execute(Client, <<"DROP TABLE common_test.demo">>),
    {ok, _} = datalayers:execute(Client, <<"DROP DATABASE common_test">>),
    ok = datalayers:stop(Client).

get_host_addr() ->
    case os:getenv("DATALAYERS_TCP_ADDR") of
        false -> <<"localhost">>;
        Host -> iolist_to_binary(Host)
    end.
