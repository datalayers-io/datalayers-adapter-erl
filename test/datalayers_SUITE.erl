-module(datalayers_SUITE).
-include_lib("eunit/include/eunit.hrl").

-define(host, <<"172.19.0.30">>).

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

prepare_test_() ->
    {
        "Prepare and execute",
        fun() ->
            {ok, Client} = datalayers:connect(#{host => ?host}),
            {ok, PreparedStatement} = datalayers:prepare(Client, <<"INSERT INTO rust.demo (ts, sid, value, flag) VALUES (?, ?, ?, ?);">>),
            {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, [
                [erlang:system_time(millisecond), 1, 42.0, 1],
                [erlang:system_time(millisecond), 2, 43.0, 0]
            ])
        end
    }.
