-module(libdatalayers_test).
-include_lib("eunit/include/eunit.hrl").

connect_test_() ->
    {
        "Connect and execute",
        fun() ->
            Host = <<"172.19.0.30">>,
            Port = 8360,
            Username = <<"admin">>,
            Password = <<"public">>,
            {ok, Client} = libdatalayers:connect(#{
                host => Host,
                port => Port,
                username => Username,
                password => Password
            }),
            {ok, _} = libdatalayers:execute(Client, <<"CREATE DATABASE eunit_test">>),
            {ok, _} = libdatalayers:execute(Client, <<"DROP DATABASE eunit_test">>)
        end
    }.

stop_test_() ->
    {
        "Stop client",
        fun() ->
            Host = <<"172.19.0.30">>,
            Port = 8360,
            Username = <<"admin">>,
            Password = <<"public">>,
            {ok, Client} = libdatalayers:connect(#{
                host => Host,
                port => Port,
                username => Username,
                password => Password
            }),
            ok = libdatalayers:stop(Client),
            {error, client_stopped} = libdatalayers:execute(
                Client, <<"CREATE DATABASE eunit_test">>
            )
        end
    }.

prepare_test_() ->
    {
        "Prepare and execute",
        fun() ->
            Host = <<"172.19.0.30">>,
            Port = 8360,
            Username = <<"admin">>,
            Password = <<"public">>,
            {ok, Client} = libdatalayers:connect(#{
                host => Host,
                port => Port,
                username => Username,
                password => Password
            }),
            {ok, PreparedStatement} = libdatalayers:prepare(Client, <<"INSERT INTO rust.demo (ts, sid, value, flag) VALUES (?, ?, ?, ?);">>),
            ok = libdatalayers:execute_prepared(Client, PreparedStatement, [
                [<<"2023-10-01 12:00:00">>, 1, 42.0, 1],
                [<<"2023-10-01 12:05:00">>, 2, 43.0, 0]
            ]),
        end
    }.
