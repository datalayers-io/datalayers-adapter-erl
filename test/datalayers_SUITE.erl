-module(datalayers_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("datalayers.hrl").

-compile([export_all, nowarn_export_all]).

-define(conn_opts(Config), ?config(conn_opts, Config)).
-define(version(Config), ?config(version, Config)).
-define(database(Config), ?config(database, Config)).
-define(table(Config), ?config(table, Config)).
-define(container(Config), ?config(container, Config)).
-define(DOCKER_TIMEOUT_MS, 60000).

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
        t_oversized_sql_is_rejected,
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
        t_sync_after_async_ordering,
        t_async_nif_replies_to_given_pid,
        t_runs_without_the_new_options,
        t_connect_timeout_is_bounded,
        t_request_deadline_is_honored,
        t_nif_honors_the_handed_down_budget,
        t_nif_rejects_expired_work_before_dispatch,
        t_max_in_flight_cannot_be_changed_at_runtime,
        t_expired_queue_budget_is_not_confused_with_a_budget,
        t_backstop_starts_after_submission,
        t_backstop_timeout_text_matches_the_native_form,
        t_unusable_timeout_options_fall_back_to_the_default,
        t_out_of_range_request_timeout_falls_back,
        t_queued_requests_expire_without_crashing_the_socket,
        t_auto_rebuild_prepare_timeout_is_dispatched,
        t_socket_never_runs_a_blocking_nif,
        t_stop_releases_a_pending_sync_caller,
        t_second_connect_while_connecting_is_rejected,
        t_stopping_during_connect_cancels_the_attempt,
        t_killing_while_connecting_cancels_the_attempt,
        t_connect_tls_read_timeout_is_not_dispatched,
        t_docker_command_failures_are_reported
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
    [{conn_opts, ConnOpts}, {container, "datalayers_tcp"} | Config];
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
    [{conn_opts, ConnOpts}, {container, "datalayers_tls"} | Config].

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
    ClientRef = connect_client_ref(Config),
    InvalidRef = make_ref(),

    %% Stale resource references are rejected synchronously, before any work is
    %% submitted: the async NIFs answer with {error, Reason} instead of `ok`.
    ?assertMatch(
        {error, <<"invalid_client_resource">>},
        datalayers_nif:async_execute(
            self(), 1, nif_request(1000), InvalidRef, <<"SELECT 1">>
        )
    ),
    ?assertMatch(
        {error, <<"invalid_client_resource">>},
        datalayers_nif:async_use_database(
            self(), 2, nif_request(1000), InvalidRef, <<"some_db">>
        )
    ),
    ?assertMatch(
        {error, <<"invalid_client_resource">>},
        datalayers_nif:async_prepare(
            self(), 3, nif_request(1000), InvalidRef, <<"SELECT 1">>, false
        )
    ),
    ?assertMatch(
        {error, <<"invalid_client_resource">>},
        datalayers_nif:async_execute_prepare(
            self(), 4, nif_request(1000), InvalidRef, InvalidRef, []
        )
    ),
    ?assertMatch(
        {error, <<"invalid_statement_resource">>},
        datalayers_nif:async_execute_prepare(
            self(), 5, nif_request(1000), ClientRef, InvalidRef, []
        )
    ),
    ?assertMatch(
        {error, <<"invalid_client_resource">>},
        datalayers_nif:async_close_prepared(
            self(), 6, nif_request(1000), InvalidRef, InvalidRef
        )
    ),
    ?assertMatch(
        {error, <<"invalid_statement_resource">>},
        datalayers_nif:async_close_prepared(
            self(), 7, nif_request(1000), ClientRef, InvalidRef
        )
    ),

    ok = datalayers_nif:stop(ClientRef).

%% SQL is copied at the NIF boundary. Reject oversized statements at the socket
%% before they can be copied, queued, or parsed by the server.
t_oversized_sql_is_rejected(_Config) ->
    {ok, Sock} = datalayers_sock:start(),
    Sql = binary:copy(<<"x">>, ?MAX_SQL_BYTES + 1),
    Expected = {error, {invalid_sql, {too_large, ?MAX_SQL_BYTES}}},
    try
        ?assertEqual(Expected, datalayers:execute(Sock, Sql)),
        ?assertEqual(Expected, datalayers:prepare(Sock, Sql)),
        ok = datalayers:async_execute(Sock, Sql, async_reply_fun(self())),
        ?assertEqual(Expected, receive_async_result(1000)),
        ok = datalayers:async_prepare(Sock, Sql, async_reply_fun(self())),
        ?assertEqual(Expected, receive_async_result(1000))
    after
        ok = datalayers:stop(Sock)
    end.

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
    ok = datalayers:async_execute(Client, <<"SELECT version()">>, async_reply_fun(Parent)),
    {ok, [[Vsn]]} = receive_async_result(5000),
    ?assertEqual(?version(Config), Vsn),
    ok = datalayers:stop(Client).

t_async_prepare(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    Parent = self(),
    ok = datalayers:async_prepare(
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
    ok = datalayers:async_execute_prepare(
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
            ok = datalayers:async_execute(
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
    ok = datalayers:async_execute(
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
    ok = datalayers:async_execute_prepare(
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
            ok = datalayers:async_execute(
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

%% The async NIFs deliver their result to the pid they were handed, as
%% {datalayers_async_result, Id, Result}, independently of the socket process.
t_async_nif_replies_to_given_pid(Config) ->
    ClientRef = connect_client_ref(Config),
    Id = erlang:unique_integer([monotonic, positive]),
    ok = datalayers_nif:async_execute(
        self(), Id, nif_request(1000), ClientRef, <<"SELECT version()">>
    ),
    {ok, [[Vsn]]} = receive_async_result_by_id(Id, 5000),
    ?assertEqual(?version(Config), Vsn),
    ok = datalayers_nif:stop(ClientRef).

%% The upper layer (EMQX) does not have to pass `request_timeout`: the NIF falls
%% back to its own default, and that default must let a normal multi-stage request
%% through. The in-flight cap is not a connection option at all: it comes from the
%% application environment.
t_runs_without_the_new_options(Config) ->
    ConnOpts = ?conn_opts(Config),
    ?assertEqual(false, maps:is_key(request_timeout, ConnOpts)),
    ?assertEqual(false, maps:is_key(max_in_flight, ConnOpts)),
    {ok, Client} = datalayers:connect(ConnOpts),
    {ok, [[Vsn]]} = do_execute(Client, <<"SELECT version()">>),
    ?assertEqual(?version(Config), Vsn),
    {ok, PreparedStatement} = datalayers:prepare(Client, ?insert_prepare_sql_statement(Config)),
    Timestamp = erlang:system_time(millisecond),
    {ok, _} = datalayers:execute_prepare(Client, PreparedStatement, [[Timestamp, 1, 42.0, 1]]),
    ?assertMatch(
        {ok, [[_, <<"1">>, <<"42.0">>, <<"1">>]]},
        do_execute(Client, ?select_all_from_table_by_ts(Config, Timestamp))
    ),
    {ok, _} = datalayers:close_prepared(Client, PreparedStatement),
    ok = datalayers:stop(Client).

%% A peer that accepts TCP but never answers must fail the connect within the
%% per-stage timeout instead of hanging the caller forever.
t_connect_timeout_is_bounded(_Config) ->
    {Server, Port} = silent_server(),
    Opts = silent_conn_opts(Port, 500),
    {Micros, Result} = timer:tc(fun() -> datalayers:connect(Opts) end),
    exit(Server, kill),
    {error, Reason} = Result,
    ?assertNotEqual(nomatch, binary:match(Reason, <<"timed out after">>)),
    ?assert(Micros < 5_000_000).

%% The whole-request deadline must fire before the per-stage timeout when it is
%% the smaller of the two, so a request can not span several stages' worth of time.
t_request_deadline_is_honored(_Config) ->
    {Server, Port} = silent_server(),
    Opts = silent_conn_opts(Port, 5000, 200),
    {Micros, Result} = timer:tc(fun() -> datalayers:connect(Opts) end),
    exit(Server, kill),
    {error, Reason} = Result,
    ?assertNotEqual(nomatch, binary:match(Reason, <<"[request] timed out after">>)),
    ?assert(Micros < 2_000_000).

%% The budget handed down per request must bound it even when both the connection
%% default and the per-stage timeout are far larger. This is what makes the
%% remaining budget the socket computes for a queued request meaningful.
t_nif_honors_the_handed_down_budget(_Config) ->
    {Server, Port} = silent_server(),
    Opts = (silent_conn_opts(Port, 60000))#{request_timeout => 60000},
    Id = erlang:unique_integer([monotonic, positive]),
    Started = erlang:monotonic_time(millisecond),
    Token = datalayers_nif:new_cancel_token(),
    Request = {Started + 300, 300},
    ok = datalayers_nif:async_connect(self(), Id, Request, Token, Opts),
    {error, Reason} = receive_async_result_by_id(Id, 6000),
    Elapsed = erlang:monotonic_time(millisecond) - Started,
    exit(Server, kill),
    ?assertNotEqual(nomatch, binary:match(error_text(Reason), <<"[request] timed out after">>)),
    ?assert(Elapsed < 3000).

%% The absolute deadline is checked before an operation is polled. Expired work
%% must never reach the Flight endpoint.
t_nif_rejects_expired_work_before_dispatch(Config) ->
    ClientRef = connect_client_ref(Config),
    Id = erlang:unique_integer([monotonic, positive]),
    Expired = {erlang:monotonic_time(millisecond) - 1, 1000},
    ok = datalayers_nif:async_execute(self(), Id, Expired, ClientRef, <<"SELECT 1">>),
    {error, {not_dispatched, Reason}} = receive_async_result_by_id(Id, 2000),
    ?assertNotEqual(nomatch, binary:match(Reason, <<"[request] timed out after">>)),
    ok = datalayers_nif:stop(ClientRef).

%% The in-flight cap is a load-time setting: it is configured once when the
%% library is loaded, and a later attempt to change it must be refused rather
%% than silently take effect.
t_max_in_flight_cannot_be_changed_at_runtime(_Config) ->
    ?assertEqual(error, datalayers_nif:set_max_in_flight(4)),
    ?assertEqual(error, datalayers_nif:set_max_in_flight(undefined)),
    ok.

%% A queued request whose deadline has passed is answered as expired, and that
%% classification must not be confusable with a positive budget: an atom compares
%% greater than any number in Erlang term order, so `expired > 0` holds and the
%% socket used to take the dispatch branch and crash with a badarith.
t_expired_queue_budget_is_not_confused_with_a_budget(_Config) ->
    ?assertMatch(
        {ok, N} when N > 0,
        datalayers_sock:remaining_budget(erlang:monotonic_time(millisecond) + 1000)
    ),
    ?assertEqual(
        expired, datalayers_sock:remaining_budget(erlang:monotonic_time(millisecond) - 1)
    ),
    ok.

%% Submission can stay inside a NIF longer than the deadline and margin. The
%% backstop must not already be queued when that submission finally returns.
t_backstop_starts_after_submission(_Config) ->
    Id = erlang:unique_integer([positive]),
    Deadline = erlang:monotonic_time(millisecond) + 1,
    {ok, Timer} =
        datalayers_sock:submit_with_backstop(
            fun() ->
                timer:sleep(30),
                ok
            end,
            Id,
            Deadline,
            5
        ),
    receive
        {request_expired, Id} ->
            ct:fail("backstop fired before NIF submission returned")
    after 0 ->
        ok
    end,
    _ = erlang:cancel_timer(Timer),
    ok.

%% The backstop answers the same condition the native side reports, so it has to
%% use the same text: callers (and the EMQX regression test) match on it.
t_backstop_timeout_text_matches_the_native_form(_Config) ->
    ?assertEqual("15s", datalayers_sock:budget_text(15000)),
    ?assertEqual("1.5s", datalayers_sock:budget_text(1500)),
    ?assertEqual("1.05s", datalayers_sock:budget_text(1050)),
    ?assertEqual("1.001s", datalayers_sock:budget_text(1001)),
    ?assertEqual("250ms", datalayers_sock:budget_text(250)),
    ?assertEqual(
        <<"[request] timed out after 15s">>,
        iolist_to_binary(["[request] timed out after ", datalayers_sock:budget_text(15000)])
    ),
    ok.

%% A typo in a timeout option must not kill the socket process (the caller of the
%% NIF) and take its linked worker with it: the value falls back to the default,
%% the way the Erlang side already treats it.
t_unusable_timeout_options_fall_back_to_the_default(Config) ->
    Opts = (?conn_opts(Config))#{timeout => <<"15s">>, request_timeout => -1},
    Id = erlang:unique_integer([monotonic, positive]),
    Token = datalayers_nif:new_cancel_token(),
    ok = datalayers_nif:async_connect(self(), Id, nif_request(15000), Token, Opts),
    {ok, ClientRef} = receive_async_result_by_id(Id, 10000),
    ?assert(is_reference(ClientRef)),
    ok = datalayers_nif:stop(ClientRef).

%% A positive integer can still be outside every downstream range. It must fall
%% back to the default rather than crashing the linked socket process.
t_out_of_range_request_timeout_falls_back(_Config) ->
    {Server, Port} = silent_server(),
    Opts = (silent_conn_opts(Port, 500))#{request_timeout => 9223372036854775807},
    OldTrapExit = process_flag(trap_exit, true),
    try
        Result =
            try datalayers:connect(Opts) of
                Connected -> Connected
            catch
                exit:Reason -> {socket_crashed, Reason}
            end,
        {error, ErrorReason} = Result,
        ?assertNotEqual(
            nomatch, binary:match(error_text(ErrorReason), <<"timed out after">>)
        )
    after
        process_flag(trap_exit, OldTrapExit),
        exit(Server, kill)
    end.

%% Freezing the server (the container is paused, not closed, so the connection
%% stays up) makes the first request hang and the following ones queue up past
%% their budget. Expiring in the queue must not crash the socket: every accepted
%% request must still be answered exactly once, and the connection must keep
%% working once the server is back.
t_queued_requests_expire_without_crashing_the_socket(Config) ->
    Container = ?container(Config),
    Opts = (?conn_opts(Config))#{request_timeout => 1000},
    %% Connect while the server is healthy: freezing afterwards (pausing the
    %% container keeps the connection open) makes the next request hang and the
    %% following ones queue up past their budget.
    {ok, Client} = datalayers:connect(Opts),
    Result =
        case docker(["pause", Container]) of
            ok -> with_thaw_guard(Container, fun() -> frozen_request_backlog(Client) end);
            {error, PauseError} -> {error, PauseError}
        end,
    %% Recovery has to mean more than "the inflight entries were dropped": the
    %% same socket must work again once the server is back.
    Recovered =
        case Result of
            ok ->
                {ok, [[<<"1">>]]} = datalayers:execute(Client, <<"SELECT 1">>),
                ok;
            _ ->
                skipped
        end,
    ok = datalayers:stop(Client),
    case Result of
        {error, Reason} ->
            {skip, lists:flatten(io_lib:format("cannot pause the container: ~s", [Reason]))};
        ok ->
            ?assertEqual(ok, Recovered),
            ok
    end.

%% Rebuilding a closed local statement sends a Prepare RPC. If that RPC or the
%% following binding conversion exceeds the deadline, the request must not be
%% reported as `not_dispatched`.
t_auto_rebuild_prepare_timeout_is_dispatched(Config) ->
    Container = ?container(Config),
    Opts = (?conn_opts(Config))#{request_timeout => 1000},
    {ok, Client} = datalayers:connect(Opts),
    {ok, PreparedStatement} =
        datalayers:prepare(Client, ?insert_prepare_sql_statement(Config), #{
            auto_rebuild => true
        }),
    {ok, prepare_closed} = datalayers:close_prepared(Client, PreparedStatement),
    Result =
        case docker(["pause", Container]) of
            ok ->
                with_thaw_guard(Container, fun() ->
                    auto_rebuild_prepare_timeout(Client, PreparedStatement)
                end);
            {error, PauseError} ->
                {error, PauseError}
        end,
    ok = datalayers:stop(Client),
    case Result of
        ok ->
            ok;
        {error, Reason} ->
            {skip, lists:flatten(io_lib:format("cannot pause the container: ~s", [Reason]))}
    end.

auto_rebuild_prepare_timeout(Client, PreparedStatement) ->
    Timestamp = erlang:system_time(millisecond),
    ok = datalayers:async_execute_prepare(
        Client,
        PreparedStatement,
        [[Timestamp, 1, 42.0, 1]],
        async_reply_fun(self())
    ),
    {error, Reason} = receive_async_result(5000),
    ?assertNotMatch({not_dispatched, _}, Reason),
    ?assertNotEqual(nomatch, binary:match(error_text(Reason), <<"timed out after">>)),
    ok.

frozen_request_backlog(Client) ->
    Parent = self(),
    Count = 8,
    Sqls = [iolist_to_binary(io_lib:format("SELECT ~p", [N])) || N <- lists:seq(1, Count)],
    lists:foreach(
        fun(Sql) ->
            ok = datalayers:async_execute(Client, Sql, async_reply_fun(Parent))
        end,
        Sqls
    ),
    Results = [receive_async_result(20000) || _ <- Sqls],
    ?assertEqual(Count, length(Results)),
    ?assert(lists:all(fun(R) -> element(1, R) =:= error end, Results)),
    %% The queued ones never reached the server, so the socket must have
    %% classified at least one of them as such -- and, crucially, survived.
    Queued = [R || R = {error, {not_dispatched, _}} <- Results],
    ?assert(length(Queued) >= 1),
    ?assert(is_process_alive(Client)),
    ok.

%% Regression guard for emqx/emqx#19342: while a hung connect is in flight, the
%% socket process must never be inside a datalayers_nif function. Blocking in a
%% NIF would pin a BEAM scheduler thread (a DirtyIo one in the old code) and
%% starve unrelated file/log I/O across the whole VM.
t_socket_never_runs_a_blocking_nif(_Config) ->
    {Server, Port} = silent_server(),
    Opts = silent_conn_opts(Port, 2000),
    {ok, Sock} = datalayers_sock:start(),
    Self = self(),
    _Caller = spawn(fun() ->
        Self ! {connect_result, datalayers_sock:sync_command(Sock, connect, [Opts])}
    end),
    ok = assert_never_inside_nif(Sock, 30, 50),
    {error, Reason} = receive_tagged(connect_result, 6000),
    exit(Server, kill),
    ?assertNotEqual(nomatch, binary:match(Reason, <<"timed out after">>)),
    _ = catch datalayers_sock:stop(Sock),
    ok.

%% Stopping a socket must release a caller that is still waiting for a reply
%% instead of leaving it blocked on gen_server:call/3 forever.
t_stop_releases_a_pending_sync_caller(_Config) ->
    {Server, Port} = silent_server(),
    Opts = silent_conn_opts(Port, 5000),
    {ok, Sock} = datalayers_sock:start(),
    Self = self(),
    _Caller = spawn(fun() ->
        Self ! {connect_result, datalayers_sock:sync_command(Sock, connect, [Opts])}
    end),
    ct:sleep(100),
    ok = datalayers_sock:stop(Sock),
    %% The connect was already dispatched, so it is reported as a plain failure
    %% rather than a retriable one.
    ?assertEqual({error, <<"stopped">>}, receive_tagged(connect_result, 2000)),
    exit(Server, kill),
    ok.

%% A second connect must be rejected while the first is still queued or in flight;
%% otherwise the socket would end up with two clients and silently drop the first
%% connection's channel.
t_second_connect_while_connecting_is_rejected(_Config) ->
    {Server, Port} = silent_server(),
    Opts = silent_conn_opts(Port, 3000),
    {ok, Sock} = datalayers_sock:start(),
    Self = self(),
    _Caller = spawn(fun() ->
        Self ! {connect_result, datalayers_sock:sync_command(Sock, connect, [Opts])}
    end),
    ct:sleep(100),
    ?assertEqual({error, already_connected}, datalayers_sock:sync_command(Sock, connect, [Opts])),
    {error, _} = receive_tagged(connect_result, 6000),
    exit(Server, kill),
    _ = catch datalayers_sock:stop(Sock),
    ok.

%% Stopping a socket while a connect is still in progress must cancel the native
%% attempt instead of letting it run until its budget: the peer sees the hang-up.
t_stopping_during_connect_cancels_the_attempt(_Config) ->
    {Server, Port} = silent_server_reporting_close(self()),
    Opts = silent_conn_opts(Port, 60000),
    {ok, Sock} = datalayers_sock:start(),
    Self = self(),
    _Caller = spawn(fun() ->
        Self ! {connect_result, datalayers_sock:sync_command(Sock, connect, [Opts])}
    end),
    %% Let the TCP connection come up (the peer stays silent, so the handshake
    %% hangs), then stop: the attempt must give up long before the 60s budget.
    ct:sleep(200),
    ok = datalayers_sock:stop(Sock),
    _ = receive_tagged(connect_result, 2000),
    ?assertEqual({error, closed}, receive_tagged(peer_result, 5000)),
    exit(Server, kill),
    ok.

%% Killing the socket bypasses `handle_cast(stop, ...)`. The owner process still
%% has to cancel a native connect that is waiting for the Flight handshake.
t_killing_while_connecting_cancels_the_attempt(_Config) ->
    {Server, Port} = silent_server_reporting_close(self()),
    Opts = silent_conn_opts(Port, 60000),
    Parent = self(),
    Owner =
        spawn(fun() ->
            {ok, Sock} = datalayers_sock:start(),
            Parent ! {socket, self(), Sock},
            receive
                stop -> ok
            end
        end),
    Sock =
        receive
            {socket, Owner, Socket} -> Socket
        after 5000 ->
            ct:fail("socket did not start")
        end,
    _Caller = spawn(fun() ->
        _ = catch datalayers_sock:sync_command(Sock, connect, [Opts]),
        Parent ! connect_finished
    end),
    timer:sleep(200),
    exit(Sock, kill),
    PeerResult =
        receive
            {peer_result, Result} -> Result
        after 2000 ->
            erlang:error(peer_connection_was_not_cancelled)
        end,
    ?assertEqual({error, closed}, PeerResult),
    receive
        connect_finished -> ok
    after 2000 ->
        ct:fail("connect caller was not released")
    end,
    exit(Server, kill),
    ok.

%% Client::try_new reads the certificate before it starts network I/O. A timeout
%% in that stage must still be classified as not dispatched.
t_connect_tls_read_timeout_is_not_dispatched(_Config) ->
    case os:find_executable("mkfifo") of
        false ->
            {skip, "mkfifo is unavailable"};
        Mkfifo ->
            Path =
                filename:join(
                    "/tmp",
                    "datalayers-fifo-" ++ integer_to_list(erlang:unique_integer([positive]))
                ),
            _ = file:delete(Path),
            try
                {0, _} = run_executable(Mkfifo, [Path]),
                Writer = start_fifo_writer(Path),
                Id = erlang:unique_integer([monotonic, positive]),
                Token = datalayers_nif:new_cancel_token(),
                Opts = #{
                    host => <<"127.0.0.1">>,
                    port => 1,
                    username => <<"admin">>,
                    password => <<"public">>,
                    tls_cert => list_to_binary(Path),
                    timeout => 60000,
                    request_timeout => 200
                },
                ok = datalayers_nif:async_connect(
                    self(), Id, nif_request(200), Token, Opts
                ),
                Result = receive_async_result_by_id(Id, 2000),
                {error, {not_dispatched, Reason}} = Result,
                ?assertNotEqual(nomatch, binary:match(Reason, <<"timed out after">>)),
                wait_for_port(Writer, 3000)
            after
                _ = file:delete(Path)
            end
    end.

%% Container-control failures must take the skip path instead of pretending the
%% command succeeded and then failing unrelated timeout assertions.
t_docker_command_failures_are_reported(_Config) ->
    OldPath = os:getenv("PATH"),
    true = os:putenv("PATH", "/definitely/no/docker/here"),
    try
        ?assertMatch({error, _}, docker(["pause", "no_such_container"]))
    after
        true = os:putenv("PATH", OldPath)
    end.

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

    %% Best-effort cleanup of a previous run. `DROP TABLE IF EXISTS db.table`
    %% fails when `db` does not exist (datalayers >= 2.4.9 follows Postgres,
    %% where IF EXISTS covers the table but not the schema), which is the normal
    %% state on a fresh server.
    ok = cleanup(Client, ?drop_table(Config)),
    ok = cleanup(Client, ?drop_database(Config)),

    {ok, _} = do_execute(Client, ?create_database(Config)),
    {ok, _} = do_execute(Client, ?create_table(Config)),

    ok = datalayers:stop(Client),
    Config.

drop_database_and_table(Config) ->
    {ok, Client} = datalayers:connect(?conn_opts(Config)),
    ok = cleanup(Client, ?drop_table(Config)),
    ok = cleanup(Client, ?drop_database(Config)),
    ok = datalayers:stop(Client).

%% Run a cleanup statement, tolerating "it is not there" failures: dropping a
%% table in a database that does not exist (yet) is not an error for the test.
%% Cleanup may only tolerate "the object is not there": `DROP ... IF EXISTS`
%% still reports that when the database does not exist (datalayers >= 2.4.9).
%% Anything else -- a bad credential, a broken server, a malformed statement --
%% must fail the test rather than be swallowed here.
cleanup(Client, SQL) ->
    case datalayers:execute(Client, SQL) of
        {ok, _} ->
            ok;
        {error, Reason} = Error ->
            case binary:match(error_text(Reason), <<"does not exist">>) of
                nomatch -> ct:fail("cleanup failed: ~p: ~p", [SQL, Error]);
                _ -> ok
            end
    end.

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

nif_request(TimeoutMs) ->
    {erlang:monotonic_time(millisecond) + TimeoutMs, TimeoutMs}.

%% Connect straight through the NIF surface and return the client resource ref,
%% so tests can drive the async NIF entry points without a socket process.
connect_client_ref(Config) ->
    Id = erlang:unique_integer([monotonic, positive]),
    Token = datalayers_nif:new_cancel_token(),
    ok = datalayers_nif:async_connect(
        self(), Id, nif_request(15000), Token, ?conn_opts(Config)
    ),
    receive
        {datalayers_async_result, Id, {ok, Ref}} when is_reference(Ref) ->
            Ref;
        {datalayers_async_result, Id, {error, Reason}} ->
            ct:fail("async_connect failed: ~p", [Reason])
    after 10000 ->
        ct:fail("async_connect did not reply")
    end.

receive_async_result_by_id(Id, Timeout) ->
    receive
        {datalayers_async_result, Id, Result} -> Result
    after Timeout ->
        ct:fail("timed out waiting for async result ~p", [Id])
    end.

receive_tagged(Tag, Timeout) ->
    receive
        {Tag, Value} -> Value
    after Timeout ->
        ct:fail("timed out waiting for ~p", [Tag])
    end.

%% A failure may be reported as a plain text reason or as `not_dispatched` (the
%% request never reached the server); both carry the same human-readable text.
error_text(Text) when is_binary(Text) -> Text;
error_text({not_dispatched, Text}) -> Text.

%% Run Fun while the container is paused, and make sure it is resumed even if the
%% caller is killed: a socket crash kills its linked caller, which would otherwise
%% leave the rest of the suite running against a frozen server.
with_thaw_guard(Container, Fun) ->
    Parent = self(),
    Keeper =
        spawn(fun() ->
            MRef = monitor(process, Parent),
            receive
                released ->
                    ok;
                {'DOWN', MRef, process, Parent, _} ->
                    _ = docker(["unpause", Container])
            end
        end),
    try
        Fun()
    after
        _ = docker(["unpause", Container]),
        Keeper ! released
    end.

docker(Args) ->
    case os:find_executable("docker") of
        false ->
            {error, <<"docker executable not found">>};
        Executable ->
            docker(Executable, Args)
    end.

docker(Executable, Args) ->
    case run_executable(Executable, Args) of
        {0, _} ->
            ok;
        {Status, Output} ->
            {error,
                iolist_to_binary(
                    io_lib:format("docker ~ts exited with status ~p: ~ts", [
                        lists:join(" ", Args), Status, Output
                    ])
                )}
    end.

run_executable(Executable, Args) ->
    Port = open_port(
        {spawn_executable, Executable},
        [exit_status, stderr_to_stdout, use_stdio, binary, {args, Args}]
    ),
    collect_executable(Port, []).

collect_executable(Port, Output) ->
    receive
        {Port, {data, Data}} ->
            collect_executable(Port, [Data | Output]);
        {Port, {exit_status, Status}} ->
            {Status, iolist_to_binary(lists:reverse(Output))}
    after ?DOCKER_TIMEOUT_MS ->
        _ = catch port_close(Port),
        {-1, iolist_to_binary(lists:reverse(Output))}
    end.

start_fifo_writer(Path) ->
    Shell = os:find_executable("sh"),
    open_port(
        {spawn_executable, Shell},
        [
            exit_status,
            stderr_to_stdout,
            use_stdio,
            binary,
            {args, ["-c", "sleep 1; printf x > " ++ Path]}
        ]
    ).

wait_for_port(Port, Timeout) ->
    receive
        {Port, {exit_status, _Status}} ->
            ok;
        {Port, {_Tag, _Data}} ->
            wait_for_port(Port, Timeout)
    after Timeout ->
        _ = catch port_close(Port),
        erlang:error(helper_process_timed_out)
    end.

%% Options for a plain TCP connection to a local silent server. Deliberately built
%% from scratch so the TLS group does not leak a tls_cert into this test.
silent_conn_opts(Port, Timeout) ->
    silent_conn_opts(Port, Timeout, undefined).

silent_conn_opts(Port, Timeout, undefined) ->
    #{
        host => <<"127.0.0.1">>,
        port => Port,
        username => <<"admin">>,
        password => <<"public">>,
        timeout => Timeout
    };
silent_conn_opts(Port, Timeout, RequestTimeout) ->
    (silent_conn_opts(Port, Timeout, undefined))#{request_timeout => RequestTimeout}.

%% A TCP server that accepts connections and never writes a byte: the half-open
%% peer that makes a blocking client hang.
silent_server() ->
    Parent = self(),
    Server =
        spawn(fun() ->
            {ok, Listen} = gen_tcp:listen(0, [
                binary, {active, false}, {reuseaddr, true}, {ip, {127, 0, 0, 1}}
            ]),
            {ok, Port} = inet:port(Listen),
            Parent ! {silent_server, self(), Port},
            hold_connections(Listen, [])
        end),
    receive
        {silent_server, Server, Port} -> {Server, Port}
    after 5000 ->
        ct:fail("silent server did not start")
    end.

hold_connections(Listen, Held) ->
    case gen_tcp:accept(Listen, 5000) of
        {ok, Socket} ->
            %% Keep the socket open and never write: a silent peer.
            hold_connections(Listen, [Socket | Held]);
        {error, timeout} ->
            hold_connections(Listen, Held);
        {error, _} ->
            ok
    end.

%% Like silent_server/0, but the acceptor also watches whether the client hangs
%% up and reports the read result, so a test can tell "the native attempt gave up"
%% from "it is still running".
silent_server_reporting_close(Parent) ->
    Server =
        spawn(fun() ->
            {ok, Listen} = gen_tcp:listen(0, [
                binary, {active, false}, {reuseaddr, true}, {ip, {127, 0, 0, 1}}
            ]),
            {ok, Port} = inet:port(Listen),
            Parent ! {silent_server, self(), Port},
            Result =
                case gen_tcp:accept(Listen, 5000) of
                    {ok, Socket} -> drain_until_closed(Socket, 20000);
                    {error, Reason} -> {error, Reason}
                end,
            Parent ! {peer_result, Result}
        end),
    receive
        {silent_server, Server, Port} -> {Server, Port}
    after 5000 ->
        ct:fail("silent server did not start")
    end.

%% Read (and discard) whatever the peer sends until it hangs up, so the caller
%% can tell "the attempt gave up" from "it is still running". The client sends the
%% HTTP/2 connection preface first, so a single read would not see the close.
drain_until_closed(Socket, Timeout) ->
    case gen_tcp:recv(Socket, 0, Timeout) of
        {ok, _Ignored} -> drain_until_closed(Socket, Timeout);
        {error, _} = Error -> Error
    end.

assert_never_inside_nif(_Sock, 0, _SleepMs) ->
    ok;
assert_never_inside_nif(Sock, N, SleepMs) ->
    case process_info(Sock, current_function) of
        {current_function, {datalayers_nif, Func, Arity}} ->
            ct:fail("socket process was blocked inside datalayers_nif:~p/~p", [Func, Arity]);
        _ ->
            ok
    end,
    ct:sleep(SleepMs),
    assert_never_inside_nif(Sock, N - 1, SleepMs).
