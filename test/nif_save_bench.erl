%% Manual measurement harness, not run by CI.
%%
%% It reports two things about copying the parameter term (`OwnedEnv::save`)
%% before submitting: `submit_us`, how long the calling process is blocked by
%% that call, and `max_gap_us`, the largest interval a heartbeat process saw.
%% The NIF runs on a dirty CPU scheduler, so on a node with a single normal
%% scheduler (`+S 1`) the gap stays at its ~2ms baseline however large the batch
%% is; that is the expected result.
%%
%% Run it against a live Datalayers instance:
%%
%%   export DATALAYERS_TCP_ADDR=172.100.100.10
%%   erl +S 1 -noshell -pa _build/test/lib/datalayers/ebin \
%%       -run nif_save_bench main -s init stop
-module(nif_save_bench).

-export([main/0, main/1]).

main(_) ->
    main().

main() ->
    Host = os:getenv("DATALAYERS_TCP_ADDR", "127.0.0.1"),
    Opts = #{
        host => list_to_binary(Host),
        port => 8360,
        username => <<"admin">>,
        password => <<"public">>
    },
    {ok, Client} = datalayers:connect(Opts),
    ok = ensure_table(Client),
    %% The socket's client resource reference, so the NIF can be called directly
    %% and the copy happens in this process (the real path does it in the socket).
    ClientRef = element(2, sys:get_state(Client)),
    {ok, Stmt} = datalayers:prepare(
        Client, <<"INSERT INTO nif_bench.t (ts, sid, value, flag) VALUES (?, ?, ?, ?)">>
    ),
    io:format("~n schedulers=~p~n", [erlang:system_info(schedulers)]),
    Blob = binary:copy(<<"x">>, 16384),
    Cases =
        [{"none", 1, 0}] ++
            [{"int", 1, N} || N <- [1000, 10000, 50000, 200000]] ++
            [{"16KiB", Blob, N} || N <- [1000, 10000]],
    io:format("~7s~7s~9s~11s~9s~18s~n", [
        "value", "rows", "terms", "submit_us", "us/row", "max_gap_us"
    ]),
    lists:foreach(fun({Label, V, N}) -> measure(ClientRef, Stmt, Label, V, N) end, Cases),
    datalayers:stop(Client),
    ok.

ensure_table(Client) ->
    _ = datalayers:execute(Client, <<"CREATE DATABASE IF NOT EXISTS nif_bench">>),
    _ = datalayers:execute(Client, <<"DROP TABLE IF EXISTS nif_bench.t">>),
    {ok, _} = datalayers:execute(
        Client,
        <<
            "CREATE TABLE IF NOT EXISTS nif_bench.t ("
            "ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, sid INT32, value REAL,"
            "flag INT8, timestamp key(ts)) PARTITION BY HASH(sid) PARTITIONS 8"
            " ENGINE=TimeSeries"
        >>
    ),
    ok.

measure(ClientRef, Stmt, Label, Value, Rows) ->
    %% Built before the measurement: the list construction is not what we want to
    %% attribute to the NIF.
    Params = [[Value, Value, Value, Value] || _ <- lists:seq(1, Rows)],
    Terms = erts_debug:flat_size(Params),
    Id = erlang:unique_integer([monotonic, positive]),
    Heartbeat = start_heartbeat(),
    %% Let the heartbeat take a tick, otherwise it is stopped before it ever runs
    %% and it can not report a gap.
    timer:sleep(20),
    {Micros, ok} = timer:tc(fun() ->
        datalayers_nif:async_execute_prepare(self(), Id, 0, ClientRef, Stmt, Params)
    end),
    Gap = stop_heartbeat(Heartbeat),
    PerRow =
        case Rows of
            0 -> 0.0;
            _ -> Micros / Rows
        end,
    io:format("~7s~7w~9w~11w~9.2f~18w~n", [Label, Rows, Terms, Micros, PerRow, Gap]),
    receive
        {datalayers_async_result, Id, _} -> ok
    after 5000 -> ok
    end.

start_heartbeat() ->
    spawn(fun() -> heartbeat(erlang:monotonic_time(microsecond), 0) end).

heartbeat(Last, Max) ->
    receive
        {stop, From} ->
            From ! {max_gap, Max};
        _ ->
            heartbeat(Last, Max)
    after 1 ->
        Now = erlang:monotonic_time(microsecond),
        heartbeat(Now, max(Max, Now - Last))
    end.

stop_heartbeat(Pid) ->
    Pid ! {stop, self()},
    receive
        {max_gap, Gap} -> Gap
    after 2000 -> -1
    end.
