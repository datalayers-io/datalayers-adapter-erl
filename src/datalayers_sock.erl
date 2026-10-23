-module(datalayers_sock).

-include("datalayers.hrl").

-behavior(gen_server).

%% How much later than the request budget the socket's backstop timer fires. It
%% exists to detect a *lost* native completion, not to enforce the deadline (the
%% native side does that, and classifies the failure), so it must not win the race
%% against a native reply that is merely late.
-define(BACKSTOP_MARGIN_MS, 1000).

-record(state, {
    client :: client_ref() | undefined,
    opts :: opts() | undefined,
    %% cancellation signal for this socket: it exists before a connection does, so
    %% a connect that is still in progress can be given up on stop as well
    token :: reference() | undefined,
    %% Whole-request deadline in milliseconds, taken from the connect options.
    request_timeout = ?DEFAULT_REQUEST_TIMEOUT_MS :: pos_integer(),
    %% id of the request currently in flight. There is at most one per connection,
    %% which is what preserves the order of the synchronous and asynchronous
    %% requests that share this socket.
    current = undefined :: integer() | undefined,
    %% backstop timer for the in-flight request, in case the native task never
    %% reports back at all
    timer = undefined :: reference() | undefined,
    %% every outstanding request, queued or in flight: id => {Deliver, Func}.
    %% Deliver is {reply, From} for `sync_command/3` callers and
    %% {callback, Callback} for the `datalayers:async_*` callers.
    pending = #{} :: #{integer() => {deliver(), command()}},
    %% requests waiting to be dispatched, each with the absolute deadline (in
    %% monotonic milliseconds) it has to be answered by
    queue = queue:new() :: queue:queue({integer(), command(), args(), integer()})
}).

-type deliver() :: {reply, gen_server:from()} | {callback, callback()}.

-export([
    start/0,
    stop/1,
    sync_command/3,

    %% Exported for tests: decides whether a queued request is dispatched or
    %% answered as already expired.
    remaining_budget/1,

    %% Exported for tests: the textual form of a request budget.
    budget_text/1,

    %% Exported for tests: verify backstop timer ordering around submission.
    submit_with_backstop/4
]).

%% gen_server callbacks
-export([
    init/1,
    start_link/0,
    handle_call/3,
    handle_info/2,
    handle_cast/2
]).

%% ================================================================================
%% API

start() ->
    start_link().

stop(Client) when is_pid(Client) ->
    catch gen_server:cast(Client, stop),
    ok.

-spec sync_command(client(), command(), args()) -> any().
sync_command(Client, Command, Args) ->
    %% The socket answers every accepted request, and the native side always
    %% reports back (panics included), so the caller needs no deadline of its own.
    gen_server:call(Client, ?REQ(Command, Args), infinity).

%% ================================================================================
%% gen_server callbacks
start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    Token = ?NIF_MODULE:new_cancel_token(),
    %% A killed process never runs `handle_cast(stop, ...)`. Watch from
    %% initialization so this also covers a connect that has not produced a
    %% ClientRef yet.
    SockPid = self(),
    _ = spawn(fun() -> cancel_token_when_down(SockPid, Token) end),
    {ok, #state{client = undefined, opts = undefined, token = Token}}.

%% `current = undefined` means nothing is queued or in flight, so this socket
%% has never been asked to connect before. Without it a second `connect` issued
%% while the first is still in flight would be accepted (the `client` field is
%% only filled in by `deliver/4` once the result arrives), and the second
%% connection would silently replace the first one's channel.
handle_call(
    ?REQ(connect, [Opts]),
    From,
    State = #state{client = undefined, current = undefined}
) ->
    State1 = State#state{opts = Opts, request_timeout = request_timeout(Opts)},
    {noreply, enqueue(connect, [State1#state.token, Opts], {reply, From}, State1)};
handle_call(?REQ(connect, _Args), _From, State) ->
    %% Already connected, or a connect request is still queued or in flight.
    {reply, {error, already_connected}, State};
handle_call(?REQ(Func, Args), From, State) when Func =:= execute; Func =:= prepare ->
    case validate_sql(Func, Args) of
        ok ->
            case State#state.client of
                ClientRef when is_reference(ClientRef) ->
                    {noreply, enqueue(Func, Args, {reply, From}, State)};
                _ ->
                    {reply, {error, not_connected}, State}
            end;
        {error, _} = Error ->
            {reply, Error, State}
    end;
handle_call(?REQ(_Func, _Args), _From, State = #state{client = ClientRef}) when
    not is_reference(ClientRef)
->
    {reply, {error, not_connected}, State};
handle_call(?REQ(Func, Args), From, State) ->
    %% A synchronous request shares the queue with the asynchronous ones, so it must
    %% not overtake the requests already queued or in flight. It is submitted to the
    %% NIF and answered from `handle_info/2` once it completes; the socket process
    %% stays responsive the whole time.
    {noreply, enqueue(Func, Args, {reply, From}, State)}.

%% handle_info(?ASYNC_REQ{...}) -- enqueue and dispatch at most one at a time.
handle_info(?ASYNC_REQ(Func, Args, Callback), State) when
    Func =:= execute; Func =:= prepare
->
    case validate_sql(Func, Args) of
        ok ->
            case State#state.client of
                ClientRef when is_reference(ClientRef) ->
                    {noreply, enqueue(Func, Args, {callback, Callback}, State)};
                _ ->
                    reply_callback(Callback, {error, not_connected}),
                    {noreply, State}
            end;
        {error, _} = Error ->
            reply_callback(Callback, Error),
            {noreply, State}
    end;
handle_info(?ASYNC_REQ(Func, Args, Callback), State = #state{client = ClientRef}) when
    is_reference(ClientRef)
->
    {noreply, enqueue(Func, Args, {callback, Callback}, State)};
handle_info(?ASYNC_REQ(_Func, _Args, Callback), State) ->
    reply_callback(Callback, {error, not_connected}),
    {noreply, State};
%% completion message sent by the NIF task
handle_info(
    {datalayers_async_result, Id, Res},
    State = #state{current = Id, pending = Pending}
) when
    is_map_key(Id, Pending)
->
    {noreply, maybe_dispatch(complete(Id, Res, State))};
handle_info({datalayers_async_result, _Id, _Res}, State) ->
    %% stale completion (e.g. after a timeout or stop); ignore
    {noreply, State};
%% Backstop: the native task never reported back at all. The native side enforces
%% the deadline itself and reports a timeout while waiting for an in-flight slot
%% as `not_dispatched` (nothing was sent); this timer fires only when even that
%% reply is lost, which is why it is given a margin.
handle_info({request_expired, Id}, State = #state{current = Id, pending = Pending}) when
    is_map_key(Id, Pending)
->
    %% Answer the caller anyway, so a lost native completion can not pin this
    %% connection forever. Here we can not tell whether the request ever reached
    %% the server, so it is reported as an ordinary, non-retriable failure.
    Timeout = State#state.request_timeout,
    Result = {error, iolist_to_binary(["[request] timed out after ", budget_text(Timeout)])},
    {noreply, maybe_dispatch(complete(Id, Result, State))};
handle_info({request_expired, _Id}, State) ->
    {noreply, State};
handle_info(_, State) ->
    %% Ignore other messages
    {noreply, State}.

handle_cast(stop, State = #state{client = ClientRef, current = Current, pending = Pending}) ->
    %% Cancel first: a connect that is still in progress has no client resource
    %% yet, and an established connection stops its in-flight operation promptly
    %% instead of running until its deadline.
    case State#state.token of
        undefined -> ok;
        Token -> _ = ?NIF_MODULE:cancel(Token)
    end,
    case is_reference(ClientRef) of
        true ->
            %% Fire and forget: `stop/1` drops the client (closing its channel) on
            %% the NIF task, without blocking this process.
            _ = ?NIF_MODULE:stop(ClientRef);
        false ->
            ok
    end,
    %% Every accepted request is completed exactly once: requests still queued were
    %% never sent, so they are reported as `not_dispatched`, while the one already
    %% dispatched may have reached the server and is reported as an ordinary
    %% failure.
    ok = complete_accepted_requests(Current, Pending),
    cancel_timer(State#state.timer),
    {stop, normal, State#state{
        client = undefined,
        current = undefined,
        timer = undefined,
        pending = #{},
        queue = queue:new()
    }}.

%% ================================================================================
%% Helpers

%% Mirrors `ClientOpts::request_timeout/1` on the Rust side.
request_timeout(Opts) ->
    case maps:get(request_timeout, Opts, undefined) of
        Ms when is_integer(Ms), Ms > 0, Ms =< ?MAX_REQUEST_TIMEOUT_MS -> Ms;
        _ -> ?DEFAULT_REQUEST_TIMEOUT_MS
    end.

validate_sql(execute, [Sql]) ->
    validate_sql_binary(Sql);
validate_sql(prepare, [Sql, _AutoRebuild]) ->
    validate_sql_binary(Sql);
validate_sql(_Func, _Args) ->
    ok.

validate_sql_binary(Sql) when is_binary(Sql), byte_size(Sql) =< ?MAX_SQL_BYTES ->
    ok;
validate_sql_binary(Sql) when is_binary(Sql) ->
    {error, {invalid_sql, {too_large, ?MAX_SQL_BYTES}}};
validate_sql_binary(_Sql) ->
    {error, {invalid_sql, expected_binary}}.

enqueue(Func, Args, Deliver, State = #state{pending = Pending0, queue = Queue0}) ->
    %% The correlation id is an integer rather than a make_ref(): it is passed into
    %% the Rust NIF and re-encoded into the completion message from a different
    %% environment (the task's OwnedEnv). An integer decodes and encodes as i64
    %% directly, while a Reference is bound to its environment and would need an
    %% extra OwnedEnv::save/load copy. `unique_integer/1` is monotonically unique
    %% per node and never reused, so it is just as safe as a reference.
    Id = erlang:unique_integer([monotonic, positive]),
    %% The deadline is taken when the request is accepted, so the time it spends
    %% queued behind other requests counts against it too.
    Deadline = erlang:monotonic_time(millisecond) + State#state.request_timeout,
    Pending = maps:put(Id, {Deliver, Func}, Pending0),
    Queue = queue:in({Id, Func, Args, Deadline}, Queue0),
    maybe_dispatch(State#state{pending = Pending, queue = Queue}).

maybe_dispatch(State = #state{current = undefined, queue = Queue}) ->
    case queue:out(Queue) of
        {{value, {Id, Func, Args, Deadline}}, Queue1} ->
            State1 = State#state{queue = Queue1, current = Id},
            case remaining_budget(Deadline) of
                {ok, _Remaining} ->
                    Request = {Deadline, State1#state.request_timeout},
                    Submit = fun() ->
                        apply_async_nif(
                            Func, Args, State1#state.client, self(), Id, Request
                        )
                    end,
                    case submit_with_backstop(Submit, Id, Deadline, ?BACKSTOP_MARGIN_MS) of
                        {ok, Timer} ->
                            State1#state{timer = Timer};
                        {error, Reason} ->
                            %% Submission rejected (e.g. a stale resource reference);
                            %% finish the request now so the caller is not left waiting.
                            maybe_dispatch(complete(Id, {error, Reason}, State1))
                    end;
                expired ->
                    %% It never reached the server.
                    Message = <<"[request] timed out while queued on the socket">>,
                    Result = {error, {not_dispatched, Message}},
                    maybe_dispatch(complete(Id, Result, State1))
            end;
        {empty, _} ->
            State
    end;
maybe_dispatch(State) ->
    State.

%% The result is tagged on purpose: returning a bare integer or the atom
%% `expired` invites `Remaining > 0` guards, and an atom compares greater than
%% any number in Erlang term order, so the expired case would take the branch
%% meant for a positive budget and then do arithmetic on an atom.
remaining_budget(Deadline) ->
    case Deadline - erlang:monotonic_time(millisecond) of
        Remaining when Remaining > 0 -> {ok, Remaining};
        _ -> expired
    end.

submit_with_backstop(Submit, Id, Deadline, Margin) when is_function(Submit, 0) ->
    case Submit() of
        ok ->
            Delay = max(0, Deadline - erlang:monotonic_time(millisecond)) + Margin,
            Timer = erlang:send_after(Delay, self(), {request_expired, Id}),
            {ok, Timer};
        {error, Reason} ->
            {error, Reason}
    end.

%% The native side reports its budget the way Rust formats a `Duration`
%% (`15s`, `1.5s`, `250ms`), and callers match on that text. This timer answers
%% the same condition, so it has to use the same form. It knows the connection's
%% budget rather than the per-request remainder the native side was handed, so the
%% two agree for the common case of a request that was not queued.
budget_text(Ms) when is_integer(Ms), Ms >= 1000, Ms rem 1000 =:= 0 ->
    integer_to_list(Ms div 1000) ++ "s";
budget_text(Ms) when is_integer(Ms), Ms >= 1000 ->
    Fraction = string:trim(io_lib:format("~3..0w", [Ms rem 1000]), trailing, "0"),
    integer_to_list(Ms div 1000) ++ "." ++ Fraction ++ "s";
budget_text(0) ->
    "0ns";
budget_text(Ms) ->
    integer_to_list(Ms) ++ "ms".

%% Finish one request: run its Deliver function and free the in-flight slot.
complete(Id, Res, State = #state{pending = Pending0}) ->
    {Deliver, Func} = maps:get(Id, Pending0),
    Pending = maps:remove(Id, Pending0),
    cancel_timer(State#state.timer),
    deliver(Func, Deliver, Res, State#state{
        current = undefined, timer = undefined, pending = Pending
    }).

deliver(connect, {reply, From}, {ok, ClientRef} = Res, State) when is_reference(ClientRef) ->
    _ = gen_server:reply(From, Res),
    %% Bind the native operation to this process: if the socket dies without
    %% stopping (crash, kill), `terminate/2` never runs, so a monitor is the only
    %% way to cancel the operation that would otherwise keep running while the
    %% caller reacts to the DOWN and retries.
    SockPid = self(),
    _ = spawn(fun() -> cancel_when_down(SockPid, ClientRef) end),
    State#state{client = ClientRef};
deliver(_Func, {reply, From}, Res, State) ->
    _ = gen_server:reply(From, Res),
    State;
deliver(_Func, {callback, Callback}, Res, State) ->
    reply_callback(Callback, Res),
    State.

complete_accepted_requests(Current, Pending) ->
    maps:foreach(
        fun(Id, {Deliver, _Func}) ->
            Result =
                case Id =:= Current of
                    true ->
                        %% Dispatched: the server may already have accepted it, so it
                        %% must not be retried blindly.
                        {error, <<"stopped">>};
                    false ->
                        %% Still queued: it was never sent.
                        {error, {not_dispatched, <<"stopped before it was dispatched">>}}
                end,
            case Deliver of
                {reply, From} -> _ = gen_server:reply(From, Result);
                {callback, Callback} -> reply_callback(Callback, Result)
            end
        end,
        Pending
    ),
    ok.

cancel_timer(undefined) ->
    ok;
cancel_timer(Timer) ->
    _ = erlang:cancel_timer(Timer),
    ok.

%% Cancels the connection's native operation once the socket is gone. Also runs
%% for a normal stop, where the cancellation is already in flight.
cancel_token_when_down(SockPid, Token) ->
    MRef = erlang:monitor(process, SockPid),
    receive
        {'DOWN', MRef, process, SockPid, _Reason} ->
            _ = ?NIF_MODULE:cancel(Token),
            ok
    end.

cancel_when_down(SockPid, ClientRef) ->
    MRef = erlang:monitor(process, SockPid),
    receive
        {'DOWN', MRef, process, SockPid, _Reason} ->
            _ = ?NIF_MODULE:stop(ClientRef),
            ok
    end.

apply_async_nif(connect, [Token, Opts], _ClientRef, ReplyPid, Id, Request) ->
    ?NIF_MODULE:async_connect(ReplyPid, Id, Request, Token, Opts);
apply_async_nif(use_database, [Database], ClientRef, ReplyPid, Id, Request) ->
    ?NIF_MODULE:async_use_database(ReplyPid, Id, Request, ClientRef, Database);
apply_async_nif(execute, [Sql], ClientRef, ReplyPid, Id, Request) ->
    ?NIF_MODULE:async_execute(ReplyPid, Id, Request, ClientRef, Sql);
apply_async_nif(prepare, [Sql, AutoRebuild], ClientRef, ReplyPid, Id, Request) ->
    ?NIF_MODULE:async_prepare(ReplyPid, Id, Request, ClientRef, Sql, AutoRebuild);
apply_async_nif(execute_prepare, [Statement, Params], ClientRef, ReplyPid, Id, Request) ->
    ?NIF_MODULE:async_execute_prepare(ReplyPid, Id, Request, ClientRef, Statement, Params);
apply_async_nif(close_prepared, [Statement], ClientRef, ReplyPid, Id, Request) ->
    ?NIF_MODULE:async_close_prepared(ReplyPid, Id, Request, ClientRef, Statement).

reply_callback({CallbackFun, CallbackArgs}, Res) ->
    _ = erlang:apply(CallbackFun, CallbackArgs ++ [Res]),
    ok.
