-module(datalayers_sock).

-include("datalayers.hrl").

-behavior(gen_server).

-record(state, {
    client :: client_ref() | undefined,
    opts :: opts() | undefined,
    %% id of the async request currently in flight (single in-flight per
    %% connection, so per-connection ordering is preserved)
    current = undefined :: integer() | undefined,
    %% id => callback, for queued and in-flight async requests
    pending = #{} :: #{integer() => callback()},
    %% requests waiting to be dispatched: {async, Id, Func, Args} runs on the NIF
    %% worker, {sync, From, Func, Args} runs in this process.  Both kinds share the
    %% queue so that one socket preserves the order of sync and async requests.
    queue = queue:new() :: queue:queue(
        {async, integer(), command(), args()} | {sync, gen_server:from(), command(), args()}
    )
}).

-define(client_ref(Ref), #state{client = Ref}).

-define(connect, connect).
-define(is_ok, {ok, _}).
-define(is_err, {error, _}).

-export([
    start/0,
    stop/1,
    sync_command/3
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
    gen_server:call(Client, ?REQ(Command, Args), infinity).

%% ================================================================================
%% gen_server callbacks
start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #state{client = undefined, opts = undefined}}.

handle_call(
    ?REQ(?connect, Args = [Opts]),
    _From,
    State
) ->
    case apply_nif(?connect, Args) of
        {ok, ClientRef} = Ok when is_reference(ClientRef) ->
            {reply, Ok, State#state{client = ClientRef, opts = Opts}};
        ?is_err = Err ->
            {reply, Err, State}
    end;
handle_call(_, _From, State = ?client_ref(ClientRef)) when
    not is_reference(ClientRef)
->
    {reply, {error, not_connected}, State};
handle_call(?REQ(Func, Args), From, State = ?client_ref(ClientRef)) when
    is_reference(ClientRef)
->
    %% One socket serves both synchronous and asynchronous requests, so a sync
    %% request is queued as well: it must not overtake the requests that are already
    %% queued or in flight on this connection.
    Queue = queue:in({sync, From, Func, Args}, State#state.queue),
    {noreply, maybe_dispatch(State#state{queue = Queue})}.

%% handle_info({async, ...}) -- enqueue and dispatch at most one at a time.
handle_info(?ASYNC_REQ(Func, Args, Callback), State) ->
    case State#state.client of
        ClientRef when is_reference(ClientRef) ->
            %% The correlation id is an integer rather than a make_ref(): it is passed
            %% into the Rust NIF and re-encoded into the completion message from a
            %% different environment (the worker's OwnedEnv).  An integer decodes and
            %% encodes as i64 directly, while a Reference is bound to its environment
            %% and would need an extra OwnedEnv::save/load copy.  `unique_integer/1` is
            %% monotonically unique per node and never reused, so it is just as safe as
            %% a reference as a correlation key.
            Id = erlang:unique_integer([monotonic, positive]),
            Pending = maps:put(Id, Callback, State#state.pending),
            Queue = queue:in({async, Id, Func, Args}, State#state.queue),
            {noreply, maybe_dispatch(State#state{pending = Pending, queue = Queue})};
        _ ->
            reply_callback(Callback, {error, not_connected}),
            {noreply, State}
    end;
%% completion message sent by the NIF worker thread
handle_info({datalayers_async_result, Id, Res}, State) ->
    case State#state.current of
        Id ->
            case maps:take(Id, State#state.pending) of
                {Callback, Pending} ->
                    reply_callback(Callback, Res),
                    {noreply, maybe_dispatch(State#state{current = undefined, pending = Pending})};
                error ->
                    {noreply, State}
            end;
        _ ->
            %% stale result (e.g. after stop/restart); ignore
            {noreply, State}
    end;
handle_info(_, State) ->
    %% Ignore other messages
    {noreply, State}.

handle_cast(stop, State = ?client_ref(undefined)) ->
    %% Client Ref already stopped or never connected, ignore
    {stop, normal, State};
handle_cast(stop, State = ?client_ref(ClientRef)) ->
    _ = datalayers_nif:stop(ClientRef),
    %% Answer queued synchronous callers before stopping, otherwise they would wait
    %% forever on `gen_server:call(..., infinity)`.
    ok = reply_pending_sync(State#state.queue),
    {stop, normal, State#state{client = undefined, queue = queue:new()}}.

%% ================================================================================
%% Helpers

maybe_dispatch(State = #state{current = undefined, queue = Queue, client = ClientRef}) when
    is_reference(ClientRef)
->
    case queue:out(Queue) of
        {{value, {async, Id, Func, Args}}, Queue1} ->
            State1 = State#state{queue = Queue1, current = Id},
            case apply_async_nif(Func, [ClientRef | Args], Id) of
                ok ->
                    State1;
                {error, Reason} ->
                    %% Submission failed; complete it now so the caller is not stuck.
                    {Callback, Pending} = maps:take(Id, State1#state.pending),
                    reply_callback(Callback, {error, Reason}),
                    maybe_dispatch(State1#state{current = undefined, pending = Pending})
            end;
        {{value, {sync, From, Func, Args}}, Queue1} ->
            %% Runs in this process; the shared queue is what keeps it ordered with
            %% the asynchronous requests on the same connection.
            Result = apply_nif(Func, [ClientRef | Args]),
            _ = gen_server:reply(From, Result),
            maybe_dispatch(State#state{queue = Queue1});
        {empty, _} ->
            State
    end;
maybe_dispatch(State) ->
    State.

reply_pending_sync(Queue) ->
    case queue:out(Queue) of
        {{value, {sync, From, _Func, _Args}}, Queue1} ->
            _ = gen_server:reply(From, {error, stopped}),
            reply_pending_sync(Queue1);
        {{value, {async, _Id, _Func, _Args}}, Queue1} ->
            %% Async callers are released by the socket monitor on the caller side.
            reply_pending_sync(Queue1);
        {empty, _} ->
            ok
    end.

apply_async_nif(execute, [ClientRef, Sql], Id) ->
    ?NIF_MODULE:async_execute(ClientRef, self(), Id, Sql);
apply_async_nif(prepare, [ClientRef, Sql, AutoRebuild], Id) ->
    ?NIF_MODULE:async_prepare(ClientRef, self(), Id, Sql, AutoRebuild);
apply_async_nif(execute_prepare, [ClientRef, Statement, Params], Id) ->
    ?NIF_MODULE:async_execute_prepare(ClientRef, self(), Id, Statement, Params).

reply_callback({CallbackFun, CallbackArgs}, Res) ->
    _ = erlang:apply(CallbackFun, CallbackArgs ++ [Res]),
    ok.

apply_nif(Func, Args) ->
    erlang:apply(?NIF_MODULE, Func, Args).
