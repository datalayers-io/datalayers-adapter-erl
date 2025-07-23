-module(datalayers_sock).

-include("datalayers.hrl").

-behavior(gen_server).

-record(state, {
    client :: client_ref() | undefined,
    opts :: opts() | undefined
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
handle_call(?REQ(Func, Args), _From, State = ?client_ref(ClientRef)) ->
    case apply_nif(Func, [ClientRef | Args]) of
        ?is_ok = Ok -> {reply, Ok, State};
        ?is_err = Err -> {reply, Err, State}
    end.

%% handle_info({command})
handle_info(?ASYNC_REQ(Func, Args, {CallbackFun, CallBackArgs}), State) ->
    case apply_nif(Func, Args) of
        ?is_ok = Ok ->
            _ = erlang:apply(CallbackFun, [Ok | CallBackArgs]),
            {noreply, State};
        ?is_err = Err ->
            _ = erlang:apply(CallbackFun, [Err | CallBackArgs]),
            {noreply, State}
    end.

handle_cast(stop, State = ?client_ref(ClientRef)) ->
    _ = datalayers_nif:stop(ClientRef),
    {stop, normal, State#state{client = undefined}}.

%% ================================================================================
%% Helpers

apply_nif(Func, Args) ->
    erlang:apply(?NIF_MODULE, Func, Args).
