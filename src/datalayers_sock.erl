-module(datalayers_sock).

-include("datalayers.hrl").

-behavior(gen_server).

-record(state, {
    client :: client_ref() | undefined,
    opts :: opts() | undefined
}).

-define(client_ref(Ref), #state{client = Ref}).

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
    gen_server:call(Client, {command, Command, Args}, infinity).

%% ================================================================================
%% gen_server callbacks
start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #state{client = undefined, opts = undefined}}.

handle_call({command, connect, Opts}, _From, State) ->
    case datalayers_nif:connect(Opts) of
        {ok, ClientRef} when is_reference(ClientRef) ->
            {reply, {ok, ClientRef}, State#state{client = ClientRef, opts = Opts}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(_, _From, State = ?client_ref(ClientRef)) when not is_reference(ClientRef) ->
    {reply, {error, not_connected}, State};
handle_call({command, execute, Sql}, _From, State = ?client_ref(ClientRef)) ->
    case datalayers_nif:execute(ClientRef, Sql) of
        {ok, Result} ->
            {reply, {ok, Result}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({command, prepare, Sql}, _From, State = ?client_ref(ClientRef)) ->
    case datalayers_nif:prepare(ClientRef, Sql) of
        {ok, Prepared} when is_reference(Prepared) ->
            {reply, {ok, Prepared}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(
    {command, execute_prepare, #{prepare_statement := Statement, parameters := Params}},
    _From,
    State = ?client_ref(ClientRef)
) ->
    case datalayers_nif:execute_prepare(ClientRef, Statement, Params) of
        {ok, Result} ->
            {reply, {ok, Result}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(
    {command, close_prepared, #{prepare_statement := Statement}},
    _From,
    State = ?client_ref(ClientRef)
) ->
    case datalayers_nif:close_prepared(ClientRef, Statement) of
        {ok, prepare_closed} ->
            {reply, {ok, prepare_closed}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

handle_cast(stop, State = ?client_ref(ClientRef)) ->
    _ = datalayers_nif:stop(ClientRef),
    {stop, normal, State#state{client = undefined}}.
