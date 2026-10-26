-module(datalayers_nif).

-export([
    async_connect/5,
    async_use_database/5,
    async_execute/5,
    async_prepare/6,
    async_execute_prepare/6,
    async_close_prepared/5,

    stop/1,

    new_cancel_token/0,
    cancel/1,

    set_max_in_flight/1
]).

-export([init/0]).
-on_load(init/0).

init() ->
    NifName = "libdatalayers_nif",
    Niflib = filename:join(priv_dir(), NifName),
    case erlang:load_nif(Niflib, none) of
        ok ->
            configure(),
            ok;
        {error, _Reason} = Res ->
            Res
    end.

%% The process-wide in-flight cap is read once, from the application environment,
%% and can not be changed at runtime. An unusable value (or no value at all) is
%% replaced by the library's default.
configure() ->
    %% Loading the NIF does not imply that the application spec is loaded. Read
    %% the documented sys.config value before the limiter is materialised.
    _ = application:load(datalayers),
    Value =
        case application:get_env(datalayers, max_in_flight) of
            %% Anything that is not an integer is treated as "not set"; the
            %% accepted range is checked on the native side.
            {ok, Configured} when is_integer(Configured) -> Configured;
            _ -> undefined
        end,
    ok = set_max_in_flight(Value),
    ok.

%% =================================================================================================
%% NIFs
%%
%% Every operation is asynchronous: the call returns `ok` as soon as the work has
%% been submitted, or `{error, Reason}` when the arguments could not be accepted
%% (e.g. a stale resource reference). The result is delivered to `ReplyPid` as
%% `{datalayers_async_result, Id, Result}`, where `Result` is `{ok, Value}`,
%% `{error, Reason}` or `{error, {not_dispatched, Reason}}` (the last one means
%% nothing was sent to the server; whether that is worth retrying is the caller's
%% decision).
%%
%% `Request` is `{DeadlineMs, BudgetMs}` on the Erlang monotonic clock. The
%% deadline is absolute so parameter copying and runtime queueing count against
%% it; a zero budget means "use the connection's own default". No NIF blocks the
%% calling process, so a stalled server can not pin a BEAM scheduler thread.

async_connect(_ReplyPid, _Id, _Request, _Token, _Opts) ->
    not_loaded(?LINE).

async_use_database(_ReplyPid, _Id, _Request, _Client, _Database) ->
    not_loaded(?LINE).

async_execute(_ReplyPid, _Id, _Request, _Client, _Sql) ->
    not_loaded(?LINE).

async_prepare(_ReplyPid, _Id, _Request, _Client, _Sql, _AutoRebuild) ->
    not_loaded(?LINE).

async_execute_prepare(_ReplyPid, _Id, _Request, _Client, _Statement, _Params) ->
    not_loaded(?LINE).

async_close_prepared(_ReplyPid, _Id, _Request, _Client, _Statement) ->
    not_loaded(?LINE).

stop(_Client) ->
    not_loaded(?LINE).

new_cancel_token() ->
    not_loaded(?LINE).

cancel(_Token) ->
    not_loaded(?LINE).

set_max_in_flight(_Value) ->
    not_loaded(?LINE).

%% =================================================================================================
%% Helpers

not_loaded(Line) ->
    erlang:nif_error({error, {not_loaded, [{module, ?MODULE}, {line, Line}]}}).

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end.
