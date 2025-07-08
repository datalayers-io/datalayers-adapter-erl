-module(datalayers_nif).

-export([
    connect/1,
    execute/2,

    prepare/2,
    execute_prepare/3,
    close_prepared/2,

    stop/1
]).

-export([init/0]).
-on_load(init/0).

init() ->
    NifName = "libdatalayers_nif",
    Niflib = filename:join(priv_dir(), NifName),
    case erlang:load_nif(Niflib, none) of
        ok ->
            ok;
        {error, _Reason} = Res ->
            Res
    end.

%% =================================================================================================
%% NIFs

connect(_Opts) ->
    not_loaded(?LINE).

execute(_Client, _Sql) ->
    not_loaded(?LINE).

prepare(_Client, _Sql) ->
    not_loaded(?LINE).

execute_prepare(_Client, _Statement, _Params) ->
    not_loaded(?LINE).

close_prepared(_Client, _Statement) ->
    not_loaded(?LINE).

stop(_Client) ->
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
