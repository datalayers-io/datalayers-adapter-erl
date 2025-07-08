-module(datalayers_nif).

-export([
    connect/1,
    execute/2,
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
