-module(libdatalayers).


-export([load/0]).
-on_load(load/0).

-export([hello/0]).

load() ->
    erlang:load_nif(filename:join(priv(), "libdatalayers"), none).

hello() ->
    not_loaded(?LINE).

not_loaded(Line) ->
    erlang:nif_error({error, {not_loaded, [{module, ?MODULE}, {line, Line}]}}).

priv() ->
    case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end.
