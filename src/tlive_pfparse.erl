-module(tlive_pfparse).

-export([init/0, decode_file/1, decode_chunk/1, open/1, open/2]).

-on_load(init/0).

-define(APPNAME, tlive_pfparse).
-define(LIBNAME, tlive_pfparse).
-define(DEFAULT_BUF_SIZE, 4096).

decode_file(Path) ->
    {ok, Fd} = open(Path),
    do_decode_file(Fd, []).

do_decode_file(Fd, Parsed) ->
    case decode_chunk(Fd) of
        [] ->
            Parsed;
        P ->
            do_decode_file(Fd, Parsed ++ P)
    end.

decode_chunk(_) ->
  not_loaded(?LINE).

open(Path) ->
  open(Path, ?DEFAULT_BUF_SIZE).

open(_, _) ->
  not_loaded(?LINE).

init() ->
  SoName = case code:priv_dir(?APPNAME) of
      {error, bad_name} ->
          case filelib:is_dir(filename:join(["..", priv])) of
              true ->
                  filename:join(["..", priv, ?LIBNAME]);
              _ ->
                  filename:join([priv, ?LIBNAME])
          end;
      Dir ->
          filename:join(Dir, ?LIBNAME)
  end,
  erlang:load_nif(SoName, 0).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
