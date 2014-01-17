-module(riak_kv_js_manager_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_js_manager_orig).


-define(PASS, io:format("pass", [])).
-define(FAIL, io:format("fail", [])).

verify_console_reload(Val) ->
    io:format(user, "XXXX ~p~n", [Val]),
  case Val of
        ["foo","bar","baz"] -> ?PASS;
        _ -> ?FAIL
    end.

