-module(riak_kv_console_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_console_orig).


-define(PASS, io:format("pass", [])).
-define(FAIL, io:format("fail", [])).



verify_console_staged_join(Val) ->
  case Val of
        ["dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_bucket_type_status(Val) ->
    case Val of
        ["foo"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_bucket_type_activate(Val) ->
    case Val of
        ["foo"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_bucket_type_create(Val) ->
    io:format(user, "XXXX~p~n", [Val]),
    case Val of
        ["foo","{props:{[]}}"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_bucket_type_update(Val) ->
    case Val of
        ["foo","{props:{[]}}"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_bucket_type_list(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_join(Val) ->
    case Val of
        ["dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_leave(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_remove(Val) ->
    case Val of
        ["dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_down(Val) ->
    case Val of
        ["dev98@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_status(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_vnode_status(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_ringready(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_repair_2i(Val) ->
    case Val of
        ["status"] -> ?PASS;
        ["kill"] -> ?PASS;
        ["--speed","5","foo","bar","baz"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_aae_status(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_cluster_info(Val) ->
    case Val of
        ["foo","local"] -> ?PASS;
        ["foo","local","dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_reload_code(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_reip(Val) ->
   io:format(user, "XXXX ~p~n", [Val]),
   case Val of
        ["a", "b"] -> ?PASS;
        _ -> ?FAIL
    end.

