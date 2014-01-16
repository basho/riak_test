-module(riak_kv_console_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_console_orig).


-define(PASS, io:format("pass", [])).
-define(FAIL, io:format("fail", [])).

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
    case Val of
        ["foo", "{\"props\":{[]}"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_bucket_type_update(Val) ->
    case Val of
        ["foo", "{\"props\":{[]}"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_bucket_type_list(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

