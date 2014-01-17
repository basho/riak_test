-module(riak_kv_backup_console_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_backup_console_orig).


-define(PASS, io:format("pass", [])).
-define(FAIL, io:format("fail", [])).

verify_console_restore(A, B) ->
    io:format(user, "XXXX ~p ~p ~n", [A,B]),
    %case Val of
    %    ["restore","dev1@127.0.0.1","fish","autoexec.bat"] -> ?PASS;
    %    _ -> ?FAIL
    %end.
    ?FAIL,
    ok.

verify_console_backup(A,B,C) ->
    io:format(user, "XXXX ~p ~p ~p ~n", [A,B,C]),
    %case Val of
    %    [] -> ?PASS;
    %    _ -> ?FAIL
    ?FAIL,
    ok.
