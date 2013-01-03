-module(riak_kv_vnode_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_vnode_orig).

%% @doc Simulate dropped puts by truncating the preflist for every kv
%%      vnode put.  This is useful for testing read-repair, AAE, etc.
dropped_put(Preflist, BKey, Obj, ReqId, StartTime, Options, Sender) ->
    NewPreflist = lists:sublist(Preflist, length(Preflist) - 1),
    %% Uncomment to see modification in logs, too spammy to have on by default
    %% ?I_INFO("Preflist modified from ~p to ~p", [Preflist, NewPreflist]),
    ?M:put_orig(NewPreflist, BKey, Obj, ReqId, StartTime, Options, Sender).

%% @doc Make all KV vnode commands take abnormally long.
slow_handle_command(Req, Sender, State) ->
    timer:sleep(500),
    ?M:handle_command_orig(Req, Sender, State).
