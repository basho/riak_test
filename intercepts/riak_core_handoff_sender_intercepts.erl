-module(riak_core_handoff_sender_intercepts).
-compile(export_all).
-include("intercept.hrl").
-define(M, riak_core_handoff_sender_orig).

delayed_visit_item_3(K, V, Acc) ->
    timer:sleep(100),
    ?M:visit_item_orig(K, V, Acc).

delayed_visit_item_3_1ms(K, V, Acc) ->
    timer:sleep(1),
    ?M:visit_item_orig(K, V, Acc).

start_fold_global_send(TargetNode, Module, {Type, Opts}, ParentPid, SslOpts) ->
    Return = ?M:start_fold_orig(TargetNode, Module, {Type, Opts}, ParentPid, SslOpts),
    catch global:send(start_fold_started_proc, start),
    Return.