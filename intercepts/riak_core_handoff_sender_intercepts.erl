-module(riak_core_handoff_sender_intercepts).
-compile(export_all).
-include("intercept.hrl").
-define(M, riak_core_handoff_sender_orig).

delayed_visit_item_3(K, V, Acc) ->
    timer:sleep(100),
    ?M:visit_item_orig(K, V, Acc).

slightly_delayed_visit_item_3(K, V, Acc) ->
    timer:sleep(1),
    ?M:visit_item_orig(K, V, Acc).
