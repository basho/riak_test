-module(riak_core_broadcast_intercepts).
-compile(export_all).
-include("intercept.hrl").
-define(M, riak_core_broadcast_orig).

global_send(Msg, Peers) when is_list(Peers) ->
    [global_send(Msg, P) || P <- Peers];
global_send(Msg, P) ->
    ?I_INFO("sending through proxy"),
    gen_server:cast({global, cluster_meta_proxy_server}, {node(), riak_core_broadcast, P, Msg}).
