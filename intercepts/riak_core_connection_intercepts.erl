-module(riak_core_connection_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_core_connection_orig).

%% @doc Return econnrefused for all connection attempts
return_econnrefused(Addr,_ClientSpec) ->
    ?I_INFO("Returning econnrefused for all connections to: ~p",[Addr]),
    {error, econnrefused}.

%% @doc Pass through for sync_connect function
sync_connect(Addr, ClientSpec) ->
    ?I_INFO("Intercept is allowing connections"),
    ?M:sync_connect_orig(Addr, ClientSpec).
