-module(riak_core_connection_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_core_connection_orig).

%% @doc Modify port to cause connection error

return_econnrefused(Addr,ClientSpec) ->
    ?I_INFO("Returning econnrefused for all connections to: ~p",[Addr]),
    NewAddr = {"127.0.0.1",20000},
    ?M:sync_connect_orig(NewAddr,ClientSpec).
