-module(riak_core_connection_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_core_connection_orig).

%% @doc Modify port to cause connection error

return_econnrefused(Addr,_ClientSpec) ->
    ?I_INFO("Returning econnrefused for all connections to: ~p",[Addr]),
    {error, econnrefused}.
