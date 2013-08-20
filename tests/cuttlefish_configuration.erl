-module(cuttlefish_configuration).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Node] = rt:deploy_nodes(1, {cuttlefish, [{"ring_size", "8"}]}),
    {ok, RingSize} = rt:rpc_get_env(Node, [{riak_core, ring_creation_size}]),  
    ?assertEqual(8, RingSize),
    pass.