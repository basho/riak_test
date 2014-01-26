-module(cuttlefish_configuration).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->

    CuttlefishConf = [
        {"ring_size", "8"},
        {"leveldb.sync_on_write", "on"}
    ],

    [Node] = rt:deploy_nodes(1, {cuttlefish, CuttlefishConf}),
    {ok, RingSize} = rt:rpc_get_env(Node, [{riak_core, ring_creation_size}]),  
    ?assertEqual(8, RingSize),

    %% test leveldb sync typo
    {ok, LevelDBSync} = rt:rpc_get_env(Node, [{eleveldb, sync}]),  
    ?assertEqual(true, LevelDBSync),


    pass.
