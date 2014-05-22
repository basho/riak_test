-module(handoff_ttl).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% only memory supports TTL
    rt:set_backend(memory),
    Conf = [
            {riak_core, [
                    {ring_creation_size, 4}
                    ]},
            {riak_kv, [
                    {ttl, 300}, %% riak 1.4 and earlier are broken and need this
                    {memory_backend, [
                            {ttl, 300}
                            ]}
                    ]}
            ],
    [NodeA, NodeB] = rt:deploy_nodes(2, Conf),

    lager:info("writing 100 keys"),

    ?assertEqual([], rt:systest_write(NodeA, 0, 100, <<"ttl_test">>, 2)),
    ?assertEqual([], rt:systest_read(NodeA, 0, 100, <<"ttl_test">>, 2)),

    rt:join(NodeB, NodeA),

    ?assertEqual(ok, rt:wait_until_nodes_ready([NodeA, NodeB])),
    rt:wait_until_no_pending_changes([NodeA, NodeB]),

    rt:leave(NodeA),
    rt:wait_until_unpingable(NodeA),

    ?assertEqual([], rt:systest_read(NodeB, 0, 100, <<"ttl_test">>, 2)),

    %% TODO Wait the remainder of the 5 minutes and make sure all the keys are
    %% gone
    pass.
