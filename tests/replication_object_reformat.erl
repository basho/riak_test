%% Riak test for replication combined with binary riak object cluster downgrading
%%
%% Strategy: run realtime and fullsync replication while doing a cluster downgrade

-module(replication_object_reformat).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    NumNodes = rt:config(num_nodes, 6),
    ClusterASize = rt:config(cluster_a_size, 3),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_kv,
                [
                    {anti_entropy, {off, []}}
                ]
            },
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
             ]}
    ],

    Nodes = rt:deploy_nodes(NumNodes, Conf),

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    replication(ANodes, BNodes, false),
    pass.

replication([AFirst|_] = ANodes, [BFirst|_] = BNodes, Connected) ->

    DowngradeVsn = previous, %% TODO: make configurable

    AllNodes = ANodes ++ BNodes,
    rt:log_to_nodes(AllNodes, "Starting replication-object-reformat test"),

    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-systest_a">>,
%%    FullsyncOnly = <<TestHash/binary, "-fullsync_only">>,
%%    RealtimeOnly = <<TestHash/binary, "-realtime_only">>,
%%    NoRepl = <<TestHash/binary, "-no_repl">>,

    case Connected of
        false ->
            %% clusters are not connected, connect them

            %% write some initial data to A
            lager:info("Writing 100 keys to ~p", [AFirst]),
            ?assertEqual([], repl_util:do_write(AFirst, 1, 100, TestBucket, 2)),

            repl_util:name_cluster(AFirst, "A"),
            repl_util:name_cluster(BFirst, "B"),
            rt:wait_until_ring_converged(ANodes),
            rt:wait_until_ring_converged(BNodes),

            %% TODO: we'll need to wait for cluster names before continuing

            %% get the leader for the first cluster
            repl_util:wait_until_leader(AFirst),
            LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

            {ok, {_IP, Port}} = rpc:call(BFirst, application, get_env,
                [riak_core, cluster_mgr]),
            repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),

            ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
            repl_util:enable_realtime(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes),
            repl_util:start_realtime(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes),
            repl_util:enable_fullsync(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes);
        _ ->
            lager:info("waiting for leader to converge on cluster A"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
            lager:info("waiting for leader to converge on cluster B"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
            %% get the leader for the first cluster
            LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
            lager:info("Leader on cluster A is ~p", [LeaderA]),
            {ok, {_IP, _Port}} = rpc:call(BFirst, application, get_env,
                [riak_core, cluster_mgr])
    end,

    %% verify data in correct format on A and B
    confirm_object_format(AllNodes, v1),

    rt:log_to_nodes(AllNodes,
                    "Write data to A, downgade, and verify replication to B via realtime"),

    %% perform downgrade of riak binary object format on sink cluster
    Nodes = BNodes,
    N = length(Nodes),
    %% key ranges for successive writes to node A, based on how many trips in the loop...
    Firsts = lists:seq(1001,(N*1000)+1,1000),
    Lasts = lists:seq(2000,((N+1)*1000),1000),
    lager:info("BNodes: ~p, Nodes: ~p", [BNodes, Nodes]),
    [begin
         %% write some data on A
         ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
         lager:info("Writing 1000 more keys to ~p", [LeaderA]),
         ?assertEqual([], repl_util:do_write(LeaderA, First, Last, TestBucket, 2)),

         %% reformat objects on cluster
         lager:info("Reformatting objects and downgrading ~p", [Node]),
         run_reformat(Node, Node =:= BFirst), %% wait for handoffs on one node, kill on rest
         rt:wait_until_ring_converged(Nodes),
         confirm_object_format(Nodes, v0),
         rt:upgrade(Node, DowngradeVsn), %% use upgrade to downgrade
         rt:wait_for_service(Node, riak_kv),

         %% make sure cluster is still connected
         lager:info("Ensure clusters connected"),
         ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

         %% verify data is replicated to B
         lager:info("Reading 1000 keys written to ~p from ~p", [LeaderA, BFirst]),
         ?assertEqual(0, repl_util:wait_for_reads(BFirst, First, Last, TestBucket, 2))

     end || {Node,First,Last} <- lists:zip3(Nodes,Firsts,Lasts)],

    %% ensure all BNodes are online
    lager:info("Check all BNodes are pingable and kv service available"),
    [begin
         rt:wait_until_pingable(Node),
         rt:wait_for_service(Node, riak_kv)
     end || Node <- BNodes],

    case Connected of
        false ->
            %% check that the keys we wrote initially aren't replicated yet, because
            %% we've disabled fullsync_on_connect
            lager:info("Check keys written before fullsync are not present on B"),
            Res2 = rt:systest_read(BFirst, 1, 100, TestBucket, 2),
            ?assertEqual(100, length(Res2)),

            rt:log_to_nodes(AllNodes, "Fullsync from leader ~p", [LeaderA]),
            repl_util:start_and_wait_until_fullsync_complete(LeaderA),

            lager:info("Check keys written before fullsync are now present on B"),
            ?assertEqual(0, repl_util:wait_for_reads(BFirst, 1, 100, TestBucket, 2));
        _ ->
            ok
    end,

    lager:info("Test passed"),
    fin.

run_reformat(Node, KillHandoffs) ->
    verify_riak_object_reformat:run_reformat(Node, KillHandoffs).

confirm_object_format(Node, Version) ->
    verify_riak_object_reformat:confirm_object_format(Node, Version).

