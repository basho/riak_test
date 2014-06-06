%% @doc
%% This module implements a riak_test to exercise the Active Anti-Entropy Fullsync replication.
%% It sets up two clusters, runs a fullsync over all partitions, and verifies the missing keys
%% were replicated to the sink cluster.
%%
%% Specically, this test sets a custom N-value on the test bucket on the source cluster, which
%% is going to break the AAE fullsync. We don't yet handle AAE fullsync when bucket N values differ
%% between the two clusters. "not_responsible" is returned reply during the hashtree compare and
%% the fullsync source module should restart the connection using the keylist strategy.

-module(repl_aae_fullsync_custom_n).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    NumNodesWanted = 6,         %% total number of nodes needed
    ClusterASize = 3,           %% how many to allocate to cluster A
    NumKeysAOnly = 10000,       %% how many keys on A that are missing on B
    NumKeysBoth = 10000,        %% number of common keys on both A and B
    Conf = [                    %% riak configuration
            {riak_core,
                [
                 {ring_creation_size, 8},
                 {default_bucket_props, [{n_val, 1}, {allow_mult, false}]}
                ]
            },
            {riak_kv,
                [
                 %% Specify fast building of AAE trees
                 {anti_entropy, {on, []}},
                 {anti_entropy_build_limit, {100, 1000}},
                 {anti_entropy_concurrency, 100}
                ]
            },
            {riak_repl,
             [
              {fullsync_strategy, aae},
              {fullsync_on_connect, false},
              {fullsync_interval, disabled}
             ]}
           ],

    %% build clusters
    {ANodes, BNodes} = repl_aae_fullsync_util:make_clusters(NumNodesWanted, ClusterASize, Conf),

    %% run test
    aae_fs_test(NumKeysAOnly, NumKeysBoth, ANodes, BNodes),
    pass.

aae_fs_test(NumKeysAOnly, NumKeysBoth, ANodes, BNodes) ->
    AFirst = hd(ANodes),
    BFirst = hd(BNodes),
    AllNodes = ANodes ++ BNodes,

    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-systest_a">>,

    %% Set a different bucket N value between the two clusters
    NewProps = [{n_val, 2}],
    DefaultProps = get_current_bucket_props(BNodes, TestBucket),
    lager:info("Setting custom bucket n_val = ~p on node ~p", [2, AFirst]),
    update_props(DefaultProps, NewProps, AFirst, ANodes, TestBucket),

    %% populate them with data
    repl_aae_fullsync_util:prepare_cluster_data(TestBucket, NumKeysAOnly, NumKeysBoth, ANodes, BNodes),

    %%---------------------------------------------------------
    %% TEST: fullsync, check that non-RT'd keys get repl'd to B
    %% keys: 1..NumKeysAOnly
    %%---------------------------------------------------------

    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    rt:log_to_nodes(AllNodes, "Test fullsync from cluster A leader ~p to cluster B", [LeaderA]),
    lager:info("Test fullsync from cluster A leader ~p to cluster B", [LeaderA]),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    %% Start fullsync and wait for it to finish.
    {Time,_} = timer:tc(repl_util,start_and_wait_until_fullsync_complete,[LeaderA]),
    lager:info("Fullsync completed in ~p seconds", [Time/1000/1000]),

    %% verify data is replicated to B
    rt:log_to_nodes(AllNodes, "Verify: Reading ~p keys repl'd from A(~p) to B(~p)",
                 [NumKeysAOnly, LeaderA, BFirst]),
    lager:info("Verify: Reading ~p keys repl'd from A(~p) to B(~p)",
               [NumKeysAOnly, LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 1, NumKeysAOnly,
                                             TestBucket, 1)),

    ok.

update_props(DefaultProps, NewProps, Node, Nodes, Bucket) ->
    lager:info("Setting bucket properties ~p for bucket ~p on node ~p", 
               [NewProps, Bucket, Node]),
    rpc:call(Node, riak_core_bucket, set_bucket, [Bucket, NewProps]),    
    rt:wait_until_ring_converged(Nodes),

    UpdatedProps = get_current_bucket_props(Nodes, Bucket),
    ?assertNotEqual(DefaultProps, UpdatedProps).

%% fetch bucket properties via rpc 
%% from a node or a list of nodes (one node is chosen at random)
get_current_bucket_props(Nodes, Bucket) when is_list(Nodes) ->    
    Node = lists:nth(length(Nodes), Nodes),
    get_current_bucket_props(Node, Bucket);
get_current_bucket_props(Node, Bucket) when is_atom(Node) ->
    rpc:call(Node, 
             riak_core_bucket,
             get_bucket,
             [Bucket]).
