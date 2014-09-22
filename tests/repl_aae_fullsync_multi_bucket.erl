
-module(repl_aae_fullsync_multi_bucket).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(Q_VALUE, 8).
-define(N_Value, 3).

confirm() ->
    aae_fs_test_diff_n_buckets(),
    pass.

prepare_test(NumNodesWanted, NumNodesWanted, ClusterASize) ->
    Conf = [                    %% riak configuration
            {riak_core,
                [
                 {ring_creation_size, ?Q_VALUE},
                 {default_bucket_props, [{n_val, ?N_Value}]}
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
              {fullsync_interval, disabled},
              {max_fssource_retries, 5},
              {max_fssource_cluster, 1},
              {max_fssource_node, 1},
              {max_fssink_node, 1}
             ]}
           ],

    %% build clusters
    repl_aae_fullsync_util:make_clusters(NumNodesWanted, ClusterASize, Conf).

aae_fs_test_diff_n_buckets() ->
    NumNodesWanted = 2,         %% total number of nodes needed
    ClusterASize = 1,           %% how many to allocate to cluster A

    {ANodes, BNodes} = prepare_test(NumNodesWanted, NumNodesWanted, ClusterASize),
    LeaderA = repl_aae_fullsync_util:prepare_cluster(ANodes, BNodes),
    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    repl_util:start_and_wait_until_fullsync_complete(LeaderA),


    %% N, NumKeys, Div
    Tests = [{1, 30000, 10}, {3, 20000, 10}, {5 ,10000, 2}],
    
    TestsAndBuckets =
        lists:map(fun({N, NumKeys, Div}) ->
                        TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                                                      <<X>> <= erlang:md5(term_to_binary(now()))]),
                        TestBucket = <<TestHash/binary, "-systest_a">>,
                        case N of
                            ?N_Value ->
                                ok;
                            _ ->
                                NewProps = [{n_val, N}],
                                DefaultProps = repl_util:get_current_bucket_props(ANodes, <<"test">>),
                                repl_util:update_props(DefaultProps, NewProps, AFirst, ANodes, TestBucket),
                                repl_util:update_props(DefaultProps, NewProps, BFirst, BNodes, TestBucket)
                        end,
                        %% populate them with data
                        repl_util:write_to_cluster(AFirst, 1, NumKeys, TestBucket),
                        repl_util:write_to_cluster(BFirst, 1, 1, TestBucket),
                        {TestBucket, N, NumKeys, Div}
                end,Tests),
    
    NumTotalKeys = lists:sum([NumKeys || {_N, NumKeys, _Div} <- Tests]),
    MeanN = lists:sum([N * NumKeys || {N, NumKeys, _Div} <- Tests]) / NumTotalKeys,

    %% Write keys and perform fullsync.
    FullStartTime = erlang:now(),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    repl_util:start_and_wait_until_fullsync_complete(LeaderA),
    FullEndTime = erlang:now(),
    repl_util:validate_aae_fullsync(FullStartTime, FullEndTime, MeanN, ?Q_VALUE, NumTotalKeys, NumTotalKeys),

    lager:info("TotalKey ~p MeanN ~p", [NumTotalKeys, MeanN]),

    lists:foreach(fun({TestBucket, _N, NumKeys, Div}) ->
                          %% Write keys and perform fullsync.
                          StartTime = erlang:now(),
                          ChangedKeys = NumKeys div Div,
                          repl_util:write_to_cluster(AFirst, 1, ChangedKeys, TestBucket),
                          rt:wait_until_aae_trees_built(ANodes ++ BNodes),
                          repl_util:start_and_wait_until_fullsync_complete(LeaderA),

                          EndTime = erlang:now(),
                          repl_util:validate_aae_fullsync(StartTime, EndTime, MeanN, ?Q_VALUE, NumTotalKeys, ChangedKeys)
                  end, TestsAndBuckets).
