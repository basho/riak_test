-module(open_source_replication).
-behavior(riak_test).
-export([confirm/0]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%%%
%%% What This Test Does
%%%

%%% This tests checks that you can successfully upgrade from two versions of 
%%% the old open source riak (without replication) to the new version of
%%% riak that has the newly open sourced riak in it
%%%
%%% The transitions are:
%%% 2.0.5 -> 2.2.5
%%% 2.2.3 -> 2.2.5
%%%

%%% The test protocol described here is:
%%% * create cluster A in the old version
%%% * create cluster B in the old version
%%% * write to A
%%% * confirm that you can't read from B

%%% Upgrade Cluster A to the new version
%%% Upgrade Cluster B to the new version
%%% write to A
%%% confirm that you can't read from B

%%% enable real time replication on A
%%% enable real time replication on B
%%% Write to A
%%% confirm that you can read from B

confirm() ->

    %% set up the intial state
    %% two clusters
    %% * no replication on either of them (old open source)

    [ANodes, BNodes] = rt:build_clusters([{3, previous, []}, {3, previous, []}]),

    AllNodes = ANodes ++ BNodes,

    rt:wait_until_nodes_ready(ANodes),
    rt:wait_until_nodes_ready(BNodes),

    %% in the first test protocol both clusters are on the old version
    %% so no replication happens

    rt:log_to_nodes(AllNodes, "starting replication test"),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    rt:log_to_nodes(AllNodes, "Write data to A while both are in old state (no repl)"),
    ok = run_simple_write_test(AFirst, BFirst, no_repl),

    %% in the second test protocol we upgrade the first cluster and write to it
    %% there is still no replication

    UpgradeNodeFn = fun(Node) ->
                        ok = rt:upgrade(Node, current),
                        ok = rt:wait_for_service(Node, riak_kv)
                end,
    [ok = UpgradeNodeFn(X) || X <- ANodes],
    rt:log_to_nodes(AllNodes, "Write data to A after the first cluster has been updated but the second is in old state (no repl)"),
    ok = run_simple_write_test(AFirst, BFirst, no_repl),

    %% in the third test protocol we upgrade the second cluster and when we write to 
    %% the first there is still no replication because its not enabled

    [ok = UpgradeNodeFn(X) || X <- BNodes],
    rt:log_to_nodes(AllNodes, "Write data to A after both clusters have been updated (no repl)"),
    ok = run_simple_write_test(AFirst, BFirst, no_repl),

    %% in the fourth test protocol we enable replication on both clusters
    %% then when we write to A we can read from B
    
    %% enable fullsync replication
    Conf = [
            {riak_repl,
             [
              {fullsync_on_connect, false},
              {fullsync_interval,   disabled},
              {diff_batch_size,     10}
             ]}
           ],
    ok = rt:set_advanced_conf(all, Conf),

    fin = replication:replication(ANodes, BNodes, false),
    pass.

run_simple_write_test(WriteClusterNode, ReadClusterNode, no_repl) ->
    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-no_repl">>,
    lager:info("Writing 100 more keys to ~p", [WriteClusterNode]),
    ?assertEqual([], replication:do_write(WriteClusterNode, 101, 200, TestBucket, 2)),

    lager:info("Reading 0 keys written to ~p on ~p becuz no replication", 
               [WriteClusterNode, ReadClusterNode]),
    ?assertEqual(0, wait_for_reads(ReadClusterNode, 101, 200, TestBucket, 2)),
    ok;
run_simple_write_test(WriteClusterNode, ReadClusterNode, repl) ->
    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "repl">>,
    lager:info("Writing 100 more keys to ~p", [WriteClusterNode]),
    ?assertEqual([], replication:do_write(WriteClusterNode, 101, 200, TestBucket, 2)),

    lager:info("Reading 100 keys written to ~p on ~p becuz no replication", 
               [WriteClusterNode, ReadClusterNode]),
    ?assertEqual(100, wait_for_reads(ReadClusterNode, 101, 200, TestBucket, 2)),
    ok.

wait_for_reads(Node, Start, End, Bucket, R) ->
    rt:wait_until(Node,
        fun(_) ->
                rt:systest_read(Node, Start, End, Bucket, R) == []
        end),
    Reads = rt:systest_read(Node, Start, End, Bucket, R),
    lager:info("Reads: ~p", [Reads]),
    DropFun = fun({_, {error, notfound}}) -> true;
                 (_)                      -> false
              end,
    SuccessfulReads = lists:dropwhile(DropFun, Reads),
    length(SuccessfulReads).
