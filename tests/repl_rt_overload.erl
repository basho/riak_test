%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% -------------------------------------------------------------------
-module(repl_rt_overload).
-behaviour(riak_test).
-export([confirm/0, check_size/1, slow_write_calls/1, slow_trim_q/1]).
-include_lib("eunit/include/eunit.hrl").

-define(RTSINK_MAX_WORKERS, 1).
-define(RTSINK_MAX_PENDING, 1).

%% Replication Realtime Overload test
%%

%% @doc riak_test entry point
confirm() ->
    %% Start up two >1.3.2 clusters and connect them,
    {LeaderA, LeaderB, ANodes, _BNodes} = make_connected_clusters(),

    %% load intercepts. See ../intercepts/riak_repl_rt_intercepts.erl
    load_intercepts(LeaderB),
    
    %% Enable RT replication from cluster "A" to cluster "B"
    lager:info("Enabling realtime between ~p and ~p", [LeaderA, LeaderB]),
    enable_rt(LeaderA, ANodes),

    %% Verify that heartbeats are being acknowledged by the sink (B) back to source (A)
    %% ?assertEqual(verify_heartbeat_messages(LeaderA), true),

    %%rpc:call(LeaderA, lager, trace_file, ["./log/console.log", [{module, riak_repl2_rtq}], debug]),

    %% Verify RT repl of objects
    verify_rt(LeaderA, LeaderB),

    %%lager:info("Slowing do_write calls on leader B"),
    %%slow_write_calls(LeaderB),

    lager:info("Slowing trim_q calls on leader A"),
    slow_trim_q(LeaderA),

    check_rtq_msg_q(LeaderA),

    verify_overload_writes(LeaderA, LeaderB),

%%    load_rt(LeaderA, LeaderB),

    pass.

%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_rt(LeaderA, ANodes) ->
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes).

%% @doc Verify that RealTime replication is functioning correctly by
%%      writing some objects to cluster A and checking they can be
%%      read from cluster B. Each call creates a new bucket so that
%%      verification can be tested multiple times independently.
verify_rt(LeaderA, LeaderB) ->
    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-rt_test_a">>,
    First = 101,
    Last = 200,

    %% Write some objects to the source cluster (A),
    lager:info("Writing ~p keys to ~p, which should RT repl to ~p",
               [Last-First+1, LeaderA, LeaderB]),
    ?assertEqual([], repl_util:do_write(LeaderA, First, Last, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading ~p keys written from ~p", [Last-First+1, LeaderB]),
    ?assertEqual(0, repl_util:wait_for_reads(LeaderB, First, Last, TestBucket, 2)).

verify_overload_writes(LeaderA, LeaderB) ->
   TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-rt_test_overload">>,
    First = 101,
    Last = 20000,

    %% Write some objects to the source cluster (A),
    lager:info("Writing ~p keys to ~p, to ~p",
               [Last-First+1, LeaderA, LeaderB]), 
    ?assertEqual([], repl_util:do_write(LeaderA, First, Last, TestBucket, 2)),

    lager:info("Reading ~p keys from ~p", [Last-First+1, LeaderB]),
    NumReads = repl_util:wait_for_reads(LeaderB, First, Last, TestBucket, 2),

    lager:info("Received ~p reads, so there were ~p drops", [NumReads, ((Last-First) - NumReads)]).


%% @doc put some load on RealTime replication 
%load_rt(LeaderA, LeaderB) ->
%    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
%                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
%    TestBucket = <<TestHash/binary, "-rt_test_a">>,
%    First = 101,
%    Last = 2000100,%
%
%    %% Write some objects to the source cluster (A),
%    lager:info("Writing ~p keys to ~p, which should RT repl to ~p",
%               [Last-First+1, LeaderA, LeaderB]),
%    ?assertEqual([], repl_util:do_write(LeaderA, First, Last, TestBucket, 2)).

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    lager:info("connect cluster A:~p to B on port ~p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")).

%% @doc Create two clusters of 3 nodes each and connect them for replication:
%%      Cluster "A" -> cluster "B"
make_connected_clusters() ->
    NumNodes = rt_config:get(num_nodes, 2),
    ClusterASize = rt_config:get(cluster_a_size, 1),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
              %% turn off fullsync
              {fullsync_on_connect, false},
              {fullsync_interval, disabled},
              {rtq_max_bytes, 1048576},
              {rtsink_max_workers, ?RTSINK_MAX_WORKERS},
              {rt_heartbeat_timeout, ?RTSINK_MAX_PENDING}
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

    %% get the leader for the first cluster
    lager:info("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    %% get the leader for the second cluster
    lager:info("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    %% Name the clusters
    repl_util:name_cluster(AFirst, "A"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:name_cluster(BFirst, "B"),
    rt:wait_until_ring_converged(BNodes),

    %% Connect for replication
    connect_clusters(AFirst, BFirst),

    {AFirst, BFirst, ANodes, BNodes}.

%% @doc Load intercepts file from ../intercepts/riak_repl2_rtsink_conn_intercepts.erl
%%       and ../intercepts/riak_repl2_rtq_intercepts.erl
load_intercepts(Node) ->
    rt_intercept:load_code(Node).

%% @doc Slow down handle_info (write calls)
slow_write_calls(Node) ->
    %% disable forwarding of the heartbeat function call
    lager:info("Slowing down sink do_write calls on ~p", [Node]),
    rt_intercept:add(Node, {riak_repl2_rtsink_conn,
                            [{{handle_info, 2}, slow_handle_info}]}).

slow_trim_q(Node) ->
    lager:info("Slowing down trim_q calls on ~p", [Node]),
    rt_intercept:add(Node, {riak_repl2_rtq,
                            [{{trim_q, 1}, slow_trim_q}]}).

check_rtq_msg_q(Node) ->
    Pid = spawn(?MODULE, check_size, [Node]),
    Pid.

check_size(Node) ->
    Pid = rpc:call(Node, erlang, whereis, [riak_repl2_rtq]),
    Len = rpc:call(Node, erlang, process_info, [Pid, message_queue_len]),
    io:format("mailbox size of riak_repl2_rtq: ~p", [Len]),

    %%Status = rpc:call(Node, riak_repl2_rtq, status, []),
    %%io:format("status: ~p", [Status]),
    timer:sleep(5000),
    check_size(Node).