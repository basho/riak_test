-module(sweeper_long_test).
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
%% @doc The large scale test is to test:
%%
%% Properties
%%
%% large scale
%% real time balance working
%% fullsync not halted by nodes up/down/add/remove
%% realtime not halted by nodes up/down/add/remove
%% Sweeper AAE tree rebuild and Sweeper reaper
-behavior(riak_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-export([confirm/0]).

-record(state, {a_up = [], a_down = [], a_left = [], b_up= [], b_down= [], b_left =[]}).

-define(Conf,
        [{riak_kv, [{delete_mode, keep},
                    {tombstone_grace_period, 3600}, %% 1h in s
                    {reap_sweep_interval, 60 * 60}, %% 60 min
                    {anti_entropy_expire, 60 * 60 * 1000},
                    {storage_backend, riak_kv_eleveldb_backend},
                    {anti_entropy, {on, []}}
                   ]},
         {riak_repl, [{realtime_connection_rebalance_max_delay_secs, 30},%% speed up rebalancing a bit
                      {fullsync_strategy, aae},
                      {fullsync_on_connect, false},
                      {fullsync_interval, 55}
                     ]}
        ]).

-define(SizeA, 4).
-define(SizeB, 4).

-define(Sleep, 1 * 60 * 1000).

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    lager:info("Setup test"),
    {ANodes, BNodes} =
        deploy_clusters_with_rt([{?SizeA, ?Conf}, {?SizeB,?Conf}], '<->'),

    State = #state{ a_up = ANodes, b_up = BNodes},
    lager:info("Start bench"),
    start_basho_bench(ANodes),
    put(test_start, now()),

    timer:sleep(?Sleep),
    run_full_sync(State),
    timer:sleep(?Sleep),

    State2 = node_a_down(State),
    rt:wait_until_no_pending_changes(all_active_nodes(State2)),

    State3 = node_b_down(State2),
    rt:wait_until_no_pending_changes(all_active_nodes(State3)),

    State4 = node_a_up(State3),
    rt:wait_until_no_pending_changes(all_active_nodes(State4)),

    State5 = node_b_up(State4),
    rt:wait_until_no_pending_changes(all_active_nodes(State5)),

    run_full_sync(State5),
    timer:sleep(?Sleep),

    timer:sleep(8 *60 * 60 * 1000),
    pass.

run_full_sync(State) ->
    time_stamp_action(run_full_sync, "A->B"),
    LeaderA = prepare_cluster(State#state.a_up, State#state.b_up),
    {FullsyncTime, _} = timer:tc(repl_util,
                                  start_and_wait_until_fullsync_complete,
                                  [LeaderA]),
    time_stamp_action(full_done, FullsyncTime div 1000000).

start_basho_bench(Nodes) ->
    PbIps = lists:map(fun(Node) ->
                              {ok, [{PB_IP, PB_Port}]} = rt:get_pb_conn_info(Node),
                              {PB_IP, PB_Port}
                      end, Nodes),

    LoadConfig = bacho_bench_config(PbIps),
    spawn_link(fun() -> rt_bench:bench(LoadConfig, Nodes, "50percentbackround", 1, false) end).


bacho_bench_config(HostList) ->
    BenchRate =
        rt_config:get(basho_bench_rate, 20),
    BenchDuration =
        rt_config:get(basho_bench_duration, infinity),
    KeyGen =
        rt_config:get(basho_bench_keygen, {int_to_bin_bigendian, {pareto_int, 1000000000}}),
    ValGen =
        rt_config:get(basho_bench_valgen, {exponential_bin, 100, 500}),
    Operations =
        rt_config:get(basho_bench_operations, [{get, 1},{put, 1},{delete, 2}]),
    Bucket =
        rt_config:get(basho_bench_bucket, <<"mybucket">>),
    Driver =
        rt_config:get(basho_bench_driver, riakc_pb),

    rt_bench:config(BenchRate,
                    BenchDuration,
                    HostList,
                    KeyGen,
                    ValGen,
                    Operations,
                    Bucket,
                    Driver).

random_action(State) ->
    [_|ValidAUp] = State#state.a_up,
    [_|ValidBUp] = State#state.b_up,
    NodeActionList =
        lists:flatten(
          [add_actions(ValidAUp, fun node_a_down/2),
           add_actions(ValidBUp, fun node_b_down/2),
           add_actions(State#state.a_down, fun node_a_up/2),
           add_actions(State#state.b_down, fun node_b_up/2)]),
    {Node, Action} = lists:nth(random:uniform(length(NodeActionList)), NodeActionList),
    Action(State, Node).

add_actions(Nodes, Action) ->
    [{Node, Action} || Node <- Nodes].

%%%%%%%% Start / Stop

node_a_down(State) ->
    node_a_down(State, lists:last(State#state.a_up)).
node_b_down(State) ->
    node_b_down(State, lists:last(State#state.b_up)).
node_a_up(State) ->
    node_a_up(State, lists:last(State#state.a_down)).
node_b_up(State) ->
    node_b_up(State, lists:last(State#state.b_down)).

node_a_down(State, Node) ->
    stop(Node),
    new_state(State, node_a_down, [Node]).
node_b_down(State, Node) ->
    stop(Node),
    new_state(State, node_b_down, [Node]).
node_a_up(State, Node) ->
    start(Node),
    new_state(State, node_a_up, [Node]).
node_b_up(State, Node) ->
    start(Node),
    new_state(State, node_b_up, [Node]).

stop(Node) ->
    rt:stop(Node),
    rt:wait_until_unpingable(Node),
    time_stamp_action(stop, Node),
    timer:sleep(5000),
    true.
start(Node) ->
    rt:start(Node),
    rt:wait_until_ready(Node),
    timer:sleep(5000),
    time_stamp_action(start, Node),
    true.

%%%%%%%% Leave / Join
node_a_leave(State) ->
    node_a_leave(State, lists:last(State#state.a_up)).
node_b_leave(State) ->
    node_b_leave(State, lists:last(State#state.b_up)).
node_a_join(State) ->
    node_a_join(State, lists:last(State#state.a_left)).
node_b_join(State) ->
    node_b_join(State, lists:last(State#state.b_left)).

node_a_leave(State, Node) ->
    leave(Node),
    rt:wait_until_unpingable(Node),
    new_state(State, node_a_leave, [Node]).
node_b_leave(State, Node) ->
    leave(Node),
    rt:wait_until_unpingable(Node),
    new_state(State, node_b_leave, [Node]).
node_a_join(State, Node) ->
    join(Node, hd(State#state.a_up)),
    new_state(State, node_a_join, [Node]).
node_b_join(State, Node) ->
    join(Node, hd(State#state.b_up)),
    new_state(State, node_b_join, [Node]).

leave(Node) ->
    time_stamp_action(leave, Node),
    rt:leave(Node).
join(Node, Node1) ->
    start(Node),
    rt:wait_until_pingable(Node),
    rt:staged_join(Node, Node1),
    rt:plan_and_commit(Node1),
    time_stamp_action(join, Node),
    rt:try_nodes_ready([Node], 3, 500).

%%%%%%%% Update state after action
new_state(S, node_a_down, Node) ->
    S#state{a_up = S#state.a_up -- Node,
            a_down = S#state.a_down ++ Node};
new_state(S, node_b_down, Node) ->
    S#state{b_up = S#state.b_up -- Node,
            b_down = S#state.b_down ++ Node};
new_state(S, node_a_up, Node) ->
    S#state{a_down = S#state.a_down -- Node,
            a_up = S#state.a_up ++ Node};
new_state(S, node_b_up, Node) ->
    S#state{b_down = S#state.b_down -- Node,
            b_up = S#state.b_up ++ Node};

new_state(S, node_a_leave, Node) ->
    S#state{a_up = S#state.a_up -- Node,
            a_left = S#state.a_left ++ Node};
new_state(S, node_b_leave, Node) ->
    S#state{b_up = S#state.b_up -- Node,
            b_left = S#state.b_left ++ Node};
new_state(S, node_a_join, Node) ->
    S#state{a_left = S#state.a_left -- Node,
            a_up = S#state.a_up ++ Node};
new_state(S, node_b_join, Node) ->
    S#state{b_left = S#state.b_left -- Node,
            b_up = S#state.b_up ++ Node}.

all_active_nodes(State) ->
    State#state.a_up ++ State#state.b_up.

prepare_cluster([AFirst|_] = ANodes, [BFirst|_]) ->
    lager:info("Prepare cluster for fullsync"),
    LeaderA = rpc:call(AFirst,
                       riak_core_cluster_mgr, get_leader, []),
    {ok, {IP, Port}} = rpc:call(BFirst,
                                application, get_env, [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, IP, Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes), %% Only works when all nodes in ANodes are up.
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    lager:info("Prepare cluster for fullsync done"),
    LeaderA.

time_stamp_action(Action, MetaData) ->
lager:info("repl_test ~p ~p ~p", [time_since_test_start(), Action, MetaData]).

time_since_test_start() ->
    timer:now_diff(now(), get(test_start)) div 1000000.

random_up_down(State, N) ->
    _ = random:seed(now()),
    lists:foldl(fun(_N, StateIn) ->
                                 NewState = random_action(StateIn),
                                 run_full_sync(NewState)
                         end, State, lists:seq(1,N)).

deploy_clusters_with_rt(ClusterSetup, Direction) ->
    [ANodes, BNodes] = rt:build_clusters(ClusterSetup),
    setup_cluster_rt([ANodes, BNodes], Direction).

setup_cluster_rt([ANodes, BNodes], Direction) ->
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes)),
    ?assertEqual(ok, rt:wait_until_ring_converged(BNodes)),
    case Direction of
        '<->' ->
            setup_rt(ANodes, '->', BNodes), setup_rt(ANodes, '<-', BNodes);
        _ ->
            setup_rt(ANodes, Direction, BNodes)
        end,
    {ANodes, BNodes}.


setup_rt(ANodes, '->', BNodes) ->
    AFirst = hd(ANodes),
    BFirst = hd(BNodes),
    %% A -> B
    connect_clusters(AFirst, BFirst),
    repl_util:enable_realtime(AFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes)),
    repl_util:start_realtime(AFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes));

setup_rt(ANodes, '<-', BNodes) ->
    AFirst = hd(ANodes),
    BFirst = hd(BNodes),
    %% B -> A
    connect_clusters(BFirst, AFirst),
    repl_util:enable_realtime(BFirst, "A"),
    ?assertEqual(ok, rt:wait_until_ring_converged(BNodes)),
    repl_util:start_realtime(BFirst, "A"),
    ?assertEqual(ok, rt:wait_until_ring_converged(BNodes)).

%% @doc Connect two clusters for replication using their respective
%%      leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, IP, Port).
