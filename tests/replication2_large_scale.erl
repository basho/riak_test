-module(replication2_large_scale).
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%% @doc The large scale test is to test:
%%
%% Properties
%%
%% large scale repl (fullsync + realtime)
%% real time balance working
%% fullsync not halted by nodes up/down/add/remove
%%realtime not halted by nodes up/down/add/remove
%%
-behavior(riak_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-export([confirm/0]).

-record(state, {a_up = [], a_down = [], a_left = [], b_up= [], b_down= [], b_left =[]}).

-define(Conf,
        [{riak_kv, [{storage_backend, riak_kv_eleveldb_backend},
                    {anti_entropy, {on, []}}
                   ]},
         {riak_repl, [{realtime_connection_rebalance_max_delay_secs, 30},%% speed up rebalancing a bit
                      {fullsync_strategy, aae},
                      {fullsync_on_connect, false},
                      {fullsync_interval, disabled}
                     ]}
        ]).

-define(SizeA, 5).
-define(SizeB, 5).

-define(Sleep, 1 * 60 * 1000).

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    stop_bench(),
    {ANodes, BNodes} =
        case rt_config:get(preloaded_data, false) of
            true ->
                repl_util:deploy_clusters_with_rt([{?SizeA, ?Conf}, {?SizeB, ?Conf}], '<->');
            false ->
                repl_util:create_clusters_with_rt([{?SizeA, ?Conf}, {?SizeB,?Conf}], '<->')
        end,

    State = #state{ a_up = ANodes, b_up = BNodes},
%%    rt:wait_until_aae_trees_built(all_active_nodes(State)),


    start_bench(ANodes, BNodes),
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

    stop_bench(),
    timer:sleep(?Sleep),
    run_full_sync(State5),

    pass.

run_full_sync(State) ->
    time_stamp_action(run_full_sync, "A->B"),
    LeaderA = prepare_cluster(State#state.a_up, State#state.b_up),
    
%%    rt:wait_until_aae_trees_built(all_active_nodes(State)),
    {FullsyncTime, _} = timer:tc(repl_util,
                                  start_and_wait_until_fullsync_complete,
                                  [LeaderA]),
    time_stamp_action(full_done, FullsyncTime div 1000000).

start_bench(ANodes, BNodes) ->
    case ?HARNESS of
        rtssh ->
            AllLoadGens = rt_config:get(perf_loadgens, ["localhost"]),
            case length(AllLoadGens) of
                1 ->
                    start_basho_bench(ANodes ++ BNodes, AllLoadGens);
                N ->
                    {ALoadGens, BLoadGens} = lists:split(N div 2, AllLoadGens),
                    start_basho_bench(ANodes, ALoadGens),
                    start_basho_bench(BNodes, BLoadGens)
            end;
        _ ->
            ok
    end.

stop_bench() ->
    case ?HARNESS of
        rtssh ->
            rt_bench:stop_bench();
        _ ->
            ok
    end.

start_basho_bench(Nodes, LoadGens) ->
        PbIps = lists:map(fun(Node) ->
                              {ok, [{PB_IP, PB_Port}]} = rt:get_pb_conn_info(Node),
                              {PB_IP, PB_Port}
                      end, Nodes),

    LoadConfig = bacho_bench_config(PbIps),
    spawn(fun() -> rt_bench:bench(LoadConfig, Nodes, "50percentbackround", 1, false, LoadGens) end).

bacho_bench_config(HostList) ->
    BenchRate =
        rt_config:get(basho_bench_rate, 1),
    BenchDuration =
        rt_config:get(basho_bench_duration, infinity),
    KeyGen =
        rt_config:get(basho_bench_keygen, {to_binstr, "~w", {pareto_int, 10000000}}),
    ValGen =
        rt_config:get(basho_bench_valgen, {exponential_bin, 1000, 10000}),
    Operations =
        rt_config:get(basho_bench_operations, [{get, 10},{put, 2},{delete, 1}]),
    Bucket =
        rt_config:get(basho_bench_bucket, <<"mybucket">>),
    Driver =
        rt_config:get(basho_bench_driver, riakc_pb),
    ReportInterval =
         rt_config:get(basho_bench_report_interval, 5),
    
    rt_bench:config(BenchRate,
                    BenchDuration,
                    HostList, 
                    KeyGen,
                    ValGen,
                    Operations,
                    Bucket,
                    Driver,
                    ReportInterval).

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

