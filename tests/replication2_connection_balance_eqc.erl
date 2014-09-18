-module(replication2_connection_balance_eqc).
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.

%% @doc QuickCheck for connection balance
%%
%% Properties
%%  - Nodes are started/stopped at random (which will start/stop connections)
%%  - Connections shoule be balanced indepenent of nodes going up and down.

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-eqc_group_commands(true).
-include_lib("eqc/include/eqc_statem.hrl").
-compile(export_all).

-record(state, 
        {a_up = [],
         a_down = [],
         b_up= [],
         b_down= []
        }).

-define(Conf,
        [{riak_repl,
          [{realtime_connection_rebalance_max_delay_secs, 1}]}
        ]).

-define(SizeA, 4).
-define(SizeB, 4).

%% ====================================================================
%% riak_test callback
%% ====================================================================
confirm() ->
    repl_util:create_clusters_with_rt([{?SizeA, ?Conf}, {?SizeB,?Conf}]),
    random:seed(erlang:now()),
     ?assert(eqc:quickcheck(eqc:numtests(1, ?MODULE:prop_connection_balance()))),
    pass.

%% Start with SizeA nodes in cluster A and SizeB nodes in cluster B
initial_state() ->
    #state{a_up = cluster_A(),
           b_up = cluster_B()}.

cluster_A() ->
    [node_name(I) || I <- lists:seq(1, ?SizeA)].
cluster_B() ->    
    [node_name(I) || I <- lists:seq(?SizeA + 1, ?SizeA + ?SizeB)].

%% ====================================================================
%% Property
%% ====================================================================

prop_connection_balance() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                run_commands(?MODULE, Cmds),
                true
            end).

postcondition(S, {call, _, Fun, Node}, _) ->
    %% TODO: Wait for X instead of sleep 5000.
    timer:sleep(5000),
    NewS = new_state(S, Fun, Node),
    repl_util:verify_correct_connection(NewS#state.a_up, NewS#state.b_up),
    true.

precondition(_S, _NS) ->
    true.

%% ====================================================================
%% Helpers
%% ====================================================================

next_state(S, _NS, {call, _, Fun, Node}) ->
    new_state(S, Fun, Node).

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
            b_up = S#state.b_up ++ Node}.

node_a_down(Node) ->
    stop(Node).
node_b_down(Node) ->
    stop(Node).
node_a_up(Node) ->
    start(Node).
node_b_up(Node) ->
    start(Node).

stop(Node) ->
    rt:stop(Node),
    rt:wait_until_unpingable(Node),
    true.
start(Node) ->
    rt:start(Node),
    rt:wait_until_ready(Node),
    true.

command(#state{a_up = AU,
               a_down = AD,
               b_up = BU,
               b_down = BD}) ->
    oneof( [{call,?MODULE,node_a_down, [lists:nth(random:uniform(length(AU)), AU)]} || length(AU) > 1] ++
           [{call,?MODULE,node_b_down, [lists:nth(random:uniform(length(BU)), BU)]} || length(BU) > 1] ++
           [{call,?MODULE,node_a_up,   [lists:nth(random:uniform(length(AD)), AD)]} || length(AD) >= 1] ++
           [{call,?MODULE,node_b_up,   [lists:nth(random:uniform(length(BD)), BD)]} || length(BD) >= 1]).

node_name(I) ->
    list_to_atom(lists:flatten(io_lib:format("dev~b@127.0.0.1", [I]))).
