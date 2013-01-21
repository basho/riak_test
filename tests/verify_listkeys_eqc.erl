%% @author engel
%% @doc @todo Add description to verify_listkeys_eqc.
-module(verify_listkeys_eqc).
-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(riak_test).
-export([confirm/0]).

-behaviour(eqc_statem).
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3]).

-define(NUM_NODES, 4).
-define(NUM_TESTS, 4).
-define(BUCKET, <<"BUCKET">>).
-define(NUM_KEYS, 5).
-define(NUM_BUCKETS, 2).
-define(N, 3).
-define(R, 2).

-record(state, {
                node_joining = undefined,
                nodes_up = [], %% [Node]
                nodes_down = [], %% [Node]
                nodes_ready_count = 0,
                cluster_nodes = [] 
                }).

confirm() -> 
    ?assert(eqc:quickcheck(eqc:numtests(?NUM_TESTS, ?MODULE:prop_works()))),
    pass.

prop_works() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?WHENFAIL(begin lager:error("*********************** FAILED!!!! *******************") end,
            ?TRAPEXIT(
            begin
                rt:setup_harness(dummy, dummy),
                lager:info("======================== Will run commands:"),
                [lager:info(" Command : ~p~n", [Cmd]) || Cmd <- Cmds],
                {H, S, R} = run_commands(?MODULE, Cmds),
                lager:info("======================== Ran commands"),
                #state{nodes_up = NU, cluster_nodes=CN} = S,
                Destroy =
                    fun(N) ->
                        lager:info("Wiping out node ~p for good", [N]),
%%                         rt:clean_data_dir(N),
                        rt:brutal_kill(N)
                        %% Would like to wipe out dirs after stopping, but 
                        %% currently we use rpc to do that so it fails.
                    end,
                Nodes = lists:usort(NU ++ CN),
                lager:info("======================== Taking all nodes down ~p", [Nodes]),
                lists:foreach(Destroy, Nodes),
                eqc_gen:with_parameter(show_states, true, pretty_commands(?MODULE, Cmds, {H, S, R}, R == ok))
            end))).

ring_sizes() ->
    elements([8, 16, 32, 64]).

command(#state{nodes_up=[], nodes_down=[], nodes_ready_count = 0}) ->
    {call, ?MODULE, init_cmd, [?NUM_NODES, ring_sizes()]};
command(#state{nodes_up=[], nodes_down=[], nodes_ready_count = NRC, cluster_nodes = NR}) 
  when NRC > 0 ->
    {call, ?MODULE, first_node, [NR]};
command(#state{nodes_up = NU, node_joining = NJ, nodes_down = ND, 
               nodes_ready_count = NRC, cluster_nodes = NR}) ->
    oneof(
      [ {call, ?MODULE, check_listkeys_pass, [elements(NU)]} 
      || length(ND) =< 1 ] ++
      %%[ {call, ?MODULE, check_listkeys_fail, [elements(NU)]} || length(NU) < ?N ] ++
      [ {call, ?MODULE, join_node, [NRC, NR, elements(NU)]}
        || NJ =:= undefined andalso NRC > 0] ++
      [ {call, ?MODULE, wait_for_handoff, [NU]} || NJ =/= undefined] ++
      [ {call, ?MODULE, stop_node, [elements(NU)]} || length(NU) > 1] ++
      [ {call, ?MODULE, start_node, [elements(ND)]} || length(ND) > 0] ++
      []).

initial_state() ->
    #state{}.

next_state(S, V,
           {call, _, init_cmd, [NRC, _]}) ->
    S#state{cluster_nodes = V, nodes_ready_count = NRC};
next_state(S = #state{nodes_ready_count = NRC}, Node,
           {call, _, first_node, [_Nodes]}) ->
    S#state{nodes_up = [Node], nodes_ready_count = NRC - 1};
next_state(S = #state{nodes_up = NU, nodes_ready_count = NRC}, V, 
           {call, _, join_node, _}) ->
    S#state {node_joining = V,
             nodes_up = NU ++ [V],
             nodes_ready_count = NRC - 1};
next_state(S, _R,
           {call, _, wait_for_handoff, _}) ->
    S#state {node_joining = undefined};
next_state(S = #state{nodes_up = NU, nodes_down = ND}, _R, 
           {call, _, start_node, [N]}) ->
    S#state{nodes_up = NU ++ [N], nodes_down = ND -- [N]};
next_state(S = #state{nodes_up = NU, nodes_down = ND}, _R, 
           {call, _, stop_node, [N]}) ->
    S#state{nodes_up = NU -- [N], nodes_down = ND ++ [N]};
next_state(S, _V, _C) ->
    S.

%% Only call init_cmd on a fresh state 
precondition(#state{nodes_up=NU, nodes_down=ND, nodes_ready_count=NRC},
             {call, _, init_cmd, _}) ->
    NU =:= [] andalso ND =:= [] andalso NRC == 0;
%% Call first_node right after init_cmd
precondition(#state{nodes_up=NU, nodes_down=NU},
             {call, _, first_node, _}) ->
    NU =:= [] andalso NU =:= [];
precondition(#state{nodes_down = ND},
             {call, _, start_node, [N]}) ->
    lists:member(N, ND);
precondition(#state{nodes_up = NU},
             {call, _, stop_node, [N]}) ->
    lists:member(N, NU);
precondition(#state{node_joining = NJ}, 
             {call, _, wait_for_handoff, _}) ->
    NJ =/= undefined;
precondition(#state{nodes_up = NU}, {call, _, check_listkeys_pass, [N]}) ->
    lists:member(N, NU);
precondition(_S, _) ->
    true.

postcondition(_S, _Cmd, _R) ->
    true.

init_cmd(NumNodes, RingSize) ->
    lager:info("*******************[CMD]  Deploying cluster of size ~p with ring size ~p", [NumNodes, RingSize]),
    Config = [{riak_core, [{ring_creation_size, RingSize}]}],
    Nodes = rt:deploy_nodes(NumNodes, Config),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    Nodes.

first_node(Nodes) ->
    Node = hd(Nodes),
    lager:info("*******************[CMD]  First node ~p", [Node]),
    put_keys(Node, ?BUCKET, ?NUM_KEYS),
    put_buckets(Node, ?NUM_BUCKETS),
    Node.

join_node(NodesReadyCount, ReadyNodes, UpNode1) ->
    NextNode = lists:nth(length(ReadyNodes) - NodesReadyCount + 1, ReadyNodes),
    lager:info("*******************[CMD] Join node ~p to ~p", [NextNode, UpNode1]),
    rt:join(NextNode, UpNode1),
    NextNode.

wait_for_handoff(Ns) ->
    lager:info("*******************[CMD] Wait for handoffs ~p", [Ns]), 
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Ns)).

start_node(Node) ->
    lager:info("*******************[CMD] Starting Node ~p", [Node]),
    rt:start(Node),
    lager:info("Waiting for riak_kv service to be ready in ~p", [Node]),
    rt:wait_for_service(Node, riak_kv).

stop_node(Node) ->
    lager:info("*******************[CMD] Stopping Node ~p", [Node]),
    rt:stop(Node),
    rt:wait_until_unpingable(Node).

check_listkeys_pass(Node) ->
    lager:info("*******************[CMD]  Checking listkeys, expecting to PASS"),
    list_keys(Node, ?BUCKET, ?NUM_KEYS, true).

check_listkeys_fail(Node) ->
    lager:info("*******************[CMD]  Checking listkeys, expecting to FAIL"),
    list_keys(Node, ?BUCKET, ?NUM_KEYS, false).

put_keys(Node, Bucket, Num) ->
    lager:info("*******************[CMD]  Putting ~p keys into bucket ~p on node ~p", [Num, Bucket, Node]),
    Pid = rt:pbc(Node),
    try
        Keys = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
        Vals = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
        [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val)) || {Key, Val} <- lists:zip(Keys, Vals)]
    after
        catch(riakc_pb_socket:stop(Pid))
    end.

put_buckets(Node, Num) ->
    lager:info("[CMD] Putting ~p buckets on ~p", [Num, Node]),
    Pid = rt:pbc(Node),
    try
        Buckets = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
        {Key, Val} = {<<"test_key">>, <<"test_value">>},
        [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val)) || Bucket <- Buckets]
    after
        catch(riakc_pb_socket:stop(Pid))
    end.

list_keys(Node, Bucket, Num, ShouldPass) ->
    %% Move client to state
    Pid = rt:pbc(Node),
    try
        lager:info("Listing keys on node ~p.", [Node]),
        Res = riakc_pb_socket:list_keys(Pid, Bucket),
        lager:info("Result is ~p", [Res]),
        case ShouldPass of
            true ->
                {ok, Keys} = Res,
                ActualKeys = lists:usort(Keys),
                ExpectedKeys = lists:usort([list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)]),
                assert_equal(ExpectedKeys, ActualKeys);
            _ ->
                {Status, Message} = Res,
                Status == error andalso <<"insufficient_vnodes_available">> == Message
        end
    after
        catch(riakc_pb_socket:stop(Pid))
    end.

assert_equal(Expected, Actual) ->
    case Expected -- Actual of 
        [] -> ok;
        Diff -> lager:info("Expected -- Actual: ~p", [Diff])
    end,
    length(Actual) == length(Expected)
        andalso Actual == Expected.
