%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(verify_listkeys_eqc).
-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(riak_test).
-export([confirm/0]).

-behaviour(eqc_statem).
-export([initial_state/0]).

-define(BUCKET, <<"BUCKET">>).
-define(NUM_KEYS, 5).
-define(NUM_BUCKETS, 2).
-define(N, 3).
-define(R, 2).

-define(NUM_TESTS, 4).
-define(RING_SIZE, 16).
-define(LAZY_TIMER, 20).
-define(MANAGER, riak_core_metadata_manager).
-define(PREFIX, {x, x}).
-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(THRESHOLD_SECS, 10).

-record(state, {
                node_joining = undefined,
                nodes_up = [],
                nodes_down = [],
                nodes_ready_count = 0,
                cluster_nodes = [],
                ring_size = 0,
                num_keys = 0
                }).

-record(node, {name, context}).

%% ====================================================================
%% riak_test callback
%% ====================================================================
confirm() ->
%    lager:set_loglevel(lager_console_backend, warning),
%    OutputFun = fun(Str, Args) -> lager:error(Str, Args) end,
%    ?assert(eqc:quickcheck(eqc:on_output(OutputFun, eqc:numtests(?NUM_TESTS, ?MODULE:prop_test())))),
    ?assert(eqc:quickcheck(eqc:numtests(?NUM_TESTS, ?MODULE:prop_test()))),

    pass.

%% ====================================================================
%% EQC generators
%% ====================================================================
gen_numnodes() ->
    oneof([2, 3, 4, 5]).

key() ->
    elements([k1, k2, k3, k4, k5]).

val() ->
    elements([v1, v2, v3, v4, v5]).

msg() ->
    {key(), val()}.

key_context(Key, NodeContext) when is_list(NodeContext) ->
    case lists:keyfind(Key, 1, NodeContext) of
        false -> undefined;
        {Key, Ctx} -> Ctx
    end;
key_context(_Key, _NodeContext) ->
    undefined.

ring_sizes() ->
    elements([8, 16, 32, 64]).

num_keys() ->
    elements([10, 100, 1000]).

g_uuid() ->
    eqc_gen:bind(eqc_gen:bool(), fun(_) -> druuid:v4_str() end).

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_test() ->
    ?FORALL(Cmds, noshrink(commands(?MODULE)),
            ?WHENFAIL(
               begin
                   _ = lager:error("*********************** FAILED!!!!"
                                   "*******************")
               end,
               ?TRAPEXIT(
                  begin
                      %% rt:setup_harness(dummy, dummy),
                      lager:info("======================== Will run commands:"),
                      [lager:info(" Command : ~p~n", [Cmd]) || Cmd <- Cmds],
                      {H, S, R} = run_commands(?MODULE, Cmds),
                      lager:info("======================== Ran commands"),
                      #state{nodes_up = NU, cluster_nodes=CN} = S,
                      Destroy =
                          fun({node, N, _}) ->
                                  lager:info("Wiping out node ~p for good", [N]),
                                  rt:clean_data_dir(N),
                                  %% rt:brutal_kill(N)
                                  rt:stop(N)
                                  %% Would like to wipe out dirs after stopping, but
                                  %% currently we use rpc to do that so it fails.
                          end,
                      Nodes = lists:usort(NU ++ CN),
                      lager:info("======================== Taking all nodes down ~p", [Nodes]),
                      lists:foreach(Destroy, Nodes),
                      %% _ = rt:teardown(),
                      eqc_gen:with_parameter(show_states,
                                             true,
                                             pretty_commands(?MODULE, Cmds, {H, S, R}, equals(ok, R)))
                  end))).

%% ====================================================================
%% EQC commands (using group commands)
%% ====================================================================

%% -- initial_state --
initial_state() ->
    #state{}.

%% -- add_nodes --
add_nodes_pre(S, _) ->
    S#state.nodes_up == [].

add_nodes_args(_S) ->
    ?LET(Num, gen_numnodes(),
    [Num]).

add_nodes(NumNodes) ->
    lager:info("Deploying cluster of size ~p", [NumNodes]),
    Nodes = rt:build_cluster(NumNodes),
    configure_nodes(Nodes),
    rt:wait_until_nodes_ready(Nodes).

add_nodes_next(S, _, [NumNodes]) ->
    Nodes = node_list(NumNodes),
    NodeList = [ #node{ name = Node, context = [] } || Node <- Nodes ],
    S#state{ nodes_up = NodeList }.

add_nodes_post(_S, _Args, ok) ->
    true;
add_nodes_post(_S, _Args, _) ->
    false.

preload_pre(S) ->
    S#state.nodes_up /= [].

preload_args(S) ->
    [g_uuid(), S#state.nodes_up, num_keys()].

preload(Bucket, Nodes, NumKeys) ->
    Node = hd(Nodes),
    NodeName = Node#node.name,
    lager:info("*******************[CMD]  First node ~p", [NodeName]),
    lager:info("Writing to bucket ~p", [Bucket]),
    %% @TODO Make a separate property to test bucket listing
    %% put_buckets(Node, ?NUM_BUCKETS),
    %% @TODO Verify that puts have
    %% completed using similar wait as in repl_bucket_types test
    put_keys(NodeName, Bucket, NumKeys).

preload_next(S, _, [_, _, NumKeys]) ->
    lager:info("Setting num_keys in State to:~p in post condition", [NumKeys]),
    S#state{ num_keys = NumKeys }.

preload_post([S, [Bucket, Nodes], _R]) ->
    lager:info("in preload post, Bucket:~p, Nodes:~p", [Bucket, Nodes]),
    KeyRes = [ list_keys(Node, Bucket, S#state.num_keys, true) || {node, Node, _} <- Nodes ],
    false == lists:member(false, KeyRes).

%% command(#state{nodes_up=[], nodes_down=[], nodes_ready_count = 0}) ->
%%     {call, ?MODULE, init_cmd, [?NUM_NODES, ring_sizes()]};
%% command(#state{nodes_up=[], nodes_down=[], nodes_ready_count = NRC, cluster_nodes = NR})
%%   when NRC > 0 ->
%%     {call, ?MODULE, first_node, [NR]};
%% command(#state{nodes_up = NU, node_joining = NJ, nodes_down = ND,
%%                nodes_ready_count = NRC, cluster_nodes = NR}) ->
%%     oneof(
%%       [ {call, ?MODULE, check_listkeys_pass, [elements(NU)]}
%%       || length(ND) =< 1 ] ++
%%       %%[ {call, ?MODULE, check_listkeys_fail, [elements(NU)]} || length(NU) < ?N ] ++
%%       [ {call, ?MODULE, join_node, [NRC, NR, elements(NU)]}
%%         || NJ =:= undefined andalso NRC > 0] ++
%%       [ {call, ?MODULE, wait_for_handoff, [NU]} || NJ =/= undefined] ++
%%       [ {call, ?MODULE, stop_node, [elements(NU)]} || length(NU) > 1] ++
%%       [ {call, ?MODULE, start_node, [elements(ND)]} || length(ND) > 0] ++
%%       []).

%% initial_state() ->
%%     #state{}.

%% next_state(S, V,
%%            {call, _, init_cmd, [NRC, _]}) ->
%%     S#state{cluster_nodes = V, nodes_ready_count = NRC};
%% next_state(S = #state{nodes_ready_count = NRC}, Node,
%%            {call, _, first_node, [_Nodes]}) ->
%%     S#state{nodes_up = [Node], nodes_ready_count = NRC - 1};
%% next_state(S = #state{nodes_up = NU, nodes_ready_count = NRC}, V,
%%            {call, _, join_node, _}) ->
%%     S#state {node_joining = V,
%%              nodes_up = NU ++ [V],
%%              nodes_ready_count = NRC - 1};
%% next_state(S, _R,
%%            {call, _, wait_for_handoff, _}) ->
%%     S#state {node_joining = undefined};
%% next_state(S = #state{nodes_up = NU, nodes_down = ND}, _R,
%%            {call, _, start_node, [N]}) ->
%%     S#state{nodes_up = NU ++ [N], nodes_down = ND -- [N]};
%% next_state(S = #state{nodes_up = NU, nodes_down = ND}, _R,
%%            {call, _, stop_node, [N]}) ->
%%     S#state{nodes_up = NU -- [N], nodes_down = ND ++ [N]};
%% next_state(S, _V, _C) ->
%%     S.

%% %% Only call init_cmd on a fresh state
%% precondition(#state{nodes_up=NU, nodes_down=ND, nodes_ready_count=NRC},
%%              {call, _, init_cmd, _}) ->
%%     NU =:= [] andalso ND =:= [] andalso NRC == 0;
%% %% Call first_node right after init_cmd
%% precondition(#state{nodes_up=NU, nodes_down=NU},
%%              {call, _, first_node, _}) ->
%%     NU =:= [] andalso NU =:= [];
%% precondition(#state{nodes_down = ND},
%%              {call, _, start_node, [N]}) ->
%%     lists:member(N, ND);
%% precondition(#state{nodes_up = NU},
%%              {call, _, stop_node, [N]}) ->
%%     lists:member(N, NU);
%% precondition(#state{node_joining = NJ},
%%              {call, _, wait_for_handoff, _}) ->
%%     NJ =/= undefined;
%% precondition(#state{nodes_up = NU}, {call, _, check_listkeys_pass, [N]}) ->
%%     lists:member(N, NU);
%% precondition(_S, _) ->
%%     true.

%% postcondition(_S, _Cmd, _R) ->
%%     true.

%% init_cmd(NumNodes, RingSize) ->
%%     lager:info("*******************[CMD]  Deploying cluster of size ~p with ring size ~p", [NumNodes, RingSize]),
%%     Config = [{riak_core, [{ring_creation_size, RingSize}]}],
%%     Nodes = rt:deploy_nodes(NumNodes, Config),
%%     ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
%%     Nodes.


%% join_node(NodesReadyCount, ReadyNodes, UpNode1) ->
%%     NextNode = lists:nth(length(ReadyNodes) - NodesReadyCount + 1, ReadyNodes),
%%     lager:info("*******************[CMD] Join node ~p to ~p", [NextNode, UpNode1]),
%%     rt:join(NextNode, UpNode1),
%%     NextNode.

%% wait_for_handoff(Ns) ->
%%     lager:info("*******************[CMD] Wait for handoffs ~p", [Ns]),
%%     ?assertEqual(ok, rt:wait_until_no_pending_changes(Ns)).

%% start_node(Node) ->
%%     lager:info("*******************[CMD] Starting Node ~p", [Node]),
%%     rt:start(Node),
%%     lager:info("Waiting for riak_kv service to be ready in ~p", [Node]),
%%     rt:wait_for_service(Node, riak_kv).

%% stop_node(Node) ->
%%     lager:info("*******************[CMD] Stopping Node ~p", [Node]),
%%     rt:stop(Node),
%%     rt:wait_until_unpingable(Node).

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


%% ====================================================================
%% Helpers
%% ====================================================================

configure_nodes(Nodes) ->
    rt:load_modules_on_nodes([?MODULE], Nodes),
    ok.

context(Obj) ->
    riak_core_metadata_object:context(Obj).

get_view(Node) ->
  rpc:call(Node, ?MODULE, get_view, []).

get_view() ->
  It = ?MANAGER:iterator(?PREFIX, '_'),
  iterate(It, []).

iterate(It, Acc) ->
  case ?MANAGER:iterator_done(It) of
    true  -> lists:reverse(Acc);
    false -> iterate(?MANAGER:iterate(It), [?MANAGER:iterator_value(It)|Acc])
  end.

kill(Name) ->
  catch exit(whereis(Name), kill).

mk_key(Key) -> {?PREFIX, Key}.  %% TODO: prefix

node_list(NumNodes) ->
    NodesN = lists:seq(1, NumNodes),
    [?DEV(N) || N <- NodesN].

put_arguments(_Name, Key, Context, Val) ->
  [Key, Context, Val].

%% %% -- broadcast --
%% broadcast_pre(S) ->
%%     S#state.nodes_up /= [].

%% broadcast_pre(S, [Node, _, _, _]) ->
%%     lists:keymember(Node, #node.name, S#state.nodes_up).

%% broadcast_args(S) ->
%%     ?LET({{Key, Val}, #node{name = Name, context = Context}},
%%          {msg(), elements(S#state.nodes_up)},
%%     [Name, Key, Val, key_context(Key, Context)]).

%% broadcast(Node, Key0, Val0, Context) ->
%%     Key = mk_key(Key0),
%%     Val = rpc:call(Node, ?MANAGER, put, put_arguments(Node, Key, Context, Val0)),
%%     rpc:call(Node, riak_core_broadcast, broadcast, [broadcast_obj(Key, Val), ?MANAGER]),
%%     context(Val).

%% broadcast_next(S, Context, [Node, _Key, _Val, _Context]) ->
%%     S#state{ nodes_up = lists:keystore(Node, #node.name, S#state.nodes_up,
%%                                        #node{ name = Node, context = Context }) }.
