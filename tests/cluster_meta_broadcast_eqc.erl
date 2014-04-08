%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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
%% ---------------------------------------------------------------------
-module(cluster_meta_broadcast_eqc).
-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(riak_test).
-export([confirm/0]).

-behaviour(eqc_statem).

-export([initial_state/0]).

-define(NUM_TESTS, 4).
-define(RING_SIZE, 16).
-define(LAZY_TIMER, 20).
-define(MANAGER, riak_core_metadata_manager).
-define(PREFIX, {x, x}).
-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).

-include("../include/riak_core_metadata.hrl").

-record(state, {
                node_joining = undefined,
                nodes_up = [],
                nodes_down = [],
                nodes_ready_count = 0,
                cluster_nodes = [],
                ring_size = 0
                }).

-record(node, {name, context}).

%% ====================================================================
%% riak_test callback
%% ====================================================================
confirm() -> 
    ?assert(eqc:quickcheck(eqc:numtests(?NUM_TESTS, ?MODULE:prop_test()))),
    pass.

%% ====================================================================
%% EQC generators
%% ====================================================================
gen_numnodes() ->
    oneof([2, 3, 4, 5, 6]).
key() -> elements([k1, k2, k3, k4, k5]).
val() -> elements([v1, v2, v3, v4, v5]).
msg() -> {key(), val()}.
key_context(Key, NodeContext) when is_list(NodeContext) ->
    case lists:keyfind(Key, 1, NodeContext) of
        false -> undefined;
        {Key, Ctx} -> Ctx
    end;
key_context(_Key, _NodeContext) ->
    undefined.

%% ====================================================================
%% EQC commands (using group commands)
%% ====================================================================

%% -- initial_state --
initial_state() ->
    #state{}.

%% -- add_nodes --
add_nodes_pre(S) -> 
    S#state.nodes_up == [].

add_nodes_args(_S) ->
    ?LET(Num, gen_numnodes(),
    [Num]).

add_nodes(NumNodes) ->
    lager:info("Deploying cluster of size ~p", [NumNodes]),
    Nodes = rt:build_cluster(NumNodes),
    configure_nodes(Nodes),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)).

add_nodes_next(S, _, [NumNodes]) ->
    Nodes = node_list(NumNodes),
    [ start_manager(Node) || Node <- Nodes ],
    NodeList = [ #node{ name = Node, context = [] } || Node <- Nodes ],
    lager:info("Node list:~p", [NodeList]),
    S#state{ nodes_up = NodeList }.
 
%% -- broadcast --
broadcast_pre(S) -> 
   S#state.nodes_up /= [].

broadcast_pre(S, [Node, _, _, _]) -> 
   lists:keymember(Node, #node.name, S#state.nodes_up).

broadcast_args(S) ->
    ?LET({{Key, Val}, #node{name = Name, context = Context}}, 
         {msg(), elements(S#state.nodes_up)},
    [Name, Key, Val, key_context(Key, Context)]).

broadcast(Node, Key0, Val0, Context) ->
    Key = mk_key(Key0),
    lager:info("Doing put of key:~p on node ~p", [Key, Node]),
    Val = rpc:call(Node, ?MANAGER, put, put_arguments(Node, Key, Context, Val0)),
    rpc:call(Node, riak_core_broadcast, broadcast, [broadcast_obj(Key, Val), ?MANAGER]),
    context(Val).

broadcast_next(S, Context, [Node, _Key, _Val, _Context]) ->
    S1 = S#state{ nodes_up = lists:keystore(Node, #node.name, S#state.nodes_up,
                                             #node{ name = Node, context = Context }) },
    lager:info("stored context:~p, node_up:~p.", [Context, S1]),
    S1.

%% -- sleep --
sleep_pre(S) -> S#state.nodes_up /= [].
sleep_args(_) -> [choose(1, ?LAZY_TIMER)].
sleep(N) -> timer:sleep(N).


%% ====================================================================
%% EQC Properties
%% ====================================================================
prop_test() ->
    ?SETUP(fun() -> setup(), fun() -> ok end end,
    ?FORALL(Cmds, ?SIZED(N, resize(N div 2, commands(?MODULE))),
    ?LET(Shrinking, parameter(shrinking, false),
    ?ALWAYS(if Shrinking -> 1; true -> 1 end,
    begin
        lager:info("======================== Will run commands ======================="),
        [lager:info(" Command : ~p~n", [Cmd]) || Cmd <- Cmds],
        {H, S, R} = run_commands(?MODULE, Cmds),
        lager:info("======================== Ran commands ============================"),
        #state{nodes_up = NU, cluster_nodes=CN} = S,
        %{_Trace, Ok} = event_logger:get_events(300, 10000),
        stop_servers(S#state.nodes_up),
        timer:sleep(5000),
        Views = [ {Node, get_view(Node)} || #node{name = Node} <- S#state.nodes_up ],
        lager:info("VIEWS:~p", [Views]),
        Destroy =
        fun({node, N, _}) ->
            lager:info("Wiping out node ~p for good", [N]),
            rt:clean_data_dir(N),
            rt:brutal_kill(N)
            %% Would like to wipe out dirs after stopping, but 
            %% currently we use rpc to do that so it fails.
            end,
            Nodes = lists:usort(NU ++ CN),
            lager:info("======================== Taking all nodes down ~p", [Nodes]),
            lists:foreach(Destroy, Nodes),
            eqc_gen:with_parameter(show_states, true, 
                pretty_commands(?MODULE, Cmds, {H, S, R},
                    conjunction(
                    [ 
                     {consistent, prop_consistent(length(Nodes), Views)},
                     {valid_views, [] == [ bad || {_, View} <- Views, not is_list(View) ]}
%%                    , {termination, equals(ok, ok)}
                ])))
        end)))).


prop_consistent(_NumNodes, []) -> true;
prop_consistent(NumNodes, Views) ->
  Dicts = [ Dict || {_, Dict} <- Views ],
  NumNodes == length(lists:usort(Dicts)).

%% ====================================================================
%% Helpers
%% ====================================================================
broadcast_obj(Key, Val) ->
  #metadata_broadcast{ pkey = Key, obj = Val }.

configure_nodes(Nodes) ->
    [begin
         ok = rpc:call(Node, application, set_env, [riak_core, broadcast_exchange_timer, 4294967295]),
         ok = rpc:call(Node, application, set_env, [riak_core, gossip_limit, {10000000, 4294967295}]),
         rt_intercept:add(Node, {riak_core_broadcast, [{{send,2}, global_send}]})
     end || Node <- Nodes],
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

setup() ->
  error_logger:tty(true),
  (catch proxy_server:start_link()).

start_manager(Node) ->
  lager:info("Starting manager on node:~p", [Node]),
  Dir = atom_to_list(Node),
  os:cmd("mkdir " ++ Dir),
  os:cmd("rm " ++ Dir ++ "/*"),
  (catch exit(whereis(?MANAGER), kill)),
  timer:sleep(1),
  {ok, Mgr} = ?MANAGER:start_link([{data_dir, Dir}, {node_name, Node}]),
  unlink(Mgr).

start_server(Node, Eager, Lazy, Names) ->
  try
    lager:info("running start link with Node:~p Names:~p, Eager:~p, Lazy:~p", [Node, Names, Eager, Lazy]),
    {ok, Pid} = riak_core_broadcast:start_link(Names, Eager, Lazy, Names),
    unlink(Pid),
    start_manager(Node),
    ok
  catch _:Err ->
    io:format("OOPS\n~p\n~p\n", [Err, erlang:get_stacktrace()])
  end.

stop_servers(Nodes) ->
  [ rpc:call(Node, ?MODULE, kill, [riak_core_broadcast])
    || #node{name = Node} <- Nodes ].

where_is_event_logger() ->
  io:format("event_logger: ~p\n", [global:whereis_name(event_logger)]).

%% Cluster/node mgmt
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

