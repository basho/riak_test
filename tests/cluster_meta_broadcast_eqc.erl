-module(cluster_meta_broadcast_eqc).
-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(riak_test).
-export([confirm/0]).

-behaviour(eqc_statem).
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3]).

-define(NUM_TESTS, 4).
-define(N, 3).
-define(R, 2).
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

confirm() -> 
    ?assert(eqc:quickcheck(eqc:numtests(?NUM_TESTS, ?MODULE:prop_test()))),
    pass.

%% Generators
gen_numnodes() ->
    oneof([2, 3, 4, 5, 6]).
key() -> elements([k1, k2, k3, k4, k5]).
val() -> elements([v1, v2, v3, v4, v5]).
msg() -> {key(), val()}.
key_context(Key, NodeContext) ->
    case lists:keyfind(Key, 1, NodeContext) of
        false -> undefined;
        {Key, Ctx} -> Ctx
    end.

%% -- Commands ---------------------------------------------------------------

initial_state() ->
    #state{}.

add_nodes_pre(S) -> 
    S#state.nodes_up == [].

add_nodes_args(_S) ->
    ?LET(Num, gen_numnodes(),
    [Num]).

add_nodes(NumNodes) ->
    lager:info("Deploying cluster of size ~p", [NumNodes]),
    Config = [{riak_core, [{ring_creation_size, ?RING_SIZE}]}],
    Nodes = rt:deploy_nodes(NumNodes, Config),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)).

add_nodes_next(S, _, [NumNodes]) ->
    Nodes = node_list(NumNodes),
    NodeList = [ #node{ name = Node, context = [] } || Node <- Nodes ],
    lager:info("Node list:~p", [NodeList]),
    S#state{ nodes_up = NodeList }.
 
%% -- broadcast --
broadcast_pre(S) -> 
   S#state.nodes_up /= [].

%broadcast_pre(S, [Node, _, _, _]) -> 
%   lists:keymember(Node, #node.name, S#state.nodes_up).

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

%broadcast_next(S, Context, [Node, _Key, _Val, _Context]) ->
%  S#state{ broadcast_node = lists:keystore(Node, #node.name, S#state.nodes_up,
%                                  #node{ name = Node, context = Context }) }.

prop_test() ->
    ?FORALL(Cmds, ?SIZED(N, resize(N div 2, commands(?MODULE))),
    ?LET(Shrinking, parameter(shrinking, false),
    ?ALWAYS(if Shrinking -> 1; true -> 1 end,
    begin
        rt:setup_harness(dummy, dummy),
        lager:info("======================== Will run commands ======================="),
        [lager:info(" Command : ~p~n", [Cmd]) || Cmd <- Cmds],
        {H, S, R} = run_commands(?MODULE, Cmds),
        lager:info("======================== Ran commands ============================"),
        #state{nodes_up = NU, cluster_nodes=CN} = S,
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
            eqc_gen:with_parameter(show_states, true, pretty_commands(?MODULE, Cmds, {H, S, R}, R == ok))
        end))).

%% Helpers
start_manager(Node) ->
  Dir = atom_to_list(Node),
  os:cmd("mkdir " ++ Dir),
  os:cmd("rm " ++ Dir ++ "/*"),
  (catch exit(whereis(?MANAGER), kill)),
  timer:sleep(1),
  {ok, Mgr} = ?MANAGER:start_link([{data_dir, Dir}, {node_name, Node}]),
  unlink(Mgr).

mk_key(Key) -> {?PREFIX, Key}.  %% TODO: prefix

put_arguments(_Name, Key, Context, Val) ->
  [Key, Context, Val].

broadcast_obj(Key, Val) ->
  #metadata_broadcast{ pkey = Key, obj = Val }.

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

where_is_event_logger() ->
  io:format("event_logger: ~p\n", [global:whereis_name(event_logger)]).

context(Obj) ->
    riak_core_metadata_object:context(Obj).

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

node_list(NumNodes) ->
    NodesN = lists:seq(1, NumNodes),
    [?DEV(N) || N <- NodesN].
