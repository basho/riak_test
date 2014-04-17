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

-define(NUM_TESTS, 10).
-define(RING_SIZE, 16).
-define(LAZY_TIMER, 20).
-define(MANAGER, riak_core_metadata_manager).
-define(PROXY_SERVER, cluster_meta_proxy_server).
-define(PREFIX, {x, x}).
-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(THRESHOLD_SECS, 10).
-define(msg_q_ets, msq_q_ets).

-include("../include/riak_core_metadata.hrl").

-record(state, {
                node_joining = undefined,
                nodes_up = [],
                nodes_down = [],
                nodes_ready_count = 0,
                cluster_nodes = [],
                ring_size = 0,
                proxy_pid
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
    lager:info("======================== Starting CM Proxy ======================="),
    ProxyPid = start_proxy_server(),
    unlink(ProxyPid),
    lager:info("Started proxy server, pid:~p", [ProxyPid]),
    #state{ proxy_pid = ProxyPid }.

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
    NodeList = [ #node{ name = Node, context = [] } || Node <- Nodes ],
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
    Val = rpc:call(Node, ?MANAGER, put, put_arguments(Node, Key, Context, Val0)),
    rpc:call(Node, riak_core_broadcast, broadcast, [broadcast_obj(Key, Val), ?MANAGER]),
    maybe_send_msgs(),
    context(Val).

broadcast_next(S, Context, [Node, _Key, _Val, _Context]) ->
    S#state{ nodes_up = lists:keystore(Node, #node.name, S#state.nodes_up,
                                       #node{ name = Node, context = Context }) }.    
        
    
%% ====================================================================
%% EQC Properties
%% ====================================================================
prop_test() ->
    ?FORALL(Cmds, ?SIZED(N, resize(N div 2, commands(?MODULE))),
    ?LET(Shrinking, parameter(shrinking, false),
    ?ALWAYS(if Shrinking -> 1; true -> 1 end,
    begin
        lager:info("======================== Will run commands ======================="),
        [lager:info(" Command : ~p~n", [Cmd]) || Cmd <- Cmds],
        {H, S, R} = run_commands(?MODULE, Cmds),
        lager:info("======================== Ran commands ============================"),
        empty_q_and_stop_proxy(S#state.proxy_pid),
        #state{nodes_up = NU, cluster_nodes=CN} = S,
        Views = [ {Node, get_view(Node)} || #node{name = Node} <- S#state.nodes_up ],
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
                     {consistent, prop_consistent(Views)},
                     {valid_views, [] == [ bad || {_, View} <- Views, not is_list(View) ]}
                ])))
        end))).

prop_consistent([]) -> true;
prop_consistent(Views) ->
    [{_, FirstVal}|RestViews] = Views,
    Res = [ Val == FirstVal || {_, Val} <- RestViews ],
    false == lists:member(false, Res).

%% ====================================================================
%% Helpers
%% ====================================================================
broadcast_obj(Key, Val) ->
  #metadata_broadcast{ pkey = Key, obj = Val }.

configure_nodes(Nodes) ->
    [begin
         ok = rpc:call(Node, application, set_env, [riak_core, broadcast_exchange_timer, 4294967295]),
         ok = rpc:call(Node, application, set_env, [riak_core, broadcast_lazy_timer,  4294967295]),
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

empty_q_and_stop_proxy(Pid) ->
    cluster_meta_proxy_server:burst_send(),
    lager:info("Waiting until the proxy server's queue is empty."),
    ?assertEqual(ok, rt:wait_until(fun() -> 
                                      cluster_meta_proxy_server:is_empty(?THRESHOLD_SECS)
                                   end, 100, 1000)),
    stop_proxy_server(Pid).

maybe_send_msgs() ->
    maybe_send(random:uniform(4)).

maybe_send(R) when R == 3 ->    
    cluster_meta_proxy_server:burst_send();
maybe_send(_) ->
    lager:info("letting queue build..."). 

start_proxy_server() ->
    lager:info("Starting cluster_meta_proxy_server."),
    case cluster_meta_proxy_server:start_link() of
        {ok, Pid} ->
	    Pid;
	{error, {already_started, Pid}} ->
	    Pid
    end.

stop_proxy_server(Pid) ->
    ensure_process_dead(Pid),
    lager:info("cluster_meta_proxy_server now dead.").

ensure_process_dead(Pid) ->
    catch exit(Pid, kill),
    lager:info("Killed ~p, waiting to ensure it's dead.", [Pid]),
    rt:wait_until(fun() -> 
                      undefined == erlang:process_info(Pid)
                  end).
