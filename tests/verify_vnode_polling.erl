%% -------------------------------------------------------------------
%%
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
%%% @doc
%%% riak_test for soft-limit vnode polling and put fsm routing
%%% see riak/1661 for details.
%%% @end


-module(verify_vnode_polling).
-behavior(riak_test).
-compile([export_all, nowarn_export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"bucket">>).
-define(KEY, <<"key">>).
-define(VALUE, <<"value">>).

-define(RING_SIZE, 8).

-define(CONF(MBoxCheck),
        [{riak_kv, 
                [{anti_entropy, {off, []}},
                    {mbox_check_enabled, MBoxCheck}]},
            {riak_core,
                [{default_bucket_props,
                    [{allow_mult, true},
                        {dvv_enabled, true},
                        {ring_creation_size, ?RING_SIZE},
                        {vnode_management_timer, 1000},
                        {handoff_concurrency, 100},
                        {vnode_inactivity_timeout, 1000}]}]}]).

confirm() ->
    Nodes = rt:build_cluster(5, ?CONF(true)),
    Node1 = hd(Nodes),

    Preflist = rt:get_preflist(Node1, ?BUCKET, ?KEY),

    lager:info("Preflist ~p~n", [Preflist]),

    %% NOTE: The order of these tests IS IMPORTANT, as there is no
    %% facility currently to unload/disable intercepts once loaded.
    test_disabled_mbox_check(Nodes, Preflist),
    test_no_local_pl_forward(Nodes, Preflist, true),
    test_local_coord(Preflist),
    test_forward_on_local_softlimit(Preflist),
    test_local_coord_all_loaded(Preflist),
    test_forward_least_loaded(Nodes, Preflist),
    test_mbox_poll_timeout(Nodes, Preflist),

    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ?CONF(false)) end, Nodes),
    rt:wait_until_ring_converged(Nodes),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end, Nodes),
    test_no_local_pl_forward(Nodes, Preflist, false),
    pass.

%% @doc the case the client themselves disabled the mbox check
test_disabled_mbox_check(Nodes, Preflist) ->
    lager:info("test_disabled_mbox_check"),
    {FSMNode, Client} = non_pl_client(Nodes, Preflist),

    WriteRes = client_write(Client, ?BUCKET, ?KEY, ?VALUE, [{mbox_check, false}]),
    ?assertEqual(ok, WriteRes),

    Stats = get_all_nodes_stats(Nodes),
    %% should be a normal good old fashioned coord_redirect stat bump
    FSMNodeStats = proplists:get_value(FSMNode, Stats),
    CoordRedirCnt = proplists:get_value(coord_redirs_total, FSMNodeStats),
    ?assertEqual(1, CoordRedirCnt).

%% @doc the base case where a non-pl node receives the put, and
%% chooses the first unloaded vnode to respond as the coordinator to
%% forward to
test_no_local_pl_forward(Nodes, Preflist, IsEnabled) ->
    lager:info("test_no_local_pl_forward"),
    {FSMNode, Client} = non_pl_client(Nodes, Preflist),

    WriteRes = client_write(Client, ?BUCKET, ?KEY, ?VALUE),
    ?assertEqual(ok, WriteRes),

    Stats = get_all_nodes_stats(Nodes),
    FSMNodeStats = proplists:get_value(FSMNode, Stats),

    CoordRedirCnt = proplists:get_value(coord_redir_unloaded_total, FSMNodeStats),
    case IsEnabled of
        true ->
            ?assertEqual(1, CoordRedirCnt);
        false ->
            ?assertEqual(0, CoordRedirCnt)
    end.


%% @doc check that when a node is on the preflist, and is unloaded, it
%% coords
test_local_coord(Preflist) ->
    lager:info("test_local_coord"),
    PLNodes = [PLNode || {{_Idx, PLNode}, _Type} <-  Preflist],
    {ok, Client} = riak:client_connect(hd(PLNodes)),

    WriteRes = client_write(Client, ?BUCKET, ?KEY, ?VALUE),
    ?assertEqual(ok, WriteRes),

    Stats = get_all_nodes_stats(PLNodes),
    %% should be a stat bump for local coord
    CoordNodeStats = proplists:get_value(hd(PLNodes), Stats),
    LocalCoordCnt = proplists:get_value(coord_local_unloaded_total, CoordNodeStats),
    ?assertEqual(1, LocalCoordCnt),
    [?assertEqual(0, proplists:get_value(coord_local_unloaded_total, NodeStats)) ||
        {Node, NodeStats} <- Stats, Node /= hd(PLNodes)].


%% @doc use an intercept to simulate a soft-limited local vnode, check
%% that the put is forwarded
test_forward_on_local_softlimit(Preflist) ->
    lager:info("test_forward_on_local_softlimit"),
    PLNodes = [PLNode || {{_Idx, PLNode}, _Type} <-  Preflist],
    HDNode = hd(PLNodes),
    {ok, Client} = riak:client_connect(HDNode),

    ok = rt_intercept:add(HDNode,
                          {riak_core_vnode_proxy,
                           [
                            {{soft_load_mailbox_check, 2}, soft_load_mbox}
                           ]}),

    lager:info("interecept added"),
    {{Idx, Node}, _} = hd(Preflist),
    kill_proxy(Idx, Node),

    WriteRes = client_write(Client, ?BUCKET, ?KEY, ?VALUE),
    ?assertEqual(ok, WriteRes),

    Stats = get_all_nodes_stats(PLNodes),
    %% %% with the local soft-limit, should be a bump in the stat
    %% `coord_redir_loaded_local'
    CoordNodeStats = proplists:get_value(hd(PLNodes), Stats),
    LocalLoadedForwardCnt = proplists:get_value(coord_redir_loaded_local_total, CoordNodeStats),
    SoftloadedCnt = proplists:get_value(soft_loaded_vnode_mbox_total, CoordNodeStats),
    ?assertEqual(1, LocalLoadedForwardCnt),
    ?assertEqual(1, SoftloadedCnt).

%% @doc tests that when all nodes are softloaded, the local
%% coordinates (and increments the correct stat). Now all nodes in the
%% PL have the intercept loaded.
test_local_coord_all_loaded(Preflist) ->
    lager:info("test_local_coord_all_loaded"),
    PLNodes = [PLNode || {{_Idx, PLNode}, _Type} <-  Preflist],
    HDNode = hd(PLNodes),
    {ok, Client} = riak:client_connect(HDNode),

    [ok, ok, ok] = [rt_intercept:add(Node,
                                     {riak_core_vnode_proxy,
                                      [
                                       {{soft_load_mailbox_check, 2}, soft_load_mbox}
                                      ]}) || Node <- PLNodes],

    lager:info("interecept added to whole preflist"),
    [kill_proxy(Idx, Node) || {{Idx, Node}, _Type} <- Preflist],

    WriteRes = client_write(Client, ?BUCKET, ?KEY, ?VALUE),
    ?assertEqual(ok, WriteRes),

    Stats = get_all_nodes_stats(PLNodes),
    %% with all mboxen loaded, there should be a bump in the stat
    %% `coord_local_soft_loaded'
    CoordNodeStats = proplists:get_value(hd(PLNodes), Stats),
    LocalSoftloadedCoord = proplists:get_value(coord_local_soft_loaded_total, CoordNodeStats),
    SoftloadedCnt = proplists:get_value(soft_loaded_vnode_mbox_total, CoordNodeStats),
    ?assertEqual(1, LocalSoftloadedCoord),
    %% NOE: 4 because we have to take account of previous test, we
    %% don't/can't reset stats between runs.
    ?assertEqual(4, SoftloadedCnt).

%% @doc the same case as above (all loaded) except we land in a
%% non-coordinating node (NOTE: all pl nodes have intercept added already!)
test_forward_least_loaded(Nodes, Preflist) ->
    lager:info("test_forward_least_loaded"),
    {FSMNode, Client} = non_pl_client(Nodes, Preflist),

    WriteRes = client_write(Client, ?BUCKET, ?KEY, ?VALUE),
    ?assertEqual(ok, WriteRes),

    Stats = get_all_nodes_stats(Nodes),
    FSMNodeStats = proplists:get_value(FSMNode, Stats),

    %% a random vnode is forwarded (so not really _least_ loaded)
    CoordRedirLeastLoaded = proplists:get_value(coord_redir_least_loaded_total, FSMNodeStats),
    ?assertEqual(1, CoordRedirLeastLoaded).

%% @doc the case the that no vnode mbox replies within the timeout,
%% and so a random node is forwarded to
test_mbox_poll_timeout(Nodes, Preflist) ->
    lager:info("test_mbox_poll_timeout"),
    {FSMNode, Client} = non_pl_client(Nodes, Preflist),
    PLNodes = [PLNode || {{_Idx, PLNode}, _Type} <-  Preflist],

    [ok, ok, ok] = [rt_intercept:add(Node,
                                     {riak_core_vnode_proxy,
                                      [
                                       {{soft_load_mailbox_check, 2}, timout_mbox_check}
                                      ]}) || Node <- PLNodes],

    lager:info("interecept re-added to whole preflist"),
    [kill_proxy(Idx, Node) || {{Idx, Node}, _Type} <- Preflist],

    WriteRes = client_write(Client, ?BUCKET, ?KEY, ?VALUE),
    ?assertEqual(ok, WriteRes),

    Stats = get_all_nodes_stats(Nodes),
    FSMNodeStats = proplists:get_value(FSMNode, Stats),

    lager:info("coord node stats ~p", [FSMNodeStats]),

    %% there was a timeout waiting for soft-load replies
    MboxTimeoutCnt = proplists:get_value(vnode_mbox_check_timeout_total, FSMNodeStats),
    %% a random vnode is forwarded (so not really _least_ loaded)
    CoordRedirLeastLoaded = proplists:get_value(coord_redir_least_loaded_total, FSMNodeStats),
    %% NOTE: includes previous test update to stats
    ?assertEqual(2, CoordRedirLeastLoaded),
    ?assertEqual(1, MboxTimeoutCnt).

client_write(Client, Bucket, Key, Value) ->
    client_write(Client, Bucket, Key, Value, []).

client_write(Client, Bucket, Key, Value, Opts) ->
    Obj = riak_object:new(Bucket, Key, Value),
    riak_client:put(Obj, Opts, Client).

get_all_nodes_stats(Nodes) ->
    [{Nd, rpc:call(Nd, riak_kv_stat, get_stats, [])} || Nd <- Nodes].

%% @private we have to force the proxy to reload/restart to get the
%% intercept code, so kill it, ugly, sorry, but at the same time,
%% WOW, what other language enables this?
kill_proxy(Idx, Node) ->
    {ProxyName, Node} = rpc:call(Node, riak_core_vnode_proxy, reg_name, [riak_kv_vnode, Idx, Node]),
    ProxyPid = rpc:call(Node, erlang, whereis, [ProxyName]),
    true = rpc:call(Node, erlang, exit, [ProxyPid, kill]).

non_pl_client(Nodes, Preflist) ->
    %% make a client with a node _NOT_ on the preflist
    PLNodes = sets:from_list([Node || {{_Idx, Node}, _Type} <- Preflist]),
    NonPLNodes = sets:to_list(sets:subtract(sets:from_list(Nodes), PLNodes)),

    FSMNode = hd(NonPLNodes),

    {ok, Client} = riak:client_connect(FSMNode),
    {FSMNode, Client}.
