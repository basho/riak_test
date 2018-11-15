%% -------------------------------------------------------------------
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
%%% riak_test for capability guard against using put fsm use of
%%% "soft-limits" via riak_core_vnode_proxy message queues in a mixed
%%% cluster
%%% @end

-module(verify_vnode_polling_caps).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"test-bucket">>).
-define(KEY, <<"key">>).
-define(VALUE, <<"value">>).

confirm() ->
    %% Create a mixed cluster of current and previous
    %% and do some puts, checking stats to show that no "soft-limit"
    %% stats have changed
    %% Upgrade nodes to current
    %% Do some PUTs and check that stats have changed to indicate
    %% soft-limit use

    [Prev1, Prev2, _Curr1, _Curr2] = Nodes = rt:build_cluster([previous, previous, current, current]),

    Preflist = rt:get_preflist(Prev1, ?BUCKET, ?KEY),

    lager:info("Preflist ~p~n", [Preflist]),

    test_no_mbox_check(Nodes, Preflist),

    lager:info("upgrade all to current"),

    rt:upgrade(Prev1, current),
    rt:upgrade(Prev2, current),

    [?assertEqual(ok, rt:wait_until_capability(Node, {riak_kv, put_soft_limit}, true)) || Node <- Nodes],
    test_mbox_check(Nodes, Preflist),

    pass.

%% @doc the case the client themselves disabled the mbox check
test_no_mbox_check(Nodes, Preflist) ->
    lager:info("test_no_mbox_check"),
    {FSMNode, Client} = non_pl_client(Nodes, Preflist),

    WriteRes = client_write(Client, ?BUCKET, ?KEY, ?VALUE),
    ?assertEqual(ok, WriteRes),

    Stats = get_all_nodes_stats(Nodes),
    %% should be a normal good old fashioned coord_redirect stat bump
    FSMNodeStats = proplists:get_value(FSMNode, Stats),
    CoordRedirCnt = proplists:get_value(coord_redirs_total, FSMNodeStats),
    ?assertEqual(1, CoordRedirCnt),
    %% if undefined then zero
    CoordMboxRedirCnt = proplists:get_value(coord_redir_unloaded_total, FSMNodeStats, 0),
    ?assertEqual(0, CoordMboxRedirCnt).

%% @doc the base case where a non-pl node receives the put, and
%% chooses the first unloaded vnode to respond as the coordinator to
%% forward to
test_mbox_check(Nodes, Preflist) ->
    lager:info("test_mbox_check"),
    {FSMNode, Client} = non_pl_client(Nodes, Preflist),

    WriteRes = client_write(Client, ?BUCKET, ?KEY, ?VALUE),
    ?assertEqual(ok, WriteRes),

    Stats = get_all_nodes_stats(Nodes),
    FSMNodeStats = proplists:get_value(FSMNode, Stats),

    CoordMboxRedirCnt = proplists:get_value(coord_redir_unloaded_total, FSMNodeStats),
    ?assertEqual(1, CoordMboxRedirCnt),

    %% i.e. unchanged from above (therefore different code path!)
    CoordRedirCnt = proplists:get_value(coord_redirs_total, FSMNodeStats),
    ?assertEqual(1, CoordRedirCnt).

client_write(Client, Bucket, Key, Value) ->
    client_write(Client, Bucket, Key, Value, []).

client_write(Client, Bucket, Key, Value, Opts) ->
    Obj = riak_object:new(Bucket, Key, Value),
    Client:put(Obj, Opts).

get_all_nodes_stats(Nodes) ->
    [{Nd, rpc:call(Nd, riak_kv_stat, get_stats, [])} || Nd <- Nodes].

non_pl_client(Nodes, Preflist) ->
    %% make a client with a node _NOT_ on the preflist
    PLNodes = sets:from_list([Node || {{_Idx, Node}, _Type} <- Preflist]),
    NonPLNodes = sets:to_list(sets:subtract(sets:from_list(Nodes), PLNodes)),

    FSMNode = hd(NonPLNodes),

    {ok, Client} = riak:client_connect(FSMNode),
    {FSMNode, Client}.
