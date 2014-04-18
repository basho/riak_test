%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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

-module(ensemble_remove_node).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    NumNodes = 3,
    NVal = 3,
    Config = ensemble_util:fast_config(NVal),
    lager:info("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(NumNodes, Config, NVal),
    [Node, Node2, Node3] = Nodes,
    ok = ensemble_util:wait_until_stable(Node, NVal),
    lager:info("Store a value in the root ensemble"),
    {ok, _} = riak_ensemble_client:kput_once(Node, root, testerooni, 
        testvalue, 1000),
    lager:info("Read value from the root ensemble"),
    {ok, _} = riak_ensemble_client:kget(Node, root, testerooni, 1000),
    lager:info("Removing Nodes 2 and 3 from the cluster"),
    rt:leave(Node2),
    ok = ensemble_util:wait_until_stable(Node, NVal),
    Members1 = rpc:call(Node, riak_ensemble_manager, get_members, [root]),
    lager:info("Root Members (1) = ~p", [Members1]),
    rt:leave(Node3),
    ok = ensemble_util:wait_until_stable(Node, NVal),
    Members2 = rpc:call(Node, riak_ensemble_manager, get_members, [root]),
    lager:info("Root Members (2) = ~p", [Members2]),
    Remaining = Nodes -- [Node2, Node3],
    rt:wait_until_nodes_agree_about_ownership(Remaining), 
    ok = rt:wait_until_unpingable(Node2),
    ok = rt:wait_until_unpingable(Node3),
    lager:info("Read value from the root ensemble"),
    {ok, _Obj} = riak_ensemble_client:kget(Node, root, testerooni, 1000),
    Members3 = rpc:call(Node, riak_ensemble_manager, get_members, [root]),
    ?assertEqual(1, length(Members3)),
    Cluster = rpc:call(Node, riak_ensemble_manager, cluster, []),
    ?assertEqual(1, length(Cluster)),
    pass.
