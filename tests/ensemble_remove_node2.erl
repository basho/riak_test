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

-module(ensemble_remove_node2).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-compile({parse_transform, rt_intercept_pt}).
-define(M, riak_kv_ensemble_backend_orig).

confirm() ->
    NumNodes = 4,
    NVal = 4,
    Config = ensemble_util:fast_config(NVal),
    lager:info("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(NumNodes, Config, NVal),
    [Node, Node2, Node3, Node4] = Nodes,
    ok = ensemble_util:wait_until_stable(Node, NVal),
    lager:info("Store a value in the root ensemble"),
    {ok, _} = riak_ensemble_client:kput_once(Node, root, testerooni, 
        testvalue, 1000),
    lager:info("Read value from the root ensemble"),
    {ok, _} = riak_ensemble_client:kget(Node, root, testerooni, 1000),
    Members4 = rpc:call(Node, riak_ensemble_manager, get_members, [root]),
    lager:info("Members ~w", [Members4]),
    ?assertEqual(4, length(Members4)),

    lager:info("Creating/activating 'strong' bucket type"),
    rt:create_and_activate_bucket_type(Node, <<"strong">>,
                                       [{consistent, true}, {n_val, NVal}]),
    ensemble_util:wait_until_stable(Node, NVal),
    Bucket = {<<"strong">>, <<"test">>},
    Key = <<"testkey">>,
    PBC = rt:pbc(Node),
    ok = rt:pbc_write(PBC, Bucket, Key, testval),
    Val1 = rt:pbc_read(PBC, Bucket, Key),
    ?assertEqual(element(1, Val1), riakc_obj),

    %% Pretends that local_ensembles still true so cannot exit
    InterceptFun = 
        fun(LeavingNode) ->
            rt_intercept:add(LeavingNode,
                                {riak_kv_vnode,
                                    [{{ready_to_exit, 0},
                                        never_ready_to_exit}]})
        end,
    lists:foreach(InterceptFun, [Node2, Node3, Node4]),

    lager:info("Removing Nodes 2, 3 and 4 from the cluster"),
    rt:leave(Node2),
    ok = ensemble_util:wait_until_stable(Node, NVal),
    rt:leave(Node3),
    ok = ensemble_util:wait_until_stable(Node, NVal),
    rt:leave(Node4),
    ok = ensemble_util:wait_until_stable(Node, NVal),
    Remaining = Nodes -- [Node2, Node3, Node4],
    rt:wait_until_nodes_agree_about_ownership(Remaining), 

    lager:info("Need to wait at least a minute to confirm no exit"),
    timer:sleep(60000),

    %% Nodes should still be in leaving state
    lager:info("Getting ring from node ~w", [Node]),
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Leaving = lists:usort(riak_core_ring:members(Ring, [leaving])),
    ?assertEqual(Leaving, [Node2, Node3, Node4]),

    %% We should still be able to read from k/v ensembles, but the nodes should
    %% never exit
    lager:info("Reading From SC Bucket"),
    Val2 = rt:pbc_read(PBC, Bucket, Key),
    ?assertEqual(element(1, Val2), riakc_obj),
    
    ok = ensemble_util:wait_until_stable(Node, NVal),
    lager:info("Read value from the root ensemble"),
    {ok, _Obj} = riak_ensemble_client:kget(Node, root, testerooni, 1000),
    Members4 = rpc:call(Node, riak_ensemble_manager, get_members, [root]),
    lager:info("Members ~w", [Members4]),
    ?assertEqual(4, length(Members4)),
    Cluster = rpc:call(Node, riak_ensemble_manager, cluster, []),
    lager:info("Cluster ~w", [Cluster]),
    ?assertEqual(4, length(Cluster)),

    lager:info("Removing intercept and waiting until nodes shutdown"),
    ok = rt_intercept:clean(Node2, riak_kv_vnode),
    ok = rt_intercept:clean(Node3, riak_kv_vnode),
    ok = rt_intercept:clean(Node4, riak_kv_vnode),

    ok = rt:wait_until_unpingable(Node2),
    ok = rt:wait_until_unpingable(Node3),
    ok = rt:wait_until_unpingable(Node4),
    rpc:call(Node, riak_core_console, member_status, [[]]),
    rpc:call(Node, riak_core_console, ring_status, [[]]),

    pass.
