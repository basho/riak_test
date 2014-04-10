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

%% Tests the specific corner case where two ensemble peers become
%% corrupted one after the other. The goal is to provoke the scenario
%% where one of the peers initially trusts the other and syncs with it,
%% but completes the sync after the peer becomes untrusted.
%%
%% Actually hitting this specific interleaving may require multiple runs,
%% but it has been observed and lead to the addition of the `check_sync`
%% logic to riak_ensemble/riak_ensemble_peer.erl that verifies a peer is
%% still trustworthy after a peer syncs with it.
%%
%% Without the check_sync addition, this test could incorectly report
%% {error, notfound} -- eg. data loss. With the addition, this test
%% should now always pass.

-module(ensemble_interleave).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    NVal = 5,
    Quorum = NVal div 2 + 1,
    Config = ensemble_util:fast_config(NVal),
    lager:info("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(8, Config, NVal),
    Node = hd(Nodes),
    vnode_util:load(Nodes),

    lager:info("Creating/activating 'strong' bucket type"),
    rt:create_and_activate_bucket_type(Node, <<"strong">>,
                                       [{consistent, true}, {n_val, NVal}]),
    ensemble_util:wait_until_stable(Node, NVal),
    Bucket = {<<"strong">>, <<"test">>},
    Keys = [<<N:64/integer>> || N <- lists:seq(1,1000)],

    Key1 = hd(Keys),
    DocIdx = rpc:call(Node, riak_core_util, chash_std_keyfun, [{Bucket, Key1}]),
    PL = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, NVal, riak_kv]),
    All = [VN || {VN, _} <- PL],
    Other = [VN || {VN={_, Owner}, _} <- PL,
                   Owner =/= Node],

    Minority = NVal - Quorum,
    PartitionedVN = lists:sublist(Other, Minority),
    Partitioned = [VNode || {_, VNode} <- PartitionedVN],
    [KillFirst,KillSecond|Suspend] = All -- PartitionedVN,

    io:format("PL: ~p~n", [PL]),
    PBC = rt:pbc(Node),
    Options = [{timeout, 500}],

    rpc:multicall(Nodes, riak_kv_entropy_manager, set_mode, [manual]),
    Part = rt:partition(Nodes -- Partitioned, Partitioned),
    ensemble_util:wait_until_stable(Node, Quorum),

    lager:info("Writing ~p consistent keys", [1000]),
    [ok = rt:pbc_write(PBC, Bucket, Key, Key) || Key <- Keys],

    lager:info("Read keys to verify they exist"),
    [rt:pbc_read(PBC, Bucket, Key, Options) || Key <- Keys],
    rt:heal(Part),

    [begin
         lager:info("Suspending vnode: ~p", [VIdx]),
         vnode_util:suspend_vnode(VNode, VIdx)
     end || {VIdx, VNode} <- Suspend],

    vnode_util:kill_vnode(KillFirst),
    timer:sleep(5000),
    vnode_util:kill_vnode(KillSecond),
    vnode_util:rebuild_vnode(KillFirst),
    rpc:multicall(Nodes, riak_kv_entropy_manager, set_mode, [automatic]),
    ensemble_util:wait_until_stable(Node, Quorum),

    lager:info("Disabling AAE"),
    rpc:multicall(Nodes, riak_kv_entropy_manager, disable, []),
    ensemble_util:wait_until_stable(Node, Quorum),

    lager:info("Re-reading keys to verify they exist"),
    Expect = [ok, {error, timeout}, {error, <<"timeout">>}],
    [rt:pbc_read_check(PBC, Bucket, Key, Expect, Options) || Key <- Keys],
    pass.
