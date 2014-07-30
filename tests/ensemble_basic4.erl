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

-module(ensemble_basic4).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    NumNodes = 5,
    NVal = 5,
    Quorum = NVal div 2 + 1,
    Config = ensemble_util:fast_config(NVal),
    lager:info("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(NumNodes, Config, NVal),
    Node = hd(Nodes),

    lager:info("Creating/activating 'strong' bucket type"),
    rt:create_and_activate_bucket_type(Node, <<"strong">>,
                                       [{consistent, true}, {n_val, NVal}]),
    ensemble_util:wait_until_stable(Node, NVal),
    Bucket = {<<"strong">>, <<"test">>},
    Keys = [<<N:64/integer>> || N <- lists:seq(1,1000)],

    Key1 = hd(Keys),
    DocIdx = rpc:call(Node, riak_core_util, chash_std_keyfun, [{Bucket, Key1}]),
    PL = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, NVal, riak_kv]),
    Other = [VN || {VN={_, Owner}, _} <- PL,
                   Owner =/= Node],

    Minority = NVal - Quorum,
    PartitionedVN = lists:sublist(Other, Minority),
    Partitioned = [VNode || {_, VNode} <- PartitionedVN],

    PBC = rt_pb:pbc(Node),

    lager:info("Partitioning quorum minority: ~p", [Partitioned]),
    Part = rt_node:partition(Nodes -- Partitioned, Partitioned),
    rpc:multicall(Nodes, riak_kv_entropy_manager, set_mode, [manual]),
    ensemble_util:wait_until_stable(Node, Quorum),

    lager:info("Writing ~p consistent keys", [1000]),
    [ok = rt_pb:pbc_write(PBC, Bucket, Key, Key) || Key <- Keys],

    lager:info("Read keys to verify they exist"),
    [rt_pb:pbc_read(PBC, Bucket, Key) || Key <- Keys],

    lager:info("Healing partition"),
    rt_node:heal(Part),

    pass.
