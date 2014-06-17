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

-module(ensemble_vnode_crash).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    NumNodes = 5,
    NVal = 5,
    Config = ensemble_util:fast_config(NVal),
    lager:info("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(NumNodes, Config, NVal),
    vnode_util:load(Nodes),
    Node = hd(Nodes),
    ensemble_util:wait_until_stable(Node, NVal),

    lager:info("Creating/activating 'strong' bucket type"),
    rt:create_and_activate_bucket_type(Node, <<"strong">>,
                                       [{consistent, true}, {n_val, NVal}]),
    ensemble_util:wait_until_stable(Node, NVal),
    Bucket = {<<"strong">>, <<"test">>},
    Keys = [<<N:64/integer>> || N <- lists:seq(1,1000)],

    Key1 = hd(Keys),
    DocIdx = rpc:call(Node, riak_core_util, chash_std_keyfun, [{Bucket, Key1}]),
    PL = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, NVal, riak_kv]),
    {{Key1Idx, Key1Node}, _} = hd(PL),

    PBC = rt:pbc(Node),

    lager:info("Writing ~p consistent keys", [1000]),
    [ok = rt:pbc_write(PBC, Bucket, Key, Key) || Key <- Keys],

    lager:info("Read keys to verify they exist"),
    [rt:pbc_read(PBC, Bucket, Key) || Key <- Keys],

    {ok, VnodePid} =rpc:call(Key1Node, riak_core_vnode_manager, get_vnode_pid,
        [Key1Idx, riak_kv_vnode]),
    lager:info("Killing Vnode ~p for Key1 {~p, ~p}", [VnodePid, Key1Node,
            Key1Idx]),
    true = rpc:call(Key1Node, erlang, exit, [VnodePid, testkill]),

    lager:info("Wait for stable ensembles"),
    ensemble_util:wait_until_stable(Node, NVal),
    lager:info("Re-reading keys"),
    [rt:pbc_read(PBC, Bucket, Key) || Key <- Keys],

    lager:info("Killing Vnode Proxy for Key1"),
    Proxy = rpc:call(Key1Node, riak_core_vnode_proxy, reg_name, [riak_kv_vnode,
            Key1Idx]),
    ProxyPid = rpc:call(Key1Node, erlang, whereis, [Proxy]),
    lager:info("Killing Vnode Proxy ~p", [Proxy]),
    true = rpc:call(Key1Node, erlang, exit, [ProxyPid, testkill]),

    lager:info("Wait for stable ensembles"),
    ensemble_util:wait_until_stable(Node, NVal),
    lager:info("Re-reading keys"),
    [rt:pbc_read(PBC, Bucket, Key) || Key <- Keys],

    pass.
