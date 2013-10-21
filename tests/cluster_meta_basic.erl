%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
-module(cluster_meta_basic).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(PREFIX1, {a, b}).
-define(KEY1, key1).
-define(VAL1, val1).
-define(VAL2, val2).

confirm() ->
    [N1 | _] = Nodes = rt:build_cluster(5),

    metadata_put(N1, ?PREFIX1, ?KEY1, ?VAL1),
    wait_until_metadata_value(Nodes, ?PREFIX1, ?KEY1, ?VAL1),
    print_tree(N1, Nodes),

    StopNodes = eager_peers(N1, N1),
    AliveNodes = Nodes -- StopNodes,
    lager:info("stopping nodes: ~p remaining nodes: ~p", [StopNodes, AliveNodes]),
    [rt:stop(N) || N <- StopNodes],

    metadata_put(N1, ?PREFIX1, ?KEY1, ?VAL2),
    wait_until_metadata_value(AliveNodes, ?PREFIX1, ?KEY1, ?VAL2),

    lager:info("bring stopped nodes back up: ~p", [StopNodes]),
    [rt:start(N) || N <- StopNodes],
    wait_until_metadata_value(Nodes, ?PREFIX1, ?KEY1, ?VAL2),
    ok.

metadata_put(Node, Prefix, Key, FunOrVal) ->
    ok = rpc:call(Node, riak_core_metadata, put, [Prefix, Key, FunOrVal]).

metadata_get(Node, Prefix, Key) ->
    rpc:call(Node, riak_core_metadata, get, [Prefix, Key]).

wait_until_metadata_value(Nodes, Prefix, Key, Val) when is_list(Nodes) ->
    [wait_until_metadata_value(Node, Prefix, Key, Val) || Node <- Nodes];
wait_until_metadata_value(Node, Prefix, Key, Val) ->
    lager:info("wait until {~p, ~p} equals ~p on ~p", [Prefix, Key, Val, Node]),
    F = fun() ->
                %% TODO: need to not resolve or else we can have ambiguities
                Val =:= metadata_get(Node, Prefix, Key)
        end,
    ?assertEqual(ok, rt:wait_until(F)),
    ok.

eager_peers(Node, Root) ->
    {Eagers, _} = rpc:call(Node, riak_core_broadcast, debug_get_peers, [Node, Root]),
    Eagers.

print_tree(Root, Nodes) ->
    Tree = rpc:call(Root, riak_core_broadcast, debug_get_tree, [Root, Nodes]),
    lager:info("broadcast tree: ~p", [Tree]).
