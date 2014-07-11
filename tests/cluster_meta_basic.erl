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
-export([confirm/0, object_count/2]).
-include_lib("eunit/include/eunit.hrl").

-define(PREFIX1, {a, b}).
-define(PREFIX2, {fold, prefix}).
-define(KEY1, key1).
-define(KEY2, key2).
-define(VAL1, val1).
-define(VAL2, val2).

confirm() ->
    Nodes = rt:build_cluster(5),
    ok = test_fold_full_prefix(Nodes),
    ok = test_metadata_conflicts(Nodes),
    ok = test_writes_after_partial_cluster_failure(Nodes),
    pass.

%% 1. write a key and waits til it propogates around the cluster
%% 2. stop the immediate eager peers of the node that performed the write
%% 3. perform an update of the key from the same node and wait until it reaches all alive nodes
%% 4. bring up stopped nodes and ensure that either lazily queued messages or anti-entropy repair
%%    propogates key to all nodes in cluster
test_writes_after_partial_cluster_failure([N1 | _]=Nodes) ->
    lager:info("testing writes after partial cluster failure"),
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

%% 1. write several keys to a prefix, fold over them accumulating a list
%% 2. ensure list of keys and values match those written to prefix
test_fold_full_prefix([N1 | _]=Nodes) ->
    rt:load_modules_on_nodes([?MODULE], Nodes),
    lager:info("testing prefix (~p) fold on ~p", [?PREFIX2, N1]),
    KeysAndVals = [{I, I} || I <- lists:seq(1, 10)],
    [metadata_put(N1, ?PREFIX2, K, V) || {K, V} <- KeysAndVals],
    %% we don't use a resolver but shouldn't have conflicts either, so assume that in
    %% head of fold function
    FoldRes = [{K, V} || {K, [V]} <- metadata_to_list(N1, ?PREFIX2)],
    SortedRes = lists:ukeysort(1, FoldRes),
    ?assertEqual(KeysAndVals, SortedRes),
    ok.

test_metadata_conflicts([N1, N2 | _]=Nodes) ->
    rt:load_modules_on_nodes([?MODULE], Nodes),
    lager:info("testing conflicting writes to a key"),
    write_conflicting(N1, N2, ?PREFIX1, ?KEY2, ?VAL1, ?VAL2),

    %% assert that we still have siblings since write_conflicting uses allow_put=false
    lager:info("checking object count after resolve on get w/o put"),
    ?assertEqual(2, rpc:call(N1, ?MODULE, object_count, [?PREFIX1, ?KEY2])),
    ?assertEqual(2, rpc:call(N2, ?MODULE, object_count, [?PREFIX1, ?KEY2])),

    %% iterate over the values and ensure we can resolve w/o doing a put
    ?assertEqual([{?KEY2, lists:usort([?VAL1, ?VAL2])}],
                 metadata_to_list(N1, ?PREFIX1, [{allow_put, false}])),
    ?assertEqual([{?KEY2, lists:usort([?VAL1, ?VAL2])}],
                 metadata_to_list(N2, ?PREFIX1, [{allow_put, false}])),
    lager:info("checking object count after resolve on itr_key_values w/o put"),
    ?assertEqual(2, rpc:call(N1, ?MODULE, object_count, [?PREFIX1, ?KEY2])),
    ?assertEqual(2, rpc:call(N2, ?MODULE, object_count, [?PREFIX1, ?KEY2])),

    %% assert that we no longer have siblings when allow_put=true
    lager:info("checking object count afger resolve on get w/ put"),
    wait_until_metadata_value(N1, ?PREFIX1, ?KEY2,
                              [{resolver, fun list_resolver/2}],
                              lists:usort([?VAL1, ?VAL2])),
    wait_until_metadata_value([N1, N2], ?PREFIX1, ?KEY2,
                              [{resolver, fun list_resolver/2}, {allow_put, false}],
                              lists:usort([?VAL1, ?VAL2])),
    wait_until_object_count([N1, N2], ?PREFIX1, ?KEY2, 1),
    ok.

write_conflicting(N1, N2, Prefix, Key, Val1, Val2) ->
    rpc:call(N1, riak_core_metadata_manager, put, [{Prefix, Key}, undefined, Val1]),
    rpc:call(N2, riak_core_metadata_manager, put, [{Prefix, Key}, undefined, Val2]),
    wait_until_metadata_value([N1, N2], Prefix, Key,
                              [{resolver, fun list_resolver/2},
                               {allow_put, false}],
                              lists:usort([Val1, Val2])).


object_count(Prefix, Key) ->
    Obj = riak_core_metadata_manager:get({Prefix, Key}),
    case Obj of
        undefined -> 0;
        _ -> riak_core_metadata_object:value_count(Obj)
    end.


list_resolver(X1, X2) when is_list(X2) andalso is_list(X1) ->
    lists:usort(X1 ++ X2);
list_resolver(X1, X2) when is_list(X2) ->
    lists:usort([X1 | X2]);
list_resolver(X1, X2) when is_list(X1) ->
    lists:usort(X1 ++ [X2]);
list_resolver(X1, X2) ->
    lists:usort([X1, X2]).

metadata_to_list(Node, FullPrefix) ->
    metadata_to_list(Node, FullPrefix, []).

metadata_to_list(Node, FullPrefix, Opts) ->
    rpc:call(Node, riak_core_metadata, to_list, [FullPrefix, Opts]).

metadata_put(Node, Prefix, Key, FunOrVal) ->
    ok = rpc:call(Node, riak_core_metadata, put, [Prefix, Key, FunOrVal]).

metadata_get(Node, Prefix, Key, Opts) ->
    rpc:call(Node, riak_core_metadata, get, [Prefix, Key, Opts]).

wait_until_metadata_value(Nodes, Prefix, Key, Val) ->
    wait_until_metadata_value(Nodes, Prefix, Key, [], Val).

wait_until_metadata_value(Nodes, Prefix, Key, Opts, Val) when is_list(Nodes) ->
    [wait_until_metadata_value(Node, Prefix, Key, Opts, Val) || Node <- Nodes];
wait_until_metadata_value(Node, Prefix, Key, Opts, Val) ->
    lager:info("wait until {~p, ~p} equals ~p on ~p", [Prefix, Key, Val, Node]),
    F = fun() ->
                Val =:= metadata_get(Node, Prefix, Key, Opts)
        end,
    ?assertEqual(ok, rt:wait_until(F)),
    ok.

wait_until_object_count(Nodes, Prefix, Key, Count) when is_list(Nodes) ->
    [wait_until_object_count(Node, Prefix, Key, Count) || Node <- Nodes];
wait_until_object_count(Node, Prefix, Key, Count) ->
    lager:info("wait until {~p, ~p} has object count ~p on ~p", [Prefix, Key, Count, Node]),
    F = fun() ->
                Count =:= rpc:call(Node, ?MODULE, object_count, [Prefix, Key])
        end,
    ?assertEqual(ok, rt:wait_until(F)),
    ok.


eager_peers(Node, Root) ->
    {Eagers, _} = rpc:call(Node, riak_core_broadcast, debug_get_peers, [Node, Root]),
    Eagers.

print_tree(Root, Nodes) ->
    Tree = rpc:call(Root, riak_core_broadcast, debug_get_tree, [Root, Nodes]),
    lager:info("broadcast tree: ~p", [Tree]).
