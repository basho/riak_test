%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(verify_listkeys).

%% -behavior(riak_test).

-export([properties/0,
         confirm/1]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"listkeys_bucket">>).
-define(NUM_BUCKETS, 1200).
-define(NUM_KEYS, 1000).
-define(UNDEFINED_BUCKET,  <<"880bf69d-5dab-44ee-8762-d24c6f759ce1">>).
-define(UNDEFINED_BUCKET_TYPE,  <<"880bf69d-5dab-44ee-8762-d24c6f759ce1">>).

properties() ->
    rt_properties:new([{node_count, 4},
                       {make_cluster, false}]).

-spec confirm(rt_properties:properties()) -> pass | fail.
confirm(Properties) ->
    [NodeId1, NodeId2, NodeId3 | _] = NodeIds = rt_properties:get(node_ids, Properties),
    NodeMap = rt_properties:get(node_map, Properties),
    Nodes = [rt_node:node_name(NodeId, NodeMap) || NodeId <- NodeIds],

    [Node1, Node2, Node3 | _] = Nodes,

    lager:info("Writing some known data to Node 1"),
    put_keys(Node1, ?BUCKET, ?NUM_KEYS),
    put_buckets(Node1, ?NUM_BUCKETS),
    timer:sleep(2000),
    check_it_all([Node1]),

    lists:foldl(fun growing_cluster_check/2, [Node1], tl(Nodes)),

    lager:info("Checking basic HTTP"),
    check_it_all(Nodes, http),

    Version = rt_properties:get(current_version, Properties),

    lager:info("Stopping Node1"),
    rt_node:stop(NodeId1, Version),

    rt:wait_until_unpingable(Node1),

    %% Stop current node, restart previous node, verify
    lists:foldl(fun shrinking_cluster_check/2,
                {{hd(NodeIds), Node1}, Nodes, Version},
                lists:zip(tl(NodeIds), tl(Nodes))),

    lager:info("Stopping Node2"),
    rt_node:stop(NodeId2, Version),
    rt:wait_until_unpingable(Node2),

    lager:info("Stopping Node3"),
    rt_node:stop(NodeId3, Version),
    rt:wait_until_unpingable(Node3),

    lager:info("Only Node1 is up, so test should fail!"),

    check_it_all([Node1], pbc, false),
    pass.

growing_cluster_check(JoiningNode, [ClusterNode|_] = Cluster) ->
    lager:info("An invitation to this party is cordially extended to ~p.", [JoiningNode]),
    rt_node:join(JoiningNode, ClusterNode),
    lager:info("Wait until there are no pending changes"),
    Ns = lists:usort([JoiningNode|Cluster]),
    rt:wait_until_no_pending_changes(Ns),
    rt:wait_for_cluster_service(Ns, riak_kv),
    ok = rt:wait_until_transfers_complete(Ns),
    lager:info("Check keys and buckets after transfer"),
    check_it_all(Ns),
    Ns.

shrinking_cluster_check({NodeId, Node}, {{PrevId, PrevNode}, Nodes, Version}) ->
    lager:info("Stopping Node ~p", [Node]),
    rt_node:stop(NodeId, Version),
    rt:wait_until_unpingable(Node),

    lager:info("Starting Node ~p", [PrevId]),
    rt_node:start(PrevId, Version),
    UpNodes = Nodes -- [Node],
    lager:info("Waiting for riak_kv service to be ready in ~p", [PrevNode]),
    rt:wait_for_cluster_service(UpNodes, riak_kv),

    lager:info("Check keys and buckets"),
    check_it_all(UpNodes),
    {{NodeId, Node}, Nodes, Version}.

put_keys(Node, Bucket, Num) ->
    Pid = rt_pb:pbc(Node),
    Keys = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
    Vals = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
    [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val)) || {Key, Val} <- lists:zip(Keys, Vals)],
    riakc_pb_socket:stop(Pid).

list_keys(Node, Interface, Bucket, Attempt, Num, ShouldPass) ->
    case Interface of
        pbc ->
            Pid = rt_pb:pbc(Node),
            Mod = riakc_pb_socket;
        http ->
            Pid = rt_http:httpc(Node),
            Mod = rhc
    end,
    lager:info("Listing keys on ~p using ~p. Attempt #~p",
               [Node, Interface, Attempt]),
    case ShouldPass of
        true ->
            {ok, Keys} = Mod:list_keys(Pid, Bucket),
            ActualKeys = lists:usort(Keys),
            ExpectedKeys = lists:usort([list_to_binary(["", integer_to_list(Ki)])
                                        || Ki <- lists:seq(0, Num - 1)]),
            assert_equal(ExpectedKeys, ActualKeys);
        _ ->
            {Status, Message} = Mod:list_keys(Pid, Bucket),
            ?assertEqual(error, Status),
            ?assertEqual(<<"insufficient_vnodes_available">>, Message)
    end,
    case Interface of
        pbc -> riakc_pb_socket:stop(Pid);
        _ -> ok
    end.

list_keys_for_undefined_bucket_type(Node, Interface, Bucket, Attempt, ShouldPass) ->
    case Interface of
        pbc ->
            Pid = rt_pb:pbc(Node),
            Mod = riakc_pb_socket;
        http ->
            Pid = rt_http:httpc(Node),
            Mod = rhc
    end,

    lager:info("Listing keys using undefined bucket type ~p on ~p using ~p. Attempt #~p",
               [?UNDEFINED_BUCKET_TYPE, Node, Interface, Attempt]),
    case ShouldPass of
        true -> ok;
        _ ->
            {Status, Message} = Mod:list_keys(Pid, { ?UNDEFINED_BUCKET_TYPE, Bucket }),
            ?assertEqual(error, Status),
            ?assertEqual(<<"No bucket-type named '880bf69d-5dab-44ee-8762-d24c6f759ce1'">>, Message)
    end,

    case Interface of
        pbc -> riakc_pb_socket:stop(Pid);
        _ -> ok
    end.

put_buckets(Node, Num) ->
    Pid = rt_pb:pbc(Node),
    Buckets = [list_to_binary(["", integer_to_list(Ki)])
               || Ki <- lists:seq(0, Num - 1)],
    {Key, Val} = {<<"test_key">>, <<"test_value">>},
    [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val))
     || Bucket <- Buckets],
    riakc_pb_socket:stop(Pid).

list_buckets(Node, Interface, Attempt, Num, ShouldPass) ->
    case Interface of
        pbc ->
            Pid = rt_pb:pbc(Node),
            Mod = riakc_pb_socket;
        http ->
            Pid = rt_http:httpc(Node),
            Mod = rhc
    end,
    lager:info("Listing buckets on ~p using ~p. Attempt #~p",
               [Node, Interface, Attempt]),

    {Status, Buckets} = Mod:list_buckets(Pid),
    case Status of
        error -> lager:info("list buckets error ~p", [Buckets]);
        _ -> ok
    end,
    ?assertEqual(ok, Status),
    ExpectedBuckets= lists:usort([?BUCKET |
                                  [list_to_binary(["", integer_to_list(Ki)])
                                   || Ki <- lists:seq(0, Num - 1)]]),
    ActualBuckets = lists:usort(Buckets),
    case ShouldPass of
        true ->
            assert_equal(ExpectedBuckets, ActualBuckets);
        _ ->
            ?assert(length(ActualBuckets) < length(ExpectedBuckets)),
            lager:info("This case expects inconsistent bucket lists")
    end,
    case Interface of
        pbc -> riakc_pb_socket:stop(Pid);
        _ -> ok
    end.

list_buckets_for_undefined_bucket_type(Node, Interface, Attempt, ShouldPass) ->
    case Interface of
        pbc ->
            Pid = rt_pb:pbc(Node),
            Mod = riakc_pb_socket;
        http ->
            Pid = rt_http:httpc(Node),
            Mod = rhc
    end,

    lager:info("Listing buckets on ~p for undefined bucket type ~p using ~p.  Attempt ~p.",
               [Node, ?UNDEFINED_BUCKET_TYPE, Interface, Attempt]),

    case ShouldPass of
        true -> ok;
        _ ->
            {Status, Message} = Mod:list_buckets(Pid, ?UNDEFINED_BUCKET_TYPE, []),
            lager:info("Received status ~p and message ~p", [Status, Message]),
            ?assertEqual(error, Status),
            ?assertEqual(<<"No bucket-type named '880bf69d-5dab-44ee-8762-d24c6f759ce1'">>, Message)
    end,

    case Interface of
        pbc ->
            riakc_pb_socket:stop(Pid);
        _ -> ok
    end.

assert_equal(Expected, Actual) ->
    case Expected -- Actual of
        [] -> ok;
        Diff -> lager:info("Expected -- Actual: ~p", [Diff])
    end,
    ?assertEqual(length(Actual), length(Expected)),
    ?assertEqual(Actual, Expected).

check_it_all(Nodes) ->
    check_it_all(Nodes, pbc).

check_it_all(Nodes, Interface) ->
    check_it_all(Nodes, Interface, true).

check_it_all(Nodes, Interface, ShouldPass) ->
    [check_a_node(N, Interface, ShouldPass) || N <- Nodes].

check_a_node(Node, Interface, ShouldPass) ->
    [list_keys(Node, Interface, ?BUCKET, Attempt, ?NUM_KEYS, ShouldPass)
     || Attempt <- [1,2,3] ],
    [list_keys_for_undefined_bucket_type(Node, Interface, ?BUCKET, Attempt, ShouldPass)
     || Attempt <- [1,2,3] ],
    [list_buckets(Node, Interface, Attempt, ?NUM_BUCKETS, ShouldPass)
     || Attempt <- [1,2,3] ],
    [list_buckets_for_undefined_bucket_type(Node, Interface, Attempt, ShouldPass)
     || Attempt <- [1,2,3] ].
