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
-module(coverage).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-define(BUCKET, <<"coverbucket">>).


%% Things to test:
%%   2i works with externally-provided coverage plans
%%   Replace chunks in traditional plan
%%   Error handling when primary partitions aren't available


confirm() ->
    inets:start(),

    Nodes = rt:build_cluster(5),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

    RingSize = ring_size(hd(Nodes)),
    ObservedRingSize = test_subpartitions(Nodes, 0),
    ?assertEqual(RingSize, ObservedRingSize),

    StupidlyGranularTest = test_subpartitions(Nodes, 64000),
    ?assertEqual(1 bsl 16, StupidlyGranularTest),

    %% Test other NVals
    test_traditional(4, Nodes, RingSize),
    test_traditional(5, Nodes, RingSize),
    pass.

create_nval_bucket_type(Node, Nodes, NVal, Type) ->
    %% n_val setup shamelessly stolen from bucket_types.erl
    TypeProps = [{n_val, NVal}],
    lager:info("Create bucket type ~p, wait for propagation", [Type]),
    rt:create_and_activate_bucket_type(Node, Type, TypeProps),
    rt:wait_until_bucket_type_status(Type, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {Type, <<"bucket">>}, TypeProps).


%%
%% Create a traditional coverage plan and tally the components to
%% compare against ring size
test_traditional(NVal, Nodes, RingSize) ->
    Node1 = lists:nth(1, Nodes),
    Pb1 = rt:pbc(Node1),
    %% create type with nval NVal
    TypeName = unicode:characters_to_binary(lists:flatten(io_lib:format("~s~B", ["N", NVal]))),
    create_nval_bucket_type(Node1, Nodes, NVal, TypeName),
    {ok, TradChunks} = riakc_pb_socket:get_coverage(Pb1, {TypeName, ?BUCKET}),

    CountedRingSize = count_traditional(NVal, TradChunks),
    ?assertEqual(RingSize, CountedRingSize),
    ok.

count_traditional(NVal, Coverage) ->
    lists:foldl(
      fun(#rpbcoverageentry{cover_context=C}, Tally) ->
              {ok, Details} = riak_kv_pb_coverage:checksum_binary_to_term(C),
              partition_count_from_filters(NVal,
                                           proplists:get_value(filters, Details, []))
                  + Tally
      end,
      0, Coverage).

%% In a traditional coverage plan, no filters means use n_val
%% partitions from this vnode, while a non-empty list of filters means
%% use only those partitions
partition_count_from_filters(NVal, []) ->
    NVal;
partition_count_from_filters(_NVal, Filters) ->
    length(Filters).

%%
%% Create a
test_subpartitions(Nodes, Granularity) ->
    Node1 = lists:nth(1, Nodes),
    Node2 = lists:nth(2, Nodes),
    Node4 = lists:nth(4, Nodes),
    Node5 = lists:nth(5, Nodes),

    Pb1 = rt:pbc(Node1),
    Pb2 = rt:pbc(Node2),
    Pb4 = rt:pbc(Node4),

    {ok, PartitionChunks} =
        riakc_pb_socket:get_coverage(Pb1, ?BUCKET, Granularity),

    %% Identify chunks attached to dev1
    ReplaceMe = find_matches(PartitionChunks, Node1),
    lager:info("Found ~B subpartitions assigned to dev1", [length(ReplaceMe)]),
    %% Stop dev1
    rt:stop_and_wait(Node1),

    %% Ask dev2 for replacements
    NoNode1 = replace_subpartition_chunks(ReplaceMe, Pb2),
    %% Make sure none of the replacements are from dev1
    ?assertEqual(0, length(find_matches(NoNode1, Node1))),
    ?assertEqual(length(ReplaceMe), length(NoNode1)),

    %% Extract a cover context for node 5
    SampleNode5 = hd(find_matches(PartitionChunks, Node5)),

    %% Ask dev4 to replace dev1 and dev5.
    NoNode1_5 = replace_subpartition_chunks(ReplaceMe, [SampleNode5], Pb4),

    ?assertEqual(0, length(find_matches(NoNode1_5, Node1))),
    ?assertEqual(0, length(find_matches(NoNode1_5, Node5))),
    ?assertEqual(length(ReplaceMe), length(NoNode1_5)),

    rt:start_and_wait(Node1),

    %% Caller wants to know size of results
    length(PartitionChunks).

find_matches(Coverage, Node) ->
    lists:filtermap(fun(#rpbcoverageentry{cover_context=C}) ->
                            {ok, Plist} = riak_kv_pb_coverage:checksum_binary_to_term(C),
                            case proplists:get_value(node, Plist) == Node of
                                true ->
                                    {true, C};
                                false ->
                                    false
                            end;
                       (C) ->
                            {ok, Plist} = riak_kv_pb_coverage:checksum_binary_to_term(C),
                            case proplists:get_value(node, Plist) == Node of
                                true ->
                                    {true, C};
                                false ->
                                    false
                            end
                    end, Coverage).

replace_subpartition_chunks(Replace, PbPid) ->
    replace_subpartition_chunks(Replace, [], PbPid).

replace_subpartition_chunks(Replace, Extra, PbPid) ->
    lists:map(fun(R) ->
                      {ok, [NewChunk]} =
                          riakc_pb_socket:replace_coverage(PbPid, ?BUCKET, R, Extra),
                      NewChunk
              end, Replace).

ring_size(Node) ->
    {ok, R} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    riak_core_ring:num_partitions(R).
