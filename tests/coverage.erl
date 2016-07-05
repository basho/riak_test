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
-define(BUCKET2i, <<"2ibucket">>).
-import(secondary_index_tests, [put_an_object/2, stream_pb/3]).


%% Things to test:
%%   2i works with externally-provided coverage plans
%%   Replace chunks in traditional plan


confirm() ->
    inets:start(),

    rt:set_backend(eleveldb),
    Nodes = rt:build_cluster(5),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

    RingSize = ring_size(hd(Nodes)),

    %% Generate a subpartition plan where each subpartition is equal
    %% to one partition. Run some replacement tests.
    ObservedRingSize = test_subpartitions(Nodes, 0),
    ?assertEqual(RingSize, ObservedRingSize),

    %% Run the same set of tests against a very fine-grained
    %% subpartition plan of 65536 chunks
    StupidlyGranularTest = test_subpartitions(Nodes, 64000),
    ?assertEqual(1 bsl 16, StupidlyGranularTest),

    %% Run coverage plan size tests against less-common NVals with a
    %% traditional coverage plan
    test_traditional(4, Nodes, RingSize),
    test_traditional(5, Nodes, RingSize),

    %% Make sure we get replacement errors when primary partitions
    %% aren't available
    test_failure(Nodes, RingSize),

    %% Make sure a coverage plan is generated from scratch without any
    %% downed nodes
    test_down(Nodes),

    %% All of this is meaningless if we can't actually issue queries
    %% leveraging the coverage plans. Do some 2i, please
    KeyCount = populate_data(Nodes),
    test_2i(KeyCount, Nodes),
    test_2i(KeyCount, Nodes, lists:nth(3, Nodes)),

    pass.

populate_data(Nodes) ->
    Pb1 = rt:pbc(hd(Nodes)),
    NumKeys = length([put_an_object(Pb1, N) || N <- lists:seq(0, 99)]),
    connstop(Pb1),
    NumKeys.


test_2i(KeyCount, Nodes) ->
    Pb1 = rt:pbc(hd(Nodes)),

    %% This won't be the same bucket name as the one in
    %% `secondary_index_tests' but all that matters for coverage
    %% generation is whether they evaluate to the same n_val
    {ok, TradCoverage} =
        riakc_pb_socket:get_coverage(Pb1, ?BUCKET),
    {ok, SubpCoverage} =
        riakc_pb_socket:get_coverage(Pb1, ?BUCKET, 300),
    connstop(Pb1),
    count_2i(KeyCount, TradCoverage, trad_all_up),

    count_2i(KeyCount, SubpCoverage, sub_all_up).

%% Tests whether we get correct results when we ask for replacement
%% coverage to exclude a node. Originally intended to drop the node,
%% but that introduces too much uncertainty (handoffs take too long,
%% and we get too few results if we don't wait)
test_2i(KeyCount, Nodes, DownNode) ->
    Pb1 = rt:pbc(hd(Nodes)),
    {ok, TradCoverage} =
        riakc_pb_socket:get_coverage(Pb1, ?BUCKET),
    {ok, SubpCoverage} =
        riakc_pb_socket:get_coverage(Pb1, ?BUCKET, 300),

    SubpCoverage2 = swap_chunks(SubpCoverage, DownNode, Pb1),
    TradCoverage2 = swap_chunks(TradCoverage, DownNode, Pb1),
    connstop(Pb1),

    count_2i(KeyCount, SubpCoverage2, sub_one_down),
    count_2i(KeyCount, TradCoverage2, trad_one_down).

map_pids(CoverageList) ->
    Pids =
        lists:foldl(fun(#rpbcoverageentry{ip=IP, port=Port}, Dict) ->
                            {ok, Pid} = riakc_pb_socket:start(binary_to_list(IP), Port),
                            dict:store({IP, Port}, Pid, Dict)
                    end,
                    dict:new(),
                    CoverageList),

    {Pids,
     lists:map(fun(#rpbcoverageentry{ip=IP, port=Port, cover_context=C}) ->
                       {dict:fetch({IP, Port}, Pids), Port, C}
               end, CoverageList)
    }.

count_2i(KeyCount, Coverage, Label) ->
    {Pids, PbCoverage} = map_pids(Coverage),

    Keys = lists:flatmap(fun({Pid, _Port, Cover}) ->
                                 {ok, PBRes} = stream_pb(Pid, {<<"$bucket">>, ?BUCKET2i}, [{cover_context, Cover}]),
                                 proplists:get_value(keys, PBRes, [])
                         end, PbCoverage),
    ?assert(all_keys_present(KeyCount, Keys, Label)),
    dict:fold(fun(_, Pid, _Acc) -> connstop(Pid) end,
              0,
              Pids).

all_keys_present(Count, Keys, WhichTest) ->
    KeyPresent =
        lists:map(fun(C) -> Key = secondary_index_tests:int_to_key(C),
                            case lists:member(Key, Keys) of
                                true ->
                                    true;
                                false ->
                                    lager:error("Missing ~s from ~s", [Key, WhichTest]),
                                    false
                            end
                  end, lists:seq(0, Count-1)),
    not lists:member(false, KeyPresent).

connstop(Pid) ->
    riakc_pb_socket:stop(Pid).

maybe_swap_chunk(#rpbcoverageentry{cover_context=C}=Entry, DownNode, Pb) ->
    {ok, Details} = riak_kv_pb_coverage:checksum_binary_to_term(C),
    maybe_swap_chunk(Entry, C,
                     proplists:get_value(node, Details),
                     DownNode, Pb).

maybe_swap_chunk(_Entry, Cover, DownNode, DownNode, Pb) ->
    {ok, NewEntries} =
        riakc_pb_socket:replace_coverage(Pb, ?BUCKET, Cover),
    NewEntries;
maybe_swap_chunk(Entry, _Cover, _EntryNode, _DownNode, _Pb) ->
    [Entry].


swap_chunks(Coverage, Node, Pb) ->
    %% Subpartition replacement will be a list of a single entry,
    %% while traditional replacement will often be a list of multiple
    %% entries. Use flatmap to handle both cases
    lists:flatmap(fun(E) -> maybe_swap_chunk(E, Node, Pb) end,
                  Coverage).


test_down(Nodes) ->
    Node2 = lists:nth(2, Nodes),
    Node4 = lists:nth(4, Nodes),
    rt:stop_and_wait(Node2),
    Pb4 = rt:pbc(Node4),

    {ok, TradChunks} = riakc_pb_socket:get_coverage(Pb4, ?BUCKET),
    ?assertEqual(0, length(find_matches(TradChunks, Node2))),
    {ok, SubPartChunks} = riakc_pb_socket:get_coverage(Pb4, ?BUCKET, 1000),
    ?assertEqual(0, length(find_matches(SubPartChunks, Node2))),

    rt:start_and_wait(Node2),
    connstop(Pb4).


create_nval_bucket_type(Node, Nodes, NVal, Type) ->
    %% n_val setup shamelessly stolen from bucket_types.erl
    TypeProps = [{n_val, NVal}],

    rt:create_and_activate_bucket_type(Node, Type, TypeProps),
    rt:wait_until_bucket_type_status(Type, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {Type, <<"bucket">>}, TypeProps).

test_failure(Nodes, RingSize) ->
    %% Setting up a bucket type with n_val 1 should be sufficient to
    %% generate primary partition errors when asking for a
    %% replacement. Test with the relevant node both down and up
    Type = <<"tcob1">>,
    Node1 = lists:nth(1, Nodes),
    Node3 = lists:nth(3, Nodes),
    Pb1 = rt:pbc(Node1),
    create_nval_bucket_type(Node1, Nodes, 1, Type),
    {ok, TradChunks} = riakc_pb_socket:get_coverage(Pb1, {Type, ?BUCKET}),
    %% With n=1, should be one chunk per vnode
    ?assertEqual(RingSize, length(TradChunks)),
    rt:stop_and_wait(Node3),
    Dev3Chunks = find_matches(TradChunks, Node3),
    lists:foreach(
      fun(C) ->
              ?assertMatch({error, _},
                           riakc_pb_socket:replace_coverage(Pb1, {Type, ?BUCKET}, C))
      end, Dev3Chunks),
    rt:start_and_wait(Node3),
    lists:foreach(
      fun(C) ->
              ?assertMatch({error, _},
                           riakc_pb_socket:replace_coverage(Pb1, {Type, ?BUCKET}, C))
      end, Dev3Chunks),
    connstop(Pb1).





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
    connstop(Pb1),
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

    %% Stop dev1
    connstop(Pb1),
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

    connstop(Pb2),
    connstop(Pb4),

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
