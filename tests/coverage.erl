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

confirm() ->
    inets:start(),

    Nodes = rt:build_cluster(5),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

    Node1 = lists:nth(1, Nodes),
    Node2 = lists:nth(2, Nodes),
    Node4 = lists:nth(4, Nodes),

    Pb1 = rt:pbc(Node1),
    Pb2 = rt:pbc(Node2),
    Pb4 = rt:pbc(Node4),

    {ok, PartitionChunks} = riakc_pb_socket:get_coverage(Pb1, ?BUCKET, 0),

    %% Identify chunks attached to dev1
    ReplaceMe = find_matches(PartitionChunks, 10017),
    %% Stop dev1
    rt:stop_and_wait(Node1),

    %% Ask dev2 for replacements
    NoNode1 = replace_subpartition_chunks(ReplaceMe, Pb2),
    %% Make sure none of the replacements are from dev1
    ?assertEqual(0, length(find_matches(NoNode1, 10017))),
    ?assertEqual(length(ReplaceMe), length(NoNode1)),

    SampleNode5 = hd(find_matches(PartitionChunks, 10057)),

    %% Ask dev4 to replace dev1 and dev5.
    NoNode1_5 = replace_subpartition_chunks(ReplaceMe, [SampleNode5], Pb4),

    ?assertEqual(0, length(find_matches(NoNode1_5, 10017))),
    ?assertEqual(0, length(find_matches(NoNode1_5, 10057))),
    ?assertEqual(length(ReplaceMe), length(NoNode1_5)),

    pass.

find_matches(Coverage, Port) ->
    lists:filtermap(fun(#rpbcoverageentry{port=P, cover_context=C}) when P == Port -> {true, C};
                    (_) -> false end,
                 Coverage).

replace_subpartition_chunks(Replace, PbPid) ->
    replace_subpartition_chunks(Replace, [], PbPid).

replace_subpartition_chunks(Replace, Extra, PbPid) ->
    lists:map(fun(R) ->
                      {ok, NewChunk} =
                          riakc_pb_socket:replace_coverage(PbPid, ?BUCKET, R, Extra),
                      NewChunk
              end, Replace).
