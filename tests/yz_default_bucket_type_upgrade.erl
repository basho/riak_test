%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%--------------------------------------------------------------------
-module(yz_default_bucket_type_upgrade).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(YZ_CAP, {yokozuna, handle_legacy_default_bucket_type_aae}).
-define(INDEX, <<"test_upgrade_idx">>).
-define(BUCKET, <<"test_upgrade_bucket">>).
-define(SEQMAX, 2000).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16},
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 8}
          ]},
         {yokozuna,
          [
           {anti_entropy_tick, 1000},
           {enabled, true}
          ]}
        ]).

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    [_, Node|_] = Cluster = rt:build_cluster(lists:duplicate(4, {OldVsn, ?CFG})),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    %% Use when we can specify a specific version to upgrade from or skip test
    %% accordingly.
    %% rt:assert_capability(Node, ?YZ_CAP, {unknown_capability, ?YZ_CAP}),

    %% Generate keys, YZ only supports UTF-8 compatible keys
    GenKeys = [<<N:64/integer>> || N <- lists:seq(1, ?SEQMAX),
                                  not lists:any(
                                        fun(E) -> E > 127 end,
                                        binary_to_list(<<N:64/integer>>))],
    KeyCount = length(GenKeys),
    lager:info("KeyCount ~p", [KeyCount]),

    OldPid = rt:pbc(Node),

    yokozuna_rt:write_data(OldPid, ?INDEX, ?BUCKET, GenKeys),
    %% wait for solr soft commit
    timer:sleep(1100),

    assert_num_found_query(OldPid, ?INDEX, KeyCount),

    %% Upgrade
    yokozuna_rt:rolling_upgrade(Cluster, current),

    CurrentCapabilities = rt:capability(Node, all),
    rt:assert_capability(Node, ?YZ_CAP, v1),
    rt:assert_supported(CurrentCapabilities, ?YZ_CAP, [v1, v0]),

    yokozuna_rt:wait_for_aae(Cluster),

    %% test query count again
    Pid = rt:pbc(Node),
    assert_num_found_query(Pid, ?INDEX, KeyCount),

    yokozuna_rt:expire_trees(Cluster),
    yokozuna_rt:wait_for_aae(Cluster),

    assert_num_found_query(Pid, ?INDEX, KeyCount),

    pass.

assert_num_found_query(Pid, Index, ExpectedCount) ->
    {ok, {_, _, _, NumFound}} = riakc_pb_socket:search(Pid, Index, <<"*:*">>),
    lager:info("Check Count, Expected: ~p | Actual: ~p~n",
               [ExpectedCount, NumFound]),
    ?assertEqual(ExpectedCount, NumFound).
