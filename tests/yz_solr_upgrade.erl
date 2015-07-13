%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
-module(yz_solr_upgrade).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(INDEX, <<"test_solr_upgrade_idx">>).
-define(BUCKET, <<"test_solr_upgrade_bucket">>).
-define(SEQMAX, 1000).
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

    [Node1, Node2|RestNodes] = Cluster = rt:build_cluster(lists:duplicate(4, {OldVsn, ?CFG})),

    %% Generate keys, YZ only supports UTF-8 compatible keys
    GenKeys = [<<N:64/integer>> || N <- lists:seq(1, ?SEQMAX),
                                  not lists:any(
                                        fun(E) -> E > 127 end,
                                        binary_to_list(<<N:64/integer>>))],
    KeyCount = length(GenKeys),
    lager:info("KeyCount ~p", [KeyCount]),

    OldPid = rt:pbc(rt:select_random(RestNodes)),

    yokozuna_rt:write_data(Cluster, OldPid, ?INDEX, ?BUCKET, GenKeys),
    %% wait for solr soft commit
    timer:sleep(1100),

    riakc_pb_socket:stop(OldPid),

    HP1 = random_hp([Node1, Node2]),
    yokozuna_rt:search_expect(HP1, ?INDEX, <<"*">>, <<"*">>, KeyCount),

    %% Upgrade Node 1 and 2
    lager:info("Upgrade to solr version 4.10.4 on Nodes 1 - 2"),
    upgrade_to_current([Node1, Node2]),

    lager:info("Write one more piece of data"),
    Pid2 = rt:pbc(Node2),
    ok = rt:pbc_write(Pid2, ?BUCKET, <<"foo">>, <<"foo">>, "text/plain"),
    timer:sleep(1100),
    riakc_pb_socket:stop(Pid2),

    HP2 = random_hp(RestNodes),
    yokozuna_rt:search_expect(HP2, ?INDEX, <<"*">>, <<"*">>, KeyCount + 1),

    %% Upgrade Rest
    lager:info("Upgrade to solr version 4.10.4 on Nodes 3 - 4"),
    upgrade_to_current(RestNodes),

    lager:info("Write one more piece of data"),
    RandPid = rt:pbc(rt:select_random(RestNodes)),
    ok = rt:pbc_write(RandPid, ?BUCKET, <<"food">>, <<"food">>, "text/plain"),
    timer:sleep(1100),
    riakc_pb_socket:stop(RandPid),

    yokozuna_rt:expire_trees(Cluster),
    yokozuna_rt:wait_for_aae(Cluster),
    HP3 = random_hp(Cluster),
    yokozuna_rt:search_expect(HP3, ?INDEX, <<"*">>, <<"*">>, KeyCount + 2),

    lager:info("Downgrade cluster to previous version. Once upgraded, the
               the index format will change, throwing an error, unless you
               reindex (& resync the AAE trees) that core/search_index again."),
    yokozuna_rt:rolling_upgrade(Cluster, previous),

    yokozuna_rt:remove_index_dirs(Cluster, ?INDEX),
    yokozuna_rt:check_exists(Cluster, ?INDEX),
    yokozuna_rt:expire_trees(Cluster),
    yokozuna_rt:wait_for_aae(Cluster),

    HP4 = random_hp(Cluster),
    yokozuna_rt:search_expect(HP4, ?INDEX, <<"*">>, <<"*">>, KeyCount + 2),

    pass.

random_hp(Nodes) ->
    rt:select_random(yokozuna_rt:host_entries(
                       rt:connection_info(Nodes))).

upgrade_to_current(Nodes) ->
    [rt:upgrade(ANode, current) || ANode <- Nodes],
    [rt:wait_for_service(ANode, riak_kv) || ANode <- Nodes],
    [rt:wait_for_service(ANode, yokozuna) || ANode <- Nodes].

downgrade_to_previous(Nodes) ->
    [rt:upgrade(ANode, previous) || ANode <- Nodes],
    [rt:wait_for_service(ANode, riak_kv) || ANode <- Nodes],
    [rt:wait_for_service(ANode, yokozuna) || ANode <- Nodes].
