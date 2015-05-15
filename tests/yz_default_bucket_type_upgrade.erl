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

-define(N, 3).
-define(YZ_CAP, {yokozuna, handle_legacy_default_bucket_type_aae}).
-define(INDEX, <<"test_upgrade_idx">>).
-define(BUCKET, <<"test_upgrade_bucket">>).
-define(SEQMAX, 2000).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16},
           {default_bucket_props, [{n_val, ?N}]},
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

    [rt:assert_capability(ANode, ?YZ_CAP, {unknown_capability, ?YZ_CAP}) || ANode <- Cluster],

    %% Generate keys, YZ only supports UTF-8 compatible keys
    GenKeys = [<<N:64/integer>> || N <- lists:seq(1, ?SEQMAX),
                                  not lists:any(
                                        fun(E) -> E > 127 end,
                                        binary_to_list(<<N:64/integer>>))],
    KeyCount = length(GenKeys),
    lager:info("KeyCount ~p", [KeyCount]),

    OldPid = rt:pbc(Node),

    yokozuna_rt:write_data(Cluster, OldPid, ?INDEX, ?BUCKET, GenKeys),
    %% wait for solr soft commit
    timer:sleep(1100),

    verify_num_found_query(Cluster, ?INDEX, KeyCount),

    %% Upgrade
    yokozuna_rt:rolling_upgrade(Cluster, current),

    [rt:assert_capability(ANode, ?YZ_CAP, v1) || ANode <- Cluster],
    [rt:assert_supported(rt:capability(ANode, all), ?YZ_CAP, [v1, v0]) || ANode <- Cluster],

    verify_num_found_query(Cluster, ?INDEX, KeyCount),

    lager:info("Write one more piece of data"),
    Pid = rt:pbc(Node),
    ok = rt:pbc_write(Pid, ?BUCKET, <<"foo">>, <<"foo">>, "text/plain"),
    timer:sleep(1100),

    yokozuna_rt:expire_trees(Cluster),
    verify_num_found_query(Cluster, ?INDEX, KeyCount + 1),

    [ok = rpc:call(ANode, application, set_env, [riak_kv, anti_entropy_build_limit, {100, 1000}])
     || ANode <- Cluster],

    pass.

verify_num_found_query(Cluster, Index, ExpectedCount) ->
    F = fun(Node) ->
                Pid = rt:pbc(Node),
                {ok, {_, _, _, NumFound}} = riakc_pb_socket:search(Pid, Index, <<"*:*">>),
                lager:info("Check Count, Expected: ~p | Actual: ~p~n",
                           [ExpectedCount, NumFound]),
                ExpectedCount =:= NumFound
        end,
    rt:wait_until(Cluster, F),
    ok.
