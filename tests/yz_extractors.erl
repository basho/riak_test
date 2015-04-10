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
%%-------------------------------------------------------------------

-module(yz_extractors).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(INDEX1, <<"test_idx1">>).
-define(BUCKET1, <<"test_bkt1">>).
-define(INDEX2, <<"test_idx2">>).
-define(BUCKET2, <<"test_bkt2">>).
-define(YZ_CAP, {yokozuna,extractor_map_in_cmd}).
-define(GET_MAP_RING_MFA, {yz_extractor, get_map, 1}).
-define(GET_MAP_MFA, {yz_extractor, get_map, 0}).
-define(GET_MAP_READTHROUGH_MFA, {yz_extractor, get_map_read_through, 0}).
-define(YZ_META_EXTRACTORS, {yokozuna, extractors}).
-define(YZ_EXTRACTOR_MAP, yokozuna_extractor_map).
-define(NEW_EXTRACTOR, {"application/httpheader", yz_noop_extractor}).
-define(DEFAULT_MAP, [{default, yz_noop_extractor},
                      {"application/json",yz_json_extractor},
                      {"application/riak_counter", yz_dt_extractor},
                      {"application/riak_map", yz_dt_extractor},
                      {"application/riak_set", yz_dt_extractor},
                      {"application/xml",yz_xml_extractor},
                      {"text/plain",yz_text_extractor},
                      {"text/xml",yz_xml_extractor}
                     ]).
-define(EXTRACTMAPEXPECT, lists:sort(?DEFAULT_MAP ++ [?NEW_EXTRACTOR])).
-define(SEQMAX, 20).
-define(CFG,
        [
         {yokozuna,
          [
           {enabled, true}
          ]}
        ]).

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    [_, Node|_] = Cluster = rt:build_cluster(lists:duplicate(4, {OldVsn, ?CFG})),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    assert_capability(Node, ?YZ_CAP, {unknown_capability, ?YZ_CAP}),

    OldPid = rt:pbc(Node),

    %% Generate keys, YZ only supports UTF-8 compatible keys
    GenKeys = [<<N:64/integer>> || N <- lists:seq(1, ?SEQMAX),
                                  not lists:any(
                                        fun(E) -> E > 127 end,
                                        binary_to_list(<<N:64/integer>>))],
    KeyCount = length(GenKeys),

    rt:count_calls(Cluster, [?GET_MAP_RING_MFA, ?GET_MAP_MFA]),

    yz_rt:write_data(OldPid, ?INDEX1, ?BUCKET1, GenKeys),
    %% wait for solr soft commit
    timer:sleep(1100),

    {ok, BProps} = riakc_pb_socket:get_bucket(OldPid, ?BUCKET1),
    N = proplists:get_value(n_val, BProps),

    ok = rt:stop_tracing(),
    PrevGetMapRingCC = rt:get_call_count(Cluster, ?GET_MAP_RING_MFA),
    PrevGetMapCC = rt:get_call_count(Cluster, ?GET_MAP_MFA),
    ?assertEqual(KeyCount * N, PrevGetMapRingCC),
    ?assertEqual(KeyCount * N, PrevGetMapCC),

    %% test query count
    assert_num_found_query(OldPid, ?INDEX1, KeyCount),
    riakc_pb_socket:stop(OldPid),

    {RingVal1, MDVal1} = get_ring_and_cmd_vals(Node, ?YZ_META_EXTRACTORS,
                                               ?YZ_EXTRACTOR_MAP),

    ?assertEqual(undefined, MDVal1),
    %% In previous version, Ring only gets map metadata if a non-default
    %% extractor is registered
    ?assertEqual(undefined, RingVal1),

    ?assertEqual(?DEFAULT_MAP, get_map(Node)),

    %% Custom Register
    ExtractMap = register_extractor(Node, element(1, ?NEW_EXTRACTOR),
                                    element(2, ?NEW_EXTRACTOR)),

    ?assertEqual(?EXTRACTMAPEXPECT, ExtractMap),

    %% Upgrade
    yz_rt:rolling_upgrade(Cluster, current),
    yz_rt:wait_for_aae(Cluster),

    CurrentCapabilities = rt:capability(Node, all),
    assert_capability(Node, ?YZ_CAP, true),
    assert_supported(CurrentCapabilities, ?YZ_CAP, [true, false]),

    %% test query count again
    Pid = rt:pbc(Node),
    assert_num_found_query(Pid, ?INDEX1, KeyCount),

    rt:count_calls(Cluster, [?GET_MAP_RING_MFA, ?GET_MAP_MFA,
                             ?GET_MAP_READTHROUGH_MFA]),

    yz_rt:write_data(Pid, ?INDEX2, ?BUCKET2, GenKeys),
    %% wait for solr soft commit
    timer:sleep(1100),

    ok = rt:stop_tracing(),
    CurrGetMapRingCC = rt:get_call_count(Cluster, ?GET_MAP_RING_MFA),
    CurrGetMapCC = rt:get_call_count(Cluster, ?GET_MAP_MFA),
    CurrGetMapRTCC = rt:get_call_count(Cluster, ?GET_MAP_READTHROUGH_MFA),
    ?assertEqual(1, CurrGetMapRingCC),
    ?assertEqual(KeyCount * N, CurrGetMapCC),
    lager:info("Number of calls to get_map_read_through/0: ~p~n, Number of calls to get_map/0: ~p~n",
              [CurrGetMapRTCC, CurrGetMapCC]),
    ?assert(CurrGetMapRTCC < CurrGetMapCC),

    riakc_pb_socket:stop(Pid),

    {_RingVal2, MDVal2} = get_ring_and_cmd_vals(Node, ?YZ_META_EXTRACTORS,
                                                ?YZ_EXTRACTOR_MAP),

    ?assertEqual(?EXTRACTMAPEXPECT, MDVal2),
    ?assertEqual(?EXTRACTMAPEXPECT, get_map(Node)),

    rt_intercept:add(Node, {yz_noop_extractor,
                            [{{extract, 1}, extract_httpheader}]}),
    rt_intercept:wait_until_loaded(Node),

    ExpectedExtraction = [{method,'GET'},
                          {host,<<"www.google.com">>},
                          {uri,<<"/">>}],
    ?assertEqual(ExpectedExtraction,
                 verify_extractor(Node,
                                  <<"GET http://www.google.com HTTP/1.1\n">>,
                                  element(2, ?NEW_EXTRACTOR))),

    pass.

%%%===================================================================
%%% Private
%%%===================================================================

get_ring_and_cmd_vals(Node, Prefix, Key) ->
    Ring = rpc:call(Node, yz_misc, get_ring, [transformed]),
    MDVal = metadata_get(Node, Prefix, Key),
    RingVal = ring_meta_get(Node, Key, Ring),
    {RingVal, MDVal}.

assert_capability(CNode, Capability, Value) ->
    lager:info("Checking Capability Setting ~p =:= ~p on ~p",
               [Capability, Value, CNode]),
    ?assertEqual(ok, rt:wait_until_capability(CNode, Capability, Value)).

assert_supported(Capabilities, Capability, Value) ->
    lager:info("Checking Capability Supported Values ~p =:= ~p",
               [Capability, Value]),
    ?assertEqual(Value, proplists:get_value(
                          Capability,
                          proplists:get_value('$supported', Capabilities))).

assert_num_found_query(Pid, Index, ExpectedCount) ->
    {ok, Results} = riakc_pb_socket:search(Pid, Index, <<"*:*">>),
    ?assertEqual(ExpectedCount, Results#search_results.num_found).

metadata_get(Node, Prefix, Key) ->
    rpc:call(Node, riak_core_metadata, get, [Prefix, Key, []]).

ring_meta_get(Node, Key, Ring) ->
    rpc:call(Node, riak_core_ring, get_meta, [Key, Ring]).

register_extractor(Node, MimeType, Mod) ->
    rpc:call(Node, yz_extractor, register, [MimeType, Mod]).

get_map(Node) ->
    rpc:call(Node, yz_extractor, get_map, []).

verify_extractor(Node, PacketData, Mod) ->
    rpc:call(Node, yz_extractor, run, [PacketData, Mod]).
