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
-module(verify_2i_mixed_cluster).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-import(secondary_index_tests, [put_an_object/2]).
-define(BUCKET, <<"2ibucket">>).

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),
    Nodes =
    [CurrentNode, OldNode1, _] =
    rt:build_cluster([{current,
                               [{riak_kv, [{anti_entropy, {off, []}}]}]},
                              OldVsn, OldVsn]),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    
    PBC1 = rt:pbc(CurrentNode),
    PBC2 = rt:pbc(OldNode1),
    HTTPC1 = rt:httpc(CurrentNode),

    Clients = [{pb, PBC1}, {pb, PBC2}, {http, HTTPC1}],
    
    [put_an_object(PBC1, N) || N <- lists:seq(0, 20)],
    
    K = fun secondary_index_tests:int_to_key/1,

    assertExactQuery(Clients, [K(5)], <<"field1_bin">>, <<"val5">>),
    assertExactQuery(Clients, [K(5)], <<"field2_int">>, 5),
    assertExactQuery(Clients, [K(N) || N <- lists:seq(5, 9)], <<"field3_int">>, 5),
    assertRangeQuery(Clients, [K(N) || N <- lists:seq(10, 18)], <<"field1_bin">>, <<"val10">>, <<"val18">>),
    assertRangeQuery(Clients, [K(N) || N <- lists:seq(10, 19)], <<"field2_int">>, 10, 19),
    assertRangeQuery(Clients, [K(N) || N <- lists:seq(10, 17)], <<"$key">>, <<"obj10">>, <<"obj17">>),

    lager:info("Delete an object, verify deletion..."),
    ToDel = [<<"obj05">>, <<"obj11">>],
    [?assertMatch(ok, riakc_pb_socket:delete(PBC1, ?BUCKET, KD)) || KD <- ToDel],
    lager:info("Make sure the tombstone is reaped..."),
    ?assertMatch(ok, rt:wait_until(fun() -> rt:pbc_really_deleted(PBC1, ?BUCKET, ToDel) end)),
    
    assertExactQuery(Clients, [], <<"field1_bin">>, <<"val5">>),
    assertExactQuery(Clients, [], <<"field2_int">>, 5),
    assertExactQuery(Clients, [K(N) || N <- lists:seq(6, 9)], <<"field3_int">>, 5),
    assertRangeQuery(Clients, [K(N) || N <- lists:seq(10, 18), N /= 11], <<"field1_bin">>, <<"val10">>, <<"val18">>),
    assertRangeQuery(Clients, [K(N) || N <- lists:seq(10, 19), N /= 11], <<"field2_int">>, 10, 19),
    assertRangeQuery(Clients, [K(N) || N <- lists:seq(10, 17), N /= 11], <<"$key">>, <<"obj10">>, <<"obj17">>),

    pass.

assertExactQuery(C, K, F, V) ->
    secondary_index_tests:assertExactQuery(C, K, F, V, {false, false}).

assertRangeQuery(C, K, F, V1, V2) ->
    secondary_index_tests:assertRangeQuery(C, K, F, V1, V2, undefined, {false, false}).
