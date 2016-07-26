%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(verify_feature_enable_flags).

-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% Start with all job classes disabled - we'll slowly enable
%% and verify all the flags over the course of the test
-define(CFG, [{riak_core, [{job_accept_class, []}]}]).
-define(JOB_CLASSES, [list_buckets, list_keys, secondary_index, map_reduce]).
-define(ALL_BUCKETS, [<<"2i_test">>, <<"basic_test">>, <<"mapred_test">>]).
-define(BASIC_TEST_KEYS, [<<"1">>, <<"2">>, <<"3">>]).

confirm() ->
    lager:info("Deploying 1 node"),
    rt:set_backend(eleveldb),
    [Node] = rt:build_cluster(1, ?CFG),

    HttpClient = rt:httpc(Node),
    PbClient = rt:pbc(Node),

    lager:info("Writing test data via protocol buffers"),
    write_test_data(PbClient),

    lager:info("Verifying features are disabled via HTTP"),
    verify_features_disabled_http(HttpClient),
    lager:info("Verifying features are disabled via protocol buffers"),
    verify_features_disabled_pb(PbClient),

    lager:info("Enabling all job classes"),
    ok = rpc:call(Node, application, set_env, [riak_core, job_accept_class, ?JOB_CLASSES]),

    lager:info("Verifying features are enabled via HTTP"),
    verify_features_enabled_http(HttpClient),
    lager:info("Verifying features are enabled via protocol buffers"),
    verify_features_enabled_pb(PbClient),

    pass.

write_test_data(Client) ->
    BasicObjs = make_objs(<<"basic_test">>),

    [O1, O2, O3] = make_objs(<<"2i_test">>),
    MD1 = riakc_obj:get_update_metadata(O2),
    MD2 = riakc_obj:set_secondary_index(MD1, [{{integer_index, "test_idx"}, [42]}]),
    O2WithIdx = riakc_obj:update_metadata(O2, MD2),
    SecIdxObjs = [O1, O2WithIdx, O3],

    MapRed1 = riakc_obj:new(<<"mapred_test">>, <<"mine">>, term_to_binary(["eggs", "bacon"])),
    MapRed2 = riakc_obj:new(<<"mapred_test">>, <<"yours">>, term_to_binary(["bread", "bacon"])),
    MapRedObjs = [MapRed1, MapRed2],

    [ok = riakc_pb_socket:put(Client, O) || O <- BasicObjs ++ SecIdxObjs ++ MapRedObjs].

make_objs(Bucket) ->
    [riakc_obj:new(Bucket,
                   list_to_binary([N + $1]), %% Keys = ["1", "2", "3"]
                   list_to_binary([N + $A])) %% Vals = ["A", "B", "C"]
     || N <- lists:seq(0, 2)].

verify_features_disabled_http(Client) ->
    verify_list_buckets_disabled_http(Client),
    verify_list_keys_disabled_http(Client),
    verify_secondary_index_disabled_http(Client).

verify_features_disabled_pb(Client) ->
    verify_list_buckets_disabled_pd(Client),
    verify_list_keys_disabled_pd(Client),
    verify_secondary_index_disabled_pd(Client).
    %%verify_map_reduce_disabled_pd().

verify_features_enabled_http(Client) ->
    verify_list_buckets_enabled_http(Client),
    verify_list_keys_enabled_http(Client),
    verify_secondary_index_enabled_http(Client).

verify_features_enabled_pb(Client) ->
    verify_list_buckets_enabled_pb(Client),
    verify_list_keys_enabled_pb(Client),
    verify_secondary_index_enabled_pb(Client).
    %%verify_map_reduce_enabled_pb().

verify_list_buckets_disabled_pd(Client) ->
    Expected = {error, <<"Operation 'list_buckets' is not enabled">>},
    ?assertEqual(Expected, riakc_pb_socket:list_buckets(Client)).

verify_list_keys_disabled_pd(Client) ->
    Expected = {error, <<"Operation 'list_keys' is not enabled">>},
    ?assertEqual(Expected, riakc_pb_socket:list_keys(Client, <<"basic_test">>)).

verify_secondary_index_disabled_pd(Client) ->
    Expected = {error, <<"Secondary index queries have been disabled in the configuration">>},
    ?assertEqual(Expected, riakc_pb_socket:get_index(Client, <<"2i_test">>,
                                                     {integer_index, "test_idx"}, 42)).

verify_list_buckets_enabled_pb(Client) ->
    {ok, Buckets} = riakc_pb_socket:list_buckets(Client),
    SortedBuckets = lists:sort(Buckets),
    ?assertEqual(SortedBuckets, ?ALL_BUCKETS).

verify_list_keys_enabled_pb(Client) ->
    {ok, Keys} = riakc_pb_socket:list_keys(Client, <<"basic_test">>),
    SortedKeys = lists:sort(Keys),
    ?assertEqual(SortedKeys, ?BASIC_TEST_KEYS).

verify_secondary_index_enabled_pb(Client) ->
    Result = riakc_pb_socket:get_index_eq(Client, <<"2i_test">>, {integer_index, "test_idx"}, 42),
    ?assertMatch({ok, {index_results_v1, [<<"2">>], _, _}}, Result).

verify_list_buckets_disabled_http(Client) ->
    Result = rhc:list_buckets(Client),
    ?assertMatch({error, {"403", _}}, Result).

verify_list_keys_disabled_http(Client) ->
    Result = rhc:list_keys(Client, <<"basic_test">>),
    ?assertMatch({error, {"403", _}}, Result).

verify_secondary_index_disabled_http(Client) ->
    Result = rhc:get_index(Client, <<"2i_test">>, {integer_index, "test_idx"}, 42),
    ?assertMatch({error, {"403", _}}, Result).

verify_list_buckets_enabled_http(Client) ->
    {ok, Buckets} = rhc:list_buckets(Client),
    SortedBuckets = lists:sort(Buckets),
    ?assertEqual(SortedBuckets, ?ALL_BUCKETS).

verify_list_keys_enabled_http(Client) ->
    {ok, Keys} = rhc:list_keys(Client, <<"basic_test">>),
    SortedKeys = lists:sort(Keys),
    ?assertEqual(SortedKeys, ?BASIC_TEST_KEYS).

verify_secondary_index_enabled_http(Client) ->
    Result = rhc:get_index(Client, <<"2i_test">>, {integer_index, "test_idx"}, 42),
    ?assertMatch({ok, {index_results_v1, [<<"2">>], _, _}}, Result).
