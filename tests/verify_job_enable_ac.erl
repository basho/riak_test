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

%% Verify functionality of async job enable/disable flags in advanced.config.
-module(verify_job_enable_ac).

-behavior(riak_test).
-compile(export_all).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include("job_enable_common.hrl").

%% Start with all job classes disabled - we'll slowly enable
%% and verify all the flags over the course of the test
-define(CFG, [{riak_core, [{?APP_CONFIG_KEY, []}]}]).
-define(ALL_BUCKETS, [<<"2i_test">>, <<"basic_test">>]).
-define(BASIC_TEST_KEYS, [<<"1">>, <<"2">>, <<"3">>]).
-define(JOB_CLASSES,
    [Class || {Class, Enabled} <- ?JOB_CLASS_DEFAULTS, Enabled]).

confirm() ->
    lager:info("Deploying 1 node"),
    rt:set_backend(eleveldb),
    [Node] = rt:build_cluster(1, ?CFG),

    HttpClient = rt:httpc(Node),
    PbClient = rt:pbc(Node),

    lager:info("Writing test data via protocol buffers"),
    write_test_data(PbClient),

    run_tests(HttpClient, [verify_list_buckets_disabled_http,
                           verify_list_keys_disabled_http,
                           verify_secondary_index_disabled_http,
                           verify_mapred_disabled_http]),
    run_tests(PbClient, [verify_list_buckets_disabled_pb,
                         verify_list_keys_disabled_pb,
                         verify_secondary_index_disabled_pb,
                         verify_mapred_disabled_pb]),

    lager:info("Enabling all job classes"),
    ok = rpc:call(Node, application, set_env,
        [riak_core, ?APP_CONFIG_KEY, ?JOB_CLASSES]),

    run_tests(HttpClient, [verify_list_buckets_enabled_http,
                           verify_list_keys_enabled_http,
                           verify_secondary_index_enabled_http,
                           verify_mapred_enabled_http]),
    run_tests(PbClient, [verify_list_buckets_enabled_pb,
                         verify_list_keys_enabled_pb,
                         verify_secondary_index_enabled_pb,
                         verify_mapred_enabled_pb]),

    pass.

write_test_data(Client) ->
    BasicObjs = make_objs(<<"basic_test">>),

    [O1, O2, O3] = make_objs(<<"2i_test">>),
    MD1 = riakc_obj:get_update_metadata(O2),
    MD2 = riakc_obj:set_secondary_index(MD1, [{{integer_index, "test_idx"}, [42]}]),
    O2WithIdx = riakc_obj:update_metadata(O2, MD2),
    SecIdxObjs = [O1, O2WithIdx, O3],

    [ok = riakc_pb_socket:put(Client, O) || O <- BasicObjs ++ SecIdxObjs].

make_objs(Bucket) ->
    [riakc_obj:new(Bucket,
                   list_to_binary([N + $1]), %% Keys = ["1", "2", "3"]
                   list_to_binary([N + $A])) %% Vals = ["A", "B", "C"]
     || N <- lists:seq(0, 2)].

run_tests(Client, TestList) ->
    lists:foreach(fun(Test) -> run_test(Client, Test) end, TestList).

run_test(Client, Test) ->
    lager:info("Running test ~p", [Test]),
    ?MODULE:Test(Client).

verify_list_buckets_disabled_pb(Client) ->
    Expected = {error, ?ERRMSG_BIN(?TOKEN_LIST_BUCKETS_S)},
    ?assertEqual(Expected, riakc_pb_socket:list_buckets(Client)).

verify_list_keys_disabled_pb(Client) ->
    Expected = {error, ?ERRMSG_BIN(?TOKEN_LIST_KEYS_S)},
    ?assertEqual(Expected, riakc_pb_socket:list_keys(Client, <<"basic_test">>)).

verify_secondary_index_disabled_pb(Client) ->
    Expected = {error, ?ERRMSG_BIN(?TOKEN_SEC_INDEX)},
    ?assertEqual(Expected, riakc_pb_socket:get_index(Client, <<"2i_test">>,
                                                     {integer_index, "test_idx"}, 42)).

verify_mapred_disabled_pb(Client) ->
    Expected = {error, ?ERRMSG_BIN(?TOKEN_MAP_REDUCE)},
    ?assertEqual(Expected, riakc_pb_socket:mapred(Client, <<"basic_test">>, [])).

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

verify_mapred_enabled_pb(Client) ->
    {ok, [{_, Results}]} = riakc_pb_socket:mapred(Client, <<"basic_test">>, []),
    SortedResults = lists:sort(Results),
    Expected = [{<<"basic_test">>, integer_to_binary(K)} || K <- lists:seq(1, 3)],
    ?assertEqual(Expected, SortedResults).

verify_list_buckets_disabled_http(Client) ->
    Result = rhc:list_buckets(Client),
    ?assertMatch({error, {"403", _}}, Result).

verify_list_keys_disabled_http(Client) ->
    Result = rhc:list_keys(Client, <<"basic_test">>),
    ?assertMatch({error, {"403", _}}, Result).

verify_secondary_index_disabled_http(Client) ->
    Result = rhc:get_index(Client, <<"2i_test">>, {integer_index, "test_idx"}, 42),
    ?assertMatch({error, {"403", _}}, Result).

verify_mapred_disabled_http(Client) ->
    Result = rhc:mapred(Client, <<"basic_test">>, []),
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

verify_mapred_enabled_http(Client) ->
    {ok, [{_, Results}]} = rhc:mapred(Client, <<"basic_test">>, []),
    SortedResults = lists:sort(Results),
    Expected = [[<<"basic_test">>, integer_to_binary(K)] || K <- lists:seq(1, 3)],
    ?assertEqual(Expected, SortedResults).
