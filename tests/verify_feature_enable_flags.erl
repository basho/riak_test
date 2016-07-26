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

confirm() ->
    lager:info("Deploying 1 node"),
    [Node] = rt:deploy_nodes(1, ?CFG),
    Client = rt:pbc(Node),

    write_test_data(Client),

    verify_features_disabled(Client),

    ok = rpc:call(Node, application, set_env, [riak_core, job_accept_class, ?JOB_CLASSES]),
    verify_features_enabled(Client),

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

verify_features_disabled(Client) ->
    verify_list_buckets_disabled(Client).
    %%verify_list_keys_disabled(),
    %%verify_secondary_index_disabled(),
    %%verify_map_reduce_disabled().

verify_features_enabled(Client) ->
    verify_list_buckets_enabled(Client).
    %%verify_list_keys_enabled(),
    %%verify_secondary_index_enabled(),
    %%verify_map_reduce_enabled().

verify_list_buckets_disabled(Client) ->
    Expected = {error, <<"Operation 'list_buckets' is not enabled">>},
    ?assertEqual(Expected, riakc_pb_socket:list_buckets(Client)).

verify_list_buckets_enabled(Client) ->
    {ok, Buckets} = riakc_pb_socket:list_buckets(Client),
    SortedBuckets = lists:sort(Buckets),
    ?assertEqual(SortedBuckets, [<<"2i_test">>, <<"basic_test">>, <<"mapred_test">>]).
