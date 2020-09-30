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
%% @doc Verify some MapReduce internals.
%%
%% This test used to be in riak_kv's test/mapred_test.erl. It was
%% called `compat_buffer_and_prereduce_test_'. It has been moved here
%% to avoid the fragile setup and teardown stages that frequently
%% broke eunit testing.
-module(mapred_index).
-behavior(riak_test).
-export([
         %% riak_test api
         confirm/0
        ]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).

confirm() ->
    Nodes = rt:build_cluster(3),

    SW = os:timestamp(),
    load_test_data(Nodes, 200),
    lager:info("Loaded ~w objects in ~w ms",
                [200, timer:now_diff(os:timestamp(), SW)/1000]),


    lager:info("Null op 2i query result - with return_terms"),
    Q = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_identity}, none, true}],
    Input0 = {index, ?BUCKET, <<"field2_int">>, 110, 189, true},
    {ok, R0} = rpcmr(hd(Nodes), Input0, Q),
    ?assertMatch(80, length(R0)),
    R0S = lists:sort(lists:map(fun({{_B, _K}, [{term, I}]}) -> I end, R0)),
    ExpectedR0S = lists:seq(110, 189),
    ?assertMatch(ExpectedR0S, R0S),

    lager:info("Null op 2i query result - without return_terms"),
    Input1 = {index, ?BUCKET, <<"field2_int">>, 110, 189, false},
    {ok, R1} = rpcmr(hd(Nodes), Input1, Q),
    ?assertMatch(80, length(R1)),
    R1S = lists:sort(lists:map(fun({_B, K}) -> K end, R1)),
    ?assertMatch(80, length(R1S)),

    lager:info("Extract from term then range filter"),
    Input2 = {index, ?BUCKET, <<"field2_bin">>, <<0:1/integer>>, <<1:1/integer>>, true},
    Q2 = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_extractinteger}, {term, int, this, 1, 32}, false},
            {reduce, {modfun, riak_kv_mapreduce, reduce_index_byrange}, {int, this, 110, 190}, true}],
    {ok, R2} = rpcmr(hd(Nodes), Input2, Q2),
    ?assertMatch(80, length(R2)),
    R2S = lists:sort(lists:map(fun({{_B, _K}, [{int, I}]}) -> I end, R2)),
    ExpectedR2S = lists:seq(110, 189),
    ?assertMatch(ExpectedR2S, R2S),

    lager:info("Extract from term then range filter - count in reduce"),
    Q2A = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_extractinteger}, {term, int, this, 1, 32}, false},
            {reduce, {modfun, riak_kv_mapreduce, reduce_index_byrange}, {int, this, 110, 190}, false},
            {reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, true}],
    {ok, [R2A]} = rpcmr(hd(Nodes), Input2, Q2A),
    ?assertMatch(80, R2A),

    lager:info("Extract term the range filter as prereduce - count in reduce"),
    Input2B =
        {index, ?BUCKET, <<"field2_bin">>, <<0:1/integer>>, <<1:1/integer>>,
            true, undefined,
            [{riak_kv_mapreduce, prereduce_index_extractinteger_fun, {term, int, this, 1, 32}},
                {riak_kv_mapreduce, prereduce_index_byrange_fun, {int, this, 110, 190}}]},
    Q2B = [{reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, true}],
    {ok, [R2B]} = rpcmr(hd(Nodes), Input2B, Q2B),
    ?assertMatch(80, R2B),

    lager:info("Filter by applying regex within reduce"),
    Input3 = {index, ?BUCKET, <<"field1_bin">>, <<"val0">>, <<"val1">>, true},
    {ok, RE} = re:compile(".*99.*"),
    Q3 = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_regex}, {term, this, RE}, true}],
    {ok, R3} = rpcmr(hd(Nodes), Input3, Q3),
    ?assertMatch(2, length(R3)),
    lager:info("Filter by applying regex to query"),
    Input4 = {index, ?BUCKET, <<"field1_bin">>, <<"val0">>, <<"val1">>, true, RE},
    {ok, R4} = rpcmr(hd(Nodes), Input4, Q),
    ?assertMatch(2, length(R4)),
    lager:info("Flter by applying regex as prereduce"),
    Input5 =
        {index, ?BUCKET, <<"field1_bin">>, <<"val0">>, <<"val1">>,
            true, undefined,
            [{riak_kv_mapreduce, prereduce_index_regex_fun, {term, this, RE}}]},
    {ok, R5} = rpcmr(hd(Nodes), Input5, Q),
    ?assertMatch(2, length(R5)),


    lager:info("Null op 2i query result - with log of Key"),
    Input6 =
        {index, ?BUCKET, <<"field2_int">>, 110, 189,
            true, undefined,
            [{riak_kv_mapreduce, prereduce_index_logidentity_fun, none}]},
    {ok, R6} = rpcmr(hd(Nodes), Input6, Q),
    ?assertMatch(80, length(R6)),


    pass.

load_test_data(Nodes, Count) ->
    PBPid = rt:pbc(hd(Nodes)),
    [put_an_object(PBPid, N) || N <- lists:seq(0, Count)].

rpcmr(Node, Inputs, Query) ->
    SW = os:timestamp(),
    rpc:call(Node, application, set_env, [riak_kv, pipe_log_level, [info, warn, error]]),
    R = rpc:call(Node, riak_kv_mrc_pipe, mapred, [Inputs, Query]),
    lager:info("Map/Reduce query complete in ~w ms",
        [timer:now_diff(os:timestamp(), SW)/1000]),
    R.

put_an_object(Pid, N) ->
    Key = int_to_key(N),
    Data = io_lib:format("data~p", [N]),
    BinIndex = int_to_field1_bin(N),
    Indexes = [{"field1_bin", BinIndex},
               {"field2_int", N},
               {"field2_bin", <<0:8/integer, N:32/integer, 0:8/integer>>},
               % every 5 items indexed together
               {"field3_int", N - (N rem 5)}
              ],
    put_an_object(Pid, Key, Data, Indexes).

put_an_object(Pid, Key, Data, Indexes) when is_list(Indexes) ->
    MetaData = dict:from_list([{<<"index">>, Indexes}]),
    Robj0 = riakc_obj:new(?BUCKET, Key),
    Robj1 = riakc_obj:update_value(Robj0, Data),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    riakc_pb_socket:put(Pid, Robj2).

int_to_key(N) ->
    list_to_binary(io_lib:format("obj~8..0B", [N])).

int_to_field1_bin(N) ->
    list_to_binary(io_lib:format("val~8..0B", [N])).
