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
%% @doc Verify some MapReduce 2i internals.

-module(mapred_index_general).
-behavior(riak_test).
-export([
         %% riak_test api
         confirm/0
        ]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).
-define(OBJECTS, 2000).

confirm() ->
    Nodes = rt:build_cluster(3),

    SW = os:timestamp(),
    load_test_data(Nodes, ?OBJECTS),
    lager:info("Loaded ~w objects in ~w ms",
                [?OBJECTS, timer:now_diff(os:timestamp(), SW)/1000]),


    lager:info("Null op 2i query result - with return_terms"),
    Q = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_identity}, undefined, true}],
    Input0 = {index, ?BUCKET, <<"field2_int">>, 1100, 1890, true},
    {ok, R0} = rpcmr(hd(Nodes), Input0, Q),
    ?assertMatch(791, length(R0)),
    R0S = lists:sort(lists:map(fun({{_B, _K}, [{term, I}]}) -> I end, R0)),
    ExpectedR0S = lists:seq(1100, 1890),
    ?assertMatch(ExpectedR0S, R0S),

    lager:info("Null op 2i query result - without return_terms"),
    Input1 = {index, ?BUCKET, <<"field2_int">>, 1100, 1890, false},
    {ok, R1} = rpcmr(hd(Nodes), Input1, Q),
    ?assertMatch(791, length(R1)),
    R1S = lists:sort(lists:map(fun({_B, K}) -> K end, R1)),
    ?assertMatch(791, length(R1S)),

    lager:info("Extract from term then range filter"),
    Input2 = {index, ?BUCKET, <<"field2_bin">>, <<0:1/integer>>, <<1:1/integer>>, true},
    Q2 = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_extractinteger}, {term, int, this, 1, 32}, false},
            {reduce, {modfun, riak_kv_mapreduce, reduce_index_applyrange}, {int, this, 1100, 1900}, true}],
    {ok, R2} = rpcmr(hd(Nodes), Input2, Q2),
    ?assertMatch(801, length(R2)),
    R2S = lists:sort(lists:map(fun({{_B, _K}, [{int, I}]}) -> I end, R2)),
    ExpectedR2S = lists:seq(1100, 1900),
    ?assertMatch(ExpectedR2S, R2S),

    lager:info("Extract from term then range filter - count in reduce"),
    Q2A = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_extractinteger}, {term, int, this, 1, 32}, false},
            {reduce, {modfun, riak_kv_mapreduce, reduce_index_applyrange}, {int, this, 1100, 1900}, false},
            {reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, true}],
    {ok, [R2A]} = rpcmr(hd(Nodes), Input2, Q2A),
    ?assertMatch(801, R2A),
    lager:info("Repeat previous query with non-standard (large) batch size"),
    Q2ABS1 = [{reduce,
                    {modfun, riak_kv_mapreduce, reduce_index_extractinteger},
                    [{reduce_phase_batch_size, 200}, {args, {term, int, this, 1, 32}}],
                    false},
                {reduce,
                    {modfun, riak_kv_mapreduce, reduce_index_applyrange},
                    [{reduce_phase_batch_size, 200}, {args, {int, this, 1100, 1900}}],
                    false},
                {reduce,
                    {modfun, riak_kv_mapreduce, reduce_count_inputs},
                    [{reduce_phase_batch_size, 200}, {args, none}],
                    true}],
    {ok, [R2ABS1]} = rpcmr(hd(Nodes), Input2, Q2ABS1),
    ?assertMatch(801, R2ABS1),
    lager:info("Repeat previous query with non-standard (small) batch size"),
    Q2ABS2 = [{reduce,
                    {modfun, riak_kv_mapreduce, reduce_index_extractinteger},
                    [{reduce_phase_batch_size, 2}, {args, {term, int, this, 1, 32}}],
                    false},
                {reduce,
                    {modfun, riak_kv_mapreduce, reduce_index_applyrange},
                    [{reduce_phase_batch_size, 2}, {args, {int, this, 1100, 1900}}],
                    false},
                {reduce,
                    {modfun, riak_kv_mapreduce, reduce_count_inputs},
                    [{reduce_phase_batch_size, 2}, {args, none}],
                    true}],
    {ok, [R2ABS2]} = rpcmr(hd(Nodes), Input2, Q2ABS2),
    ?assertMatch(801, R2ABS2),
    lager:info("Repeat previous query without batching in reduce"),
    Q2ABS3 = [{reduce,
                    {modfun, riak_kv_mapreduce, reduce_index_extractinteger},
                    [{reduce_phase_only_1, true}, {args, {term, int, this, 1, 32}}],
                    false},
                {reduce,
                    {modfun, riak_kv_mapreduce, reduce_index_applyrange},
                    [{reduce_phase_only_1, true}, {args, {int, this, 1100, 1900}}],
                    false},
                {reduce,
                    {modfun, riak_kv_mapreduce, reduce_count_inputs},
                    [{reduce_phase_only_1, true}, {args, none}],
                    true}],
    {ok, [R2ABS3]} = rpcmr(hd(Nodes), Input2, Q2ABS3),
    ?assertMatch(801, R2ABS3),


    lager:info("Extract term the range filter as prereduce - count in reduce"),
    Input2B =
        {index, ?BUCKET, <<"field2_bin">>, <<0:1/integer>>, <<1:1/integer>>,
            true, undefined,
            [{riak_kv_index_prereduce, extract_integer, {term, int, this, 1, 32}},
                {riak_kv_index_prereduce, apply_range, {int, this, 1100, 1900}}]},
    Q2B = [{reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, true}],
    {ok, [R2B]} = rpcmr(hd(Nodes), Input2B, Q2B),
    ?assertMatch(801, R2B),

    lager:info("Filter by applying regex within reduce"),
    Input3 = {index, ?BUCKET, <<"field1_bin">>, <<"val0">>, <<"val1">>, true},
    RE = ".*99.*",
    Q3 = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_applyregex}, {term, this, RE}, true}],
    {ok, R3} = rpcmr(hd(Nodes), Input3, Q3),
    ?assertMatch(38, length(R3)),
    lager:info("Filter by applying regex to query"),
    Input4 = {index, ?BUCKET, <<"field1_bin">>, <<"val0">>, <<"val1">>, true, RE},
    {ok, R4} = rpcmr(hd(Nodes), Input4, Q),
    ?assertMatch(38, length(R4)),
    lager:info("Flter by applying regex as prereduce"),
    Input5 =
        {index, ?BUCKET, <<"field1_bin">>, <<"val0">>, <<"val1">>,
            true, undefined,
            [{riak_kv_index_prereduce, apply_regex, {term, this, RE}}]},
    {ok, R5} = rpcmr(hd(Nodes), Input5, Q),
    ?assertMatch(38, length(R5)),


    lager:info("Null op 2i query result - with log of Key"),
    Input6 =
        {index, ?BUCKET, <<"field2_int">>, 1100, 1890,
            true, undefined,
            [{riak_kv_index_prereduce, log_identity, key}]},
    {ok, R6} = rpcmr(hd(Nodes), Input6, Q),
    ?assertMatch(791, length(R6)),

    lager:info("Extract term the range filter as prereduce - find max in reduce"),
    Q7 = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_max}, {int, 1}, true}],
    {ok, [R7]} = rpcmr(hd(Nodes), Input2B, Q7),
    ?assertMatch({{<<"2ibucket">>,<<"obj00001900">>},[{int,1900}]}, R7),

    lager:info("Comparison to 2i query with regex (no m/r)"),
    PBC = rt:pbc(hd(Nodes)),
    SW2i = os:timestamp(),
    {ok, ?INDEX_RESULTS{terms=R2i}} =
        riakc_pb_socket:get_index_range(PBC,
                                        ?BUCKET,
                                        <<"field1_bin">>, <<"val0">>, <<"val1">>,
                                        [{timeout, 60000},
                                            {term_regex, ".*99.*"},
                                            {return_terms, true}]),
    ?assertMatch(38, length(R2i)),
    lager:info("2i query complete in ~w ms",
                [timer:now_diff(os:timestamp(), SW2i)/1000]),
    
    lager:info("Find values similar to ..."),
    Similar =
        riak_kv_index_prereduce:simhash(list_to_binary("Arial Effect Agile")),
    Input8 =
        {index, ?BUCKET, <<"field4_bin">>, <<0:8/integer>>, <<255:8/integer>>,
            true, undefined,
            [{riak_kv_index_prereduce, extract_integer, {term, sim, this, 1, 128}},
                {riak_kv_index_prereduce, extract_hamming, {sim, distance, this, Similar}},
                {riak_kv_index_prereduce, apply_range, {distance, this, 0, 30}}]},
    Q8 = [{map, {modfun, riak_kv_mapreduce, map_identity}, none, true}],
    {ok, R8} = rpcmr(hd(Nodes), Input8, Q8),
    R8A = lists:map(fun(Obj) -> binary_to_term(riak_object:get_value(Obj)) end, R8),
    ExpLR8A = (?OBJECTS div length(word_list())) + 1,
    ?assertMatch(ExpLR8A, length(R8A)),

    lager:info("Find values similar to ... expand the allowed hamming distance"),
    Input8B =
        {index, ?BUCKET, <<"field4_bin">>, <<0:8/integer>>, <<255:8/integer>>,
            true, undefined,
            [{riak_kv_index_prereduce, extract_integer, {term, sim, this, 1, 128}},
                {riak_kv_index_prereduce, extract_hamming, {sim, distance, this, Similar}},
                {riak_kv_index_prereduce, apply_range, {distance, this, 0, 40}}]},
    {ok, R8B} = rpcmr(hd(Nodes), Input8B, Q8),
    lager:info("~w results with hamming distance of 40", [length(R8B)]),
    ?assert(length(R8B) > ExpLR8A),

    lager:info("Return set of terms"),
    Input9 = 
        {index, ?BUCKET, <<"field3_int">>, 0, 200,
            true, undefined, []},
    Q9 = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_union}, {term, integer}, true}],
    {ok, R9} = rpcmr(hd(Nodes), Input9, Q9),
    ExpR9 = lists:map(fun(I) -> I * 5 end, lists:seq(0, 40)),
    ?assertMatch(ExpR9, R9),

    lager:info("Return count by term"),
    Q10 = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_countby}, {term, integer}, true}],
    {ok, R10} = rpcmr(hd(Nodes), Input9, Q10),
    ExpR10 = lists:map(fun(I) -> {I * 5, 5} end, lists:seq(0, 40)),
    ?assertMatch(ExpR10, R10),

    lager:info("Return results - but also count by term"),
    Q11 = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_identity}, undefined, true},
            {reduce, {modfun, riak_kv_mapreduce, reduce_index_countby}, {term, integer}, true}],
    {ok, [R11A, R11B]} = rpcmr(hd(Nodes), Input9, Q11),
    ?assertMatch(205, length(R11A)),
    ?assertMatch(ExpR10, R11B),
    ?assertMatch(205, lists:foldl(fun({_T, X}, Acc) -> Acc + X end, 0, R11B)),

    lager:info("Return results - without count by term to compare query time"),
    Q12 = [{reduce, {modfun, riak_kv_mapreduce, reduce_index_identity}, undefined, true}],
    {ok, R12} = rpcmr(hd(Nodes), Input9, Q12),
    ExpR12 = lists:sort(R11A),
    ?assertMatch(ExpR12, lists:sort(R12)),

    lager:info("Not necessary to have a reduce_identity - results can just be returned?"),
    {ok, R13} = rpcmr(hd(Nodes), Input9, []),
    ?assertMatch(ExpR12, lists:sort(R13)),

    lager:info("Sort the results by key"),
    {ok, R14} = rpcmr(hd(Nodes), Input9, [{reduce, {modfun, riak_kv_mapreduce, reduce_index_sort}, key, true}]),
    ?assertMatch(ExpR12, R14),

    lager:info("Sort the results by term"),
    {ok, R15} = rpcmr(hd(Nodes), Input9, [{reduce, {modfun, riak_kv_mapreduce, reduce_index_sort}, term, true}]),
    ?assertMatch(ExpR12, lists:sort(R15)),

    lager:info("Building a bloom from one query, then passing to another"),
    Input10 = {index, ?BUCKET, <<"field4_int">>, 4, 5,
                true, undefined,
                    [{riak_kv_index_prereduce,
                        extract_hash,
                        {key, fnva, this, fnva}}]},
    {ok, R16A} = rpcmr(hd(Nodes), Input10, []),
    HintsBloom =
        riak_kv_hints:create_gcs_metavalue(
            lists:map(fun({{_B, _K}, [{fnva, H}]}) -> {fnva, H} end, R16A),
            12,
            fnva),
    Input11 =
        {index, ?BUCKET, <<"field2_int">>, 500, 1000,
            true, undefined,
            [{riak_kv_index_prereduce,
                apply_remotebloom,
                {key, this, {riak_kv_hints, HintsBloom}}}]},
    {ok, R16B} = rpcmr(hd(Nodes), Input11, []),
    ?assertMatch(true, length(R16B) >= 100),
    ?assertMatch(true, length(R16B) < 105),



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
    WordList = word_list(),
    WordCount = length(WordList),
    Words = [lists:nth(N rem WordCount + 1, WordList), 
                lists:nth((N + 1) rem WordCount + 1, WordList),
                lists:nth((N + 2) rem WordCount + 1, WordList)],
    WordStr = lists:flatten(io_lib:format("~s ~s ~s", Words)),
    Data = lists:flatten(io_lib:format("data ~p words ~s", [N, WordStr])),
    SimHash = riak_kv_index_prereduce:simhash(list_to_binary(WordStr)),
    BinIndex = int_to_field1_bin(N),
    Indexes = [{"field1_bin", BinIndex},
               {"field2_int", N},
               {"field2_bin", <<0:8/integer, N:32/integer, 0:8/integer>>},
               % every 5 items indexed together
               {"field3_int", N - (N rem 5)},
               {"field4_int", N rem 5},
               {"field4_bin", <<N:8/integer, SimHash/binary>>}
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

word_list() ->
    ["Aerial", "Affect", "Agile", "Agriculture", "Animal", "Attract", "Audubon",
        "Backyard", "Barrier", "Beak", "Bill", "Birdbath", "Branch", "Breed", "Buzzard",
        "Cage", "Camouflage", "Capture", "Carrier", "Cheep", "Chick", "Claw", "Collusion", "Color", "Control", "Couple", "Creature", "Cruise",
        "Danger", "Diet", "Distance", "Domestic", "Drift",
        "Effect", "Eggs", "Endangered", "Environment", "Estuary", "Expert", "Eyesight",
        "Feather", "Feed", "Feeder", "Fish", "Fledgling", "Flight", "Float", "Flock", "Flutter", "Fly", "Formation",
        "Game", "Garden",
        "Habitat", "Hidden", "Hover", "Hunt",
        "Identification", "Injured",
        "Jess",
        "Keen",
        "Land", "Lay", "Limb",
        "Maintain", "Marsh", "Mate", "Migration", "Movement",
        "Names", "Nature", "Nest", "Notice", "Nuisance",
        "Observation", "Order", "Ornithology",
        "Peck", "Perch", "Pet", "Photograph", "Population", "Predator", "Professional",
        "Quantity", "Quest", "Quick", "Quiet",
        "Range", "Raptor", "Rodent", "Roost",
        "Seasonal", "Seeds", "Sentinel", "Shoot", "Shorebird", "Shorebird", "Sight", "Size", "Soar", "Song", "Songbird", "Speed", "Squirrel", "Streak", "Survive",
        "Tally", "Talon", "Tame", "Temperature", "Thermals", "Track", "Tree", "Types",
        "Universal"
        "Vegetables", "Vocal", "Vulture",
        "Wade", "Watch", "Water", "Waterfowl", "Weather", "Wetlands", "Wild", "Wildlife", "Wildlife", "Window", "Wing", "Wound",
        "Yonder", "Young"
        "Zone", "Zoo"].
