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
%% @doc Verify some MapReduce with a multi-stage pre-reduce and m/r stage
%%
-module(mapred_index_reporting).
-behavior(riak_test).
-export([
         %% riak_test api
         confirm/0
        ]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).
-define(OBJECTS, 100000).

confirm() ->
    Nodes = rt:build_cluster(3),

    SW = os:timestamp(),
    load_test_data(Nodes, ?OBJECTS),
    lager:info("Loaded ~w objects in ~w ms",
                [?OBJECTS, timer:now_diff(os:timestamp(), SW)/1000]),

    % We assume today is always 2020 10 27 - this is groundhog day for this test
    AgeMap = 
        [{<<"19301027">>, <<"Over90">>}|
            lists:map(fun(Y) -> {iolist_to_binary(io_lib:format("~4..0B", [Y]) ++ "1027"),
                                    iolist_to_binary(io_lib:format("Age~2..0B", [2020 - Y]))}
                                end,
                            lists:seq(1931, 1955))],

    PreReduceFuns =
        [{riak_kv_index_prereduce,
                extract_binary,
                {term,
                    dob,
                    all,
                    0, 8}},
            {riak_kv_index_prereduce,
                extract_binary,
                {term,
                    gpprovider,
                    all,
                    8, 6}},
            {riak_kv_index_prereduce,
                extract_binary,
                {term,
                    conditions_b64,
                    all,
                    14, all}},
            
            % Split out the three binaries that make up the term
            
            {riak_kv_index_prereduce,
                extract_buckets,
                {dob,
                    age,
                    all,
                    AgeMap,
                    <<"Unexpected">>}},
            
            % Take each date of birth, and map it to an Age category.  There should not
            % be results over the highest Date Of Birth in the Age Map (i.e. people
            % younger than 65)
            
            {riak_kv_index_prereduce,
                extract_encoded,
                {conditions_b64,
                    conditions_bin,
                    all}},
            
            % This converts the base64 bitmap that represents conditions back into the binary

            {riak_kv_index_prereduce,
                extract_integer,
                {conditions_bin,
                    conditions,
                    all,
                    0, 8}},
            
            % the bitmask is a single byte integer - make it an integer
            
            {riak_kv_index_prereduce,
                extract_mask,
                {conditions,
                    is_diabetic_int,
                    all,
                    1}},

            % Only interested in a single bit of the mask

            {riak_kv_index_prereduce,
                extract_buckets,
                {is_diabetic_int,
                    is_diabetic,
                    all,
                    [{0, <<"NotD">>}, {1, <<"IsD">>}],
                    <<"Unexpected">>}},

            % Covert this flag to some text

            {riak_kv_index_prereduce,
                extract_coalesce,
                {[gpprovider, age, is_diabetic],
                    count_term,
                    this,
                    <<"|">>}}

            % Create a single term that contains only the information
            % we wish to count

                ],
    
    ReduceFuns =
        [{reduce,
                {modfun, riak_kv_mapreduce, reduce_index_countby},
                [%{reduce_phase_only_1, true},
                    {reduce_phase_batch_size, 1000},
                    {args, {count_term, binary}}
                ],
                true}

                % Count the results by the counting term
                
            ],

    QueryFun = 
        fun(PRFuns, RFuns) ->
            rpcmr(
                hd(Nodes),
                {index, 
                    ?BUCKET,
                        <<"conditions_bin">>,
                        <<"0">>, <<"19551027">>, 
                        true,
                        undefined,
                        % Find everyone over 65
                        PRFuns
                        },
                RFuns
                )
        end,

    {ok, R} = QueryFun(PreReduceFuns, ReduceFuns),
        
    % lager:info("R ~p", [R]),
    lager:info("Some time comparisons"),
    lager:info("Without reduce statement"),
    {ok, R0} = QueryFun(PreReduceFuns, []),
    lager:info("Without prereduce functions or reduce statement"),
    {ok, R1} = QueryFun([], []),

    lager:info("Number of unique terms ~w", [length(R)]),
    TermCount = lists:foldl(fun({_T, X}, Acc) -> Acc + X end, 0, R), 
    lager:info("Number of terms ~w", [TermCount]),
    lager:info("Number of matches on query ~w", [length(R1)]),

    ?assertMatch(true, TermCount == length(R1)),

    lager:info("Rereun query but force a prereduce of the reduce - i.e. distribute"),
    ForcePreReduceReduceFuns =
        [{prereduce,
                {modfun, riak_kv_mapreduce, reduce_index_countby},
                [%{reduce_phase_only_1, true},
                    {reduce_phase_batch_size, 1000},
                    {args, {count_term, binary}}
                ],
                true}

                % Count the results by the counting term
                
            ],
    {ok, R2} = QueryFun(PreReduceFuns, ForcePreReduceReduceFuns),
    ?assertMatch(true, lists:sort(R) == lists:sort(R2)),

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
    Data = lists:flatten(io_lib:format("data ~p", [N])),
    BinIndex = int_to_field1_bin(N),
    Indexes = [{"field1_bin", BinIndex},
               {"field2_int", N},
               {"field2_bin", <<0:8/integer, N:32/integer, 0:8/integer>>},
               {"conditions_bin", generate_clinicalreport_index()}
              ],
    put_an_object(Pid, Key, Data, Indexes).

put_an_object(Pid, Key, Data, Indexes) when is_list(Indexes) ->
    MetaData = dict:from_list([{<<"index">>, Indexes}]),
    Robj0 = riakc_obj:new(?BUCKET, Key),
    Robj1 = riakc_obj:update_value(Robj0, Data),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    riakc_pb_socket:put(Pid, Robj2).

generate_clinicalreport_index() ->
    RandomDoB =
        io_lib:format("~4..0B~2..0B~2..0B",
                        [1920 + rand:uniform(80),
                            rand:uniform(12),
                            rand:uniform(28)]),
    RandomGP =
        io_lib:format("GP~4..0B", [rand:uniform(5)]),

    IsDiabetic = case rand:uniform(20) of 1 -> 1; _ -> 0 end,
    IsObese = case rand:uniform(4) of 1 -> 2; _ -> 0 end,
    IsCOPD = case rand:uniform(20) of 1 -> 4; _ -> 0 end,
    IsStroke = case rand:uniform(10) of 1 -> 8; _ -> 0 end,
    IsHeartD = case rand:uniform(12) of 1 -> 16; _ -> 0 end,
    IsCancer = case rand:uniform(40) of 1 -> 32; _ -> 0 end,
    IsAlcoholD = case rand:uniform(60) of 1 -> 64; _ -> 0 end,
    IsDrugD = case rand:uniform(200) of 1 -> 128; _ -> 0 end,

    RandomChronicConditions =
        IsDiabetic + IsObese + IsCOPD + IsStroke +
        IsHeartD + IsCancer + IsAlcoholD + IsDrugD,
    
    complete_conditions_index(RandomDoB, RandomGP, RandomChronicConditions).

complete_conditions_index(DoB, GPProvider, Conditions) ->
    iolist_to_binary(DoB ++  
                        GPProvider ++
                        base64:encode(<<Conditions:8/integer>>)).


int_to_key(N) ->
    list_to_binary(io_lib:format("obj~8..0B", [N])).

int_to_field1_bin(N) ->
    list_to_binary(io_lib:format("val~8..0B", [N])).
