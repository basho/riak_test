%% -*- Mode: Erlang -*-
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
%% -------------------------------------------------------------------
%% @doc A module to test riak_ts basic create bucket/put/select cycle.

-module(ts_sql_eqc_util).

-define(MINSTEPS, 5).
-define(NODISTINCTTESTS, 20).
-define(NORANDOMDATAPOINTS, 100).
-define(STARTSTATE, ["1.3", "1.3", "1.3", "Left", "Left"]).
-define(INTERMEDIATESTATE, ["Left", "Left", "1.4", "1.4", "1.4"]).

-compile(export_all).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%% eqc generators

%%
%% These generators generate multipart tests:
%% * CREATE TABLE statements
%% * Data corresponding to the table schema
%% * INSERT INTO statements that put the data into Riak
%% * SELECT FROM statements that define queries
%% * the expected results of the query
%%
%% the generators are expected to return versioned tests
%% annotate with the version they are designed to run against
%%

gen_tests(Vsn) ->
    ?LET({Table, NoOfRecsPerQuanta, NoOfQuanta, TypeOfQuery, DataPoints},
         {gen_valid_create_table(Vsn),
          int(),
          int(),
          gen_query_type(),
          gen_random_datapoints()},
         make_data_and_query(Table, NoOfRecsPerQuanta, NoOfQuanta,
                             TypeOfQuery, DataPoints, Vsn)).

gen_valid_create_table(Vsn) ->
    ?LET(DDL, gen_valid_ddl(Vsn), DDL).

gen_random_datapoints() ->
    [
      {boolean, vector(?NORANDOMDATAPOINTS, bool())},
      {sint64,  vector(?NORANDOMDATAPOINTS, int())},
      {double,  vector(?NORANDOMDATAPOINTS, real())},
      %% {varchar, vector(?NORANDOMDATAPOINTS, utf8())}
      {varchar, vector(?NORANDOMDATAPOINTS, gen_ascii_name(""))}
    ].

gen_query_type() ->
    oneof([max, min, count, avg, plain]).

gen_vsn() ->
    oneof(["1.3", "1.4-downgradeable", "1.4-notdowngradeable"]).

%%
%% These generators generate ring transitions
%%

gen_transitions() ->
    ?LET({FirstLeg, 
          SecondLeg
          %% OnePointThreeTests, 
          %% OnePointFourTestsDowngradeable, 
          %% OnePointFourTestsNotDowngradeable
         },
          {
           vector(?MINSTEPS, gen_cluster()),
           vector(?MINSTEPS, gen_cluster())
           %% vector(?NODISTINCTTESTS, gen_tests("1.3")),
           %% vector(?NODISTINCTTESTS, gen_tests("1.4downgradeable")),
           %% vector(?NODISTINCTTESTS, gen_tests("1.4notdowngradeable"))
          },
         make_tests(FirstLeg, SecondLeg
                    %% OnePointThreeTests, 
                    %% OnePointFourTestsDowngradeable,
                    %% OnePointFourTestsNotDowngradeable
                   )).

gen_cluster() ->
    ?SUCHTHAT(Cluster, [
                        gen_node_state(),
                        gen_node_state(),
                        gen_node_state(),
                        gen_node_state(),
                        gen_node_state()
                       ],
              is_valid_cluster_state(Cluster)).

gen_node_state() ->
    oneof(["1.3", "1.4", "Stopped", "Left"]).

%%
%% These generators generate DDL records
%%
gen_valid_ddl(Vsn) ->
    ?SUCHTHAT(DDL, gen_ddl(Vsn), is_valid_ddl(DDL)).

gen_ddl(Vsn) ->
    ?LET({TblName, Keys, Quantum, Fields},
         {
           gen_name("Table"),
           gen_key(),
           gen_quantum(),
           list(gen_field(Vsn))
         },
         #ddl_v1{table         = add_string_interpolation(TblName),
                 fields        = set_positions(Keys ++ Fields),
                 partition_key = make_pk(Keys, Quantum),
                 local_key     = make_lk(Keys)}).

gen_key() ->
    ?LET({Fam, Series, TS},
         {gen_series_or_family_key_field(),
          gen_series_or_family_key_field(),
          gen_timeseries_key_field()},
         [Fam, Series, TS]).

%% this will eventually be versioned
gen_field(X) when X =:= "1.3"                 orelse
                  X =:= "1.4downgradeable"    orelse
                  X =:= "1.4notdowngradeable" ->
    ?LET({Nm, Type, Opt},
         {gen_name("Field"), gen_field_type(), gen_optional()},
         #riak_field_v1{name     = Nm,
                        type     = Type,
                        optional = Opt}).

gen_series_or_family_key_field() ->
    ?LET({Nm, Type},
         {gen_name("Key"), gen_field_type()},
         #riak_field_v1{name     = Nm,
                        type     = Type,
                        optional = false}).

gen_timeseries_key_field() ->
    ?LET(Nm,
         gen_name("TSKey"),
         #riak_field_v1{name     = Nm,
                        type     = timestamp,
                        optional = false}).

%%
%% generate field components
%%
gen_name(Prefix) ->
    %% utf8().
    gen_ascii_name(Prefix).

gen_field_type() ->
    ?LET(N, oneof([sint64, double, boolean, timestamp, varchar]), N).

gen_optional() ->
    ?LET(N, oneof([true, false]), N).

%% can't have a quantum of 0 so add one
gen_quantum() ->
    ?LET({Unit, No},
         {oneof([d, h, m, s]), int()},
         {Unit, abs(No) + 1}).

%%
%% generate query components
%%
gen_operator() ->
    ?LET(N, oneof(['=', '!=', '>', '<', '<=', '>=']), N).

%%
%% rando generators (make debugging easier)
%%
gen_ascii_name(Prefix) ->
    ?LET(N,
         vector(10, oneof([gen_ascii_lower_case(), gen_ascii_upper_case(), gen_ascii_num()])),
         list_to_binary(Prefix ++ "_" ++ N)).

gen_ascii_lower_case() ->
    ?LET(N, oneof([97, 98, 99, 100,
                   101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
                   111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
                   121, 122]), N).

gen_ascii_upper_case() ->
    ?LET(N, oneof([65, 66, 67, 68, 69, 70,
                   71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
                   81, 82, 83, 84, 85, 86, 87, 88, 89, 90]), N).

gen_ascii_num() ->
    ?LET(N, oneof([48, 49, 50, 51, 52, 53, 54, 55, 56, 57]), N).

%%
%% utility functions
%%
make_data_and_query(Table, NoOfRecsPerQuanta, NoOfQuanta, _TypeOfQuery, Datapoints, Vsn) ->
    NoOfRecsPerQuanta2 = abs(NoOfRecsPerQuanta),
    NoOfQuanta2 = abs(NoOfQuanta),
    CreateTable = make_table(Table),
    Data = make_data(Table, NoOfRecsPerQuanta2, NoOfQuanta2, Datapoints),
    {{data, ColumnData}, {column_names, ColNames}} = Data,
    Tablename = make_table_name(Table),
    Rows = transpose(ColumnData, []),
    Inserts = make_insert_into(Rows, ColNames, Tablename, []),
    [
     {create_table, Vsn, CreateTable},
     {data,         Vsn, Data},
     {insert_into,  Vsn, Inserts},
     {select_from,  Vsn, "SELECT FROM blah-blah"},
     {results,      Vsn, [some, result, set]}
    ].

make_insert_into([], _ColNames, _Tablename, Acc) ->
    lists:reverse(Acc);
make_insert_into([H | T], ColNames, Tablename, Acc) ->
    Zip = lists:zip(H, ColNames),
    {Vals, Cols} = lists:unzip([{Val, C} || {Val, C} <- Zip, Val /= none]),
    Vals2 = [to_string(X) || X <- Vals],
    NewAcc = "INSERT INTO " ++ Tablename ++ 
        " (" ++ string:join(Cols, ", ") ++ ") " ++
        "VALUES" ++
        " (" ++ string:join(Vals2, ", ") ++ ");",
    make_insert_into(T, ColNames, Tablename, [NewAcc | Acc]).

to_string(Int)   when is_integer(Int) -> integer_to_list(Int);
to_string(Float) when is_float(Float) -> float_to_list(Float);
to_string(Bool)  when Bool =:= true   orelse 
                      Bool =:= false  -> atom_to_list(Bool);
to_string(Bin)   when is_binary(Bin)  -> binary_to_list(Bin). 

transpose([[] | _], Acc) ->
    lists:reverse(Acc);
transpose(Matrix, Acc) ->
    %% collect the first element of all the columns
    CollectFn = fun(Elem, FoldAcc) ->
                         [hd(Elem) | FoldAcc]
                 end,
    Row = lists:foldr(CollectFn, [], Matrix),
    %% now remove those elements you have collected from the matrix
    RemoveFn = fun(X) ->
                       tl(X)
               end,
    ReducedMatrix = lists:map(RemoveFn, Matrix),
    transpose(ReducedMatrix, [Row | Acc]).

make_table(Table) ->
    riak_ql_to_string:ddl_rec_to_sql(Table).

make_data(#ddl_v1{fields = Fields}, 0, _, _) ->
    Cols = [binary_to_list(Name) || #riak_field_v1{name = Name} <- Fields],
    {{data, []}, {column_names, Cols}};
make_data(Table, NoOfRecsPerQuanta, NoOfQuanta, Datapoints) ->
    make_rows_and_cols(Table, NoOfRecsPerQuanta, NoOfQuanta, Datapoints).

make_rows_and_cols(Table, NoOfRecsPerQuanta, NoOfQuanta, Datapoints) ->
    QuantaSpec = get_quanta_spec(Table),
    Timestamps = make_timestamps(QuantaSpec, NoOfRecsPerQuanta, NoOfQuanta),
    NoOfRows = NoOfRecsPerQuanta * NoOfQuanta,
    make_fields(Table, NoOfRows, Timestamps, Datapoints).

make_fields(#ddl_v1{fields = Fields}, NoOfRows, Timestamps, Datapoints) ->
    Vals = make_f2(Fields, 1, NoOfRows, Timestamps, Datapoints, []),
    Cols = [binary_to_list(Name) || #riak_field_v1{name = Name} <- Fields],
    {{data, Vals}, {column_names, Cols}}.

make_f2([], _, _, _, _, Vals) ->
    lists:reverse(Vals);
make_f2([#riak_field_v1{type     = Type,
                        optional = Optional} | T],
        N, NoOfRows, Timestamps, Datapoints, ValAcc) ->
    Vals = make_val(NoOfRows, Type, Optional, Timestamps, Datapoints, []),
    make_f2(T, N + 1, NoOfRows, Timestamps, Datapoints, [Vals | ValAcc]).

make_val(0, _, _, _, _, Acc) ->
    lists:reverse(Acc);
make_val(N, timestamp, false, Timestamps, Datapoints, Acc) ->
    NewAcc = lists:nth(N, Timestamps),
    make_val(N - 1, timestamp, false, Timestamps, Datapoints, [NewAcc | Acc]);
make_val(N, timestamp, true, Timestamps, Datapoints, Acc) ->
    NewAcc = case trunc(N/2) * 2 of
                 N -> none;
                 _ -> lists:nth(N, Timestamps)
             end,
    make_val(N - 1, timestamp, true, Timestamps, Datapoints, [NewAcc | Acc]);
make_val(N, Type, Optional, Timestamps, Datapoints, Acc) ->
    {Type, Vals} = lists:keyfind(Type, 1, Datapoints),
    NewN = N rem length(Vals) + 1,
    NewAcc = case Optional of
                 false -> lists:nth(NewN, Vals);
                 true  -> case trunc(NewN/2) * 2 of
                              NewN -> none;
                              _    -> lists:nth(NewN, Vals)
                          end
             end,
    make_val(N - 1, Type, Optional, Timestamps, Datapoints, [NewAcc | Acc]).

make_timestamps({Quantity, Unit}, NoOfRecsPerQuanta, NoOfQuanta) ->
    BinUnit = list_to_binary(atom_to_list(Unit)),
    QuantaInMillis = riak_ql_quanta:unit_to_millis(Quantity, BinUnit),
    Diff = trunc(QuantaInMillis/NoOfRecsPerQuanta),
    make_timestamps2(0, QuantaInMillis, Diff, NoOfQuanta, []).

%%
%% because the quanta in milliseconds is not exactly divisible by the no of records
%% per quanta - we will build each quanta at a time
%%
make_timestamps2(_, _, _, 0, Acc) ->
    lists:flatten(lists:reverse(Acc));
make_timestamps2(LowerQuantumBoundary, QuantaInMillis, Diff, N, Acc) ->
    Upper = LowerQuantumBoundary + QuantaInMillis,
    NewAcc = lists:seq(LowerQuantumBoundary, Upper, Diff),
    make_timestamps2(Upper, QuantaInMillis, Diff, N - 1, [NewAcc | Acc]).

get_quanta_spec(#ddl_v1{partition_key = PK}) ->
    #key_v1{ast = AST} = PK,
    [Spec] = [{Quantity, Unit} || #hash_fn_v1{args = [_, Quantity, Unit]} <- AST],
    Spec.

make_table_name(#ddl_v1{table = Tablename}) ->
    binary_to_list(Tablename).

make_tests(FirstLeg, SecondLeg
           %% OnePointThreeTests, 
           %% OnePointFourTestsDowngradeable,
           %% OnePointFourNotDowngradeable
          ) ->
    Transitions = interpolate_steps([?STARTSTATE] ++
                                        FirstLeg ++
                                        [?INTERMEDIATESTATE] ++
                                        SecondLeg ++
                                        [?STARTSTATE], []),
    Transitions.

interpolate_steps([Last | []], Acc) ->
    lists:reverse([Last | Acc]);
interpolate_steps([State1, State2 | Tail], Acc) ->
    case is_one_step(State1, State2) of
        true      -> interpolate_steps([State2 | Tail], [State1 | Acc]);
        false     -> NewState = make_step(State1, State2, []),
                     interpolate_steps([NewState, State2 | Tail], [State1 | Acc]);
        identical -> interpolate_steps([State2 | Tail], Acc)
    end.

is_one_step(ClusterState1, ClusterState2) ->
    Pairs = lists:zip(ClusterState1, ClusterState2),
    Identical = length([X || {X, X} <- Pairs]),
    if
        Identical =:= 5         -> identical;
        Identical =:= 4         -> true;
        Identical <   4         -> false
    end.

%% We are taking 2 ClusterStates which differ by more than 1 change
%% and creating a new ClusterState which is 1 change different from
%% ClusterState1 by 1 change - and on the way to ClusterState2
%% BUT this intermediate state must be valid - ie 3 running Nodes

make_step([H1 | T1], [H2 | T2], Acc)  when H1 =/= H2->
    NextStep = lists:reverse([H2 | Acc]) ++ T1,
    case is_valid_cluster_state(NextStep) of
        true  -> NextStep;
        false -> make_step(T1, T2, [H1 | Acc])
    end;
make_step([H | T1], [H | T2], Acc) ->
    make_step(T1, T2, [H | Acc]).

is_valid_cluster_state(ClusterState) ->
    Running = [X || X <- ClusterState,
                    X =/= "Stopped" andalso X =/= "Left"],
    LiveNodes = length(Running),
    if
        LiveNodes >= 3 -> true;
        LiveNodes <  3 -> false
    end.

set_positions(Fields) ->
    set_p2(Fields, 1, []).

set_p2([], _N, Acc) ->
    lists:reverse(Acc);
set_p2([H | T], N, Acc) ->
    set_p2(T, N + 1, [H#riak_field_v1{position = N} | Acc]).

make_pk([Family, Series, TS], {Unit, No}) ->
    Quantum =  [#hash_fn_v1{mod  = riak_ql_quanta,
                            fn   = quantum,
                            args = [#param_v1{name = [TS#riak_field_v1.name]}, No, Unit],
                            type = timestamp}],
    AST = [#param_v1{name = [X#riak_field_v1.name]} || X <- [Family, Series]] ++ Quantum,
    #key_v1{ast = AST}.

make_lk(Key) ->
    #key_v1{ast = [#param_v1{name = [X#riak_field_v1.name]} || X <- Key]}.

is_valid_ddl(#ddl_v1{fields = Fs}) ->
    Fs2 = lists:sort([X#riak_field_v1.name || X <- Fs]),
    _IsValid = case lists:usort(Fs2) of
                   Fs2 -> true;
                   _   -> false
               end.

make_timestamp() ->
    {Mega, Secs, Micro} = erlang:now(),
    string:join([
                 integer_to_list(Mega),
                 integer_to_list(Secs),
                 integer_to_list(Micro)
                ], "_").

add_string_interpolation(TableBin) ->
    list_to_binary(binary_to_list(TableBin) ++ "_~s").

-endif.
