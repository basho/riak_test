%% -*- Mode: Erlang -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015, 2016 Basho Technologies, Inc.
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
%% @doc A util module for riak_ts tests

-module(ts_data).

-include_lib("eunit/include/eunit.hrl").

-export([get_ddl/0, get_ddl/1, get_ddl/2,
         assert/3,
         assert_error_regex/3,
         assert_float/3,
         assert_row_sets/2,
         exclusive_result_from_data/3,
         flat_format/2,
         get_bool/1,
         get_cols/0, get_cols/1,
         get_data/1,
         get_default_bucket/0,
         get_float/0,
         get_integer/0,
         get_invalid_obj/0,
         get_invalid_qry/1,
         get_long_obj/0,
         get_map/1,
         get_optional/2,
         get_short_obj/0,
         get_string/1,
         get_timestamp/0,
         get_valid_aggregation_data/1,
         get_valid_aggregation_data_not_null/1,
         get_valid_big_data/1,
         get_valid_obj/0,
         get_valid_qry/0, get_valid_qry/1,
         get_valid_qry/2, get_valid_qry/3,
         get_valid_qry_spanning_quanta/0,
         get_valid_select_data/0, get_valid_select_data/1,
         get_valid_select_data_spanning_quanta/0,
         get_varchar/0,
         remove_last/1,
         results/1
        ]).

-define(MAXVARCHARLEN,       16).
-define(MAXTIMESTAMP,        trunc(math:pow(2, 63))).
-define(MAXFLOAT,            math:pow(2, 63)).

%% This is also the name of the table
get_default_bucket() ->
    "GeoCheckin".

get_valid_qry() ->
    get_valid_qry(get_default_bucket()).

get_valid_qry(Table) ->
    "select * from " ++ Table ++ " Where time > 1 and time < 10 and myfamily = 'family1' and myseries ='seriesX'".

get_valid_qry(Lower, Upper) ->
    get_valid_qry(Lower, Upper, get_default_bucket()).

get_valid_qry(Lower, Upper, Table) ->
    lists:flatten(io_lib:format("select * from " ++ Table ++ " Where time > ~B and time < ~B and myfamily = 'family1' and myseries ='seriesX'", [Lower, Upper])).

get_invalid_qry(borked_syntax) ->
    "selectah * from GeoCheckin Where time > 1 and time < 10";
get_invalid_qry(key_not_covered) ->
    "select * from GeoCheckin Where time > 1 and time < 10";
get_invalid_qry(invalid_operator) ->
    "select * from GeoCheckin Where time > 1 and time < 10 and myfamily = 'family1' and myseries ='seriesX' and weather > 'bob'";
get_invalid_qry(field_comparison) ->
    "select * from GeoCheckin Where time > 1 and time < 10 and myfamily = 'family1' and myseries ='seriesX' and weather = family1";
get_invalid_qry(type_error) ->
    "select * from GeoCheckin Where time > 1 and time < 10 and myfamily = 'family1' and myseries ='seriesX' and weather > true".

get_valid_select_data() ->
    get_valid_select_data(fun() -> lists:seq(1, 10) end).

get_valid_select_data(SeqFun) ->
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    Times = SeqFun(),
    [{Family, Series, X, get_varchar(), get_float()} || X <- Times].


-define(SPANNING_STEP_BIG, (1000)).

get_valid_big_data(N) ->
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    Times = lists:seq(1, N),
    [{
        Family,
        Series,
        1 + N * ?SPANNING_STEP_BIG,
        N,
        N + 0.1,
        get_bool(X),
        N + 100000,
        get_varchar(),
        get_optional(X, X)
    } || X <- Times].

get_valid_aggregation_data(N) ->
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    Times = lists:seq(1, N),
    [{Family, Series, X,
      get_optional(X, get_float()),
      get_optional(X+1, get_float()),
      get_optional(X*3, get_float())} || X <- Times].

get_valid_aggregation_data_not_null(N) ->
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    Times = lists:seq(1, N),
    [{Family, Series, X,
      get_float(),
      get_float(),
      get_float()} || X <- Times].

-define(SPANNING_STEP, (1000*60*5)).

get_valid_qry_spanning_quanta() ->
    StartTime = 1 + ?SPANNING_STEP *  1,
    EndTime   = 1 + ?SPANNING_STEP * 10,
    lists:flatten(
      io_lib:format("select * from GeoCheckin Where time >= ~b and time <= ~b"
                    " and myfamily = 'family1' and myseries = 'seriesX'",
                    [StartTime, EndTime])).

get_valid_select_data_spanning_quanta() ->
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    Times = lists:seq(1 + ?SPANNING_STEP, 1 + ?SPANNING_STEP * 10, ?SPANNING_STEP),  %% five-minute intervals, to span 15-min buckets
    [{Family, Series, X, get_varchar(), get_float()} || X <- Times].

get_cols() ->
    get_cols(small).
get_cols(small) ->
    [<<"myfamily">>,
     <<"myseries">>,
     <<"time">>,
     <<"weather">>,
     <<"temperature">>];
get_cols(api) ->
    [<<"myfamily">>,
     <<"myseries">>,
     <<"time">>,
     <<"myint">>,
     <<"mybin">>,
     <<"myfloat">>,
     <<"mybool">>].


exclusive_result_from_data(Data, Start, Finish) when is_integer(Start)   andalso
                                                     is_integer(Finish)  andalso
                                                     Start  > 0          andalso
                                                     Finish > 0          andalso
                                                     Finish > Start ->
    lists:sublist(Data, Start, Finish - Start + 1).

remove_last(Data) ->
    lists:reverse(tl(lists:reverse(Data))).

%% a valid DDL - the one used in the documents
get_ddl() ->
    get_ddl(small).

get_ddl(small) ->
    get_ddl(small, get_default_bucket());
get_ddl(big) ->
    get_ddl(big, get_default_bucket());
get_ddl(api) ->
    get_ddl(api, get_default_bucket());
get_ddl(aggregation) ->
    get_ddl(aggregation, "WeatherData").


get_ddl(small, Table) ->
    "CREATE TABLE " ++ table_to_list(Table) ++ " ("
    " myfamily    varchar   not null,"
    " myseries    varchar   not null,"
    " time        timestamp not null,"
    " weather     varchar   not null,"
    " temperature double,"
    " PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')), "
    " myfamily, myseries, time))";

%% another valid DDL - one with all the good stuff like
%% different types and optional blah-blah
get_ddl(big, Table) ->
    "CREATE TABLE " ++ table_to_list(Table) ++ " ("
    " myfamily    varchar     not null,"
    " myseries    varchar     not null,"
    " time        timestamp   not null,"
    " myint       sint64      not null,"
    " myfloat     double      not null,"
    " mybool      boolean     not null,"
    " mytimestamp timestamp   not null,"
    " myvarchar   varchar     not null,"
    " myoptional  sint64,"
    " PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')),"
    " myfamily, myseries, time))";

%% the other DDLs which used to be here but were only used once, each
%% in a corresponding ts_A_create_*_fail module, have been moved to
%% those respective modules

get_ddl(api, Table) ->
    "CREATE TABLE " ++ table_to_list(Table) ++ " ("
    " myfamily    varchar     not null,"
    " myseries    varchar     not null,"
    " time        timestamp   not null,"
    " myint       sint64      not null,"
    " mybin       varchar     not null,"
    " myfloat     double      not null,"
    " mybool      boolean     not null,"
    " PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')),"
    " myfamily, myseries, time))";

%% DDL for testing aggregation behavior
get_ddl(aggregation, Table) ->
    "CREATE TABLE " ++ table_to_list(Table) ++ " ("
    " myfamily      varchar   not null,"
    " myseries      varchar   not null,"
    " time          timestamp not null,"
    " temperature   double,"
    " pressure      double,"
    " precipitation double,"
    " PRIMARY KEY ((myfamily, myseries, quantum(time, 10, 'm')), "
    " myfamily, myseries, time))".

table_to_list(Table) when is_binary(Table) ->
    binary_to_list(Table);
table_to_list(Table) ->
    Table.

get_data(api) ->
    [{<<"family1">>, <<"seriesX">>, 100, 1, <<"test1">>, 1.0, true}] ++
    [{<<"family1">>, <<"seriesX">>, 200, 2, <<"test2">>, 2.0, false}] ++
    [{<<"family1">>, <<"seriesX">>, 300, 3, <<"test3">>, 3.0, true}] ++
    [{<<"family1">>, <<"seriesX">>, 400, 4, <<"test4">>, 4.0, false}].

get_map(api) ->
    [{<<"myfamily">>, 1},
     {<<"myseries">>, 2},
     {<<"time">>,     3},
     {<<"myint">>,    4},
     {<<"mybin">>,    5},
     {<<"myfloat">>,  6},
     {<<"mybool">>,   7}].


get_valid_obj() ->
    {get_varchar(),
     get_varchar(),
     get_timestamp(),
     get_varchar(),
     get_float()}.

get_invalid_obj() ->
    {get_varchar(),
     get_integer(),   % this is the duff field
     get_timestamp(),
     get_varchar(),
     get_float()}.

get_short_obj() ->
    {get_varchar(),
        get_varchar(),
        get_timestamp(),
        get_varchar()}.

get_long_obj() ->
    {get_varchar(),
        get_varchar(),
        get_timestamp(),
        get_varchar(),
        get_float(),
        get_float()}.

get_varchar() ->
    Len = random:uniform(?MAXVARCHARLEN),
    String = get_string(Len),
    list_to_binary(String).

get_string(Len) ->
    get_s(Len, []).

get_integer() ->
    get_timestamp().

get_s(0, Acc) ->
    Acc;
get_s(N, Acc) when is_integer(N) andalso N > 0 ->
    %% make it plain ASCII
    get_s(N - 1, [crypto:rand_uniform($a, $z) | Acc]).

get_timestamp() ->
    random:uniform(?MAXTIMESTAMP).

get_float() ->
    F1 = random:uniform(trunc(?MAXFLOAT)),
    F2 = random:uniform(trunc(?MAXFLOAT)),
    F1 - F2 + random:uniform().

get_bool(N) when N < 5 -> true;
get_bool(_)            -> false.

get_optional(N, X) ->
    case N rem 2 of
        0 -> X;
        1 -> []
    end.


-define(DELTA, 1.0e-15).

assert_float(String, {ok, Thing1}, {ok, Thing2}) ->
    assert_float(String, Thing1, Thing2);
assert_float(String, {Cols, [ValsA]} = Exp, {Cols, [ValsB]} = Got) ->
    case assertf2(tuple_to_list(ValsA), tuple_to_list(ValsB)) of
        fail ->
            lager:info("*****************", []),
            lager:info("Test ~p failed", [String]),
            lager:info("Exp ~p", [Exp]),
            lager:info("Got ~p", [Got]),
            lager:info("*****************", []),
            fail;
        pass ->
            pass
    end;
assert_float(String, Exp, Got) ->
    assert(String, Exp, Got).

assertf2([], []) -> pass;
assertf2([H1 | T1], [H2 | T2]) when is_float(H1), is_float(H2) ->
    Diff = H1 - H2,
    Av = abs(H1 + H2)/2,
    if Diff/Av > ?DELTA -> fail;
       el/=se           -> assertf2(T1, T2)
    end;
assertf2([H | T1], [H | T2]) ->
    assertf2(T1, T2);
assertf2(_, _) ->
    fail.


assert(_,      X,   X)   -> pass;
%% as a special case, if you don't know the exact words to expect in
%% the error message, use a '_' to match any string
assert(_, {error, {ErrCode, _ErrMsg}}, {error, {ErrCode, '_'}}) ->
    pass;
assert(String, Exp, Got) ->
    lager:info("*****************", []),
    lager:info("Test ~p failed", [String]),
    lager:info("Exp ~p", [Exp]),
    lager:info("Got ~p", [Got]),
    lager:info("*****************", []),
    fail.

%% Match an error code and use a regex to match the error string
assert_error_regex(String, {error, {Code, Regex}}, {error, {Code, Msg}}) ->
    {ok, RE} = re:compile(Regex),
    Match = re:run(Msg, RE),
    assert_error_regex_result(Match, String, Regex, Msg);
assert_error_regex(String, Got, Expected) ->
    assert_error_regex_result(nomatch, String, Got, Expected).
assert_error_regex_result(nomatch, String, Expected, Got) ->
    assert(String, Expected, Got);
assert_error_regex_result(_, _String, _Expected, _Got) ->
    pass.

%% If `ColExpected' is the atom `rt_ignore_columns' then do not assert columns.
assert_row_sets(_, {error,_} = Error) ->
    ct:fail(Error);
assert_row_sets({rt_ignore_columns, Expected}, {_, {_, Actual}}) ->
    ct_verify_rows(Expected, Actual);
assert_row_sets({_, {ColExpected, Expected}}, {_, {ColsActual, Actual}}) ->
    ?assertEqual(ColExpected, ColsActual),
    ct_verify_rows(Expected, Actual).

ct_verify_rows(Expected, Actual) ->
    case tdiff:diff(Expected, Actual) of
        [{eq,_}] ->
            pass;
        [] ->
            pass;
        Diff ->
            ct:pal("ROW DIFF~n" ++ format_diff(0, Diff)),
            ct:fail(row_set_mismatch)
    end.

%%
format_diff(EqCount,[]) ->
    format_diff_eq_count(EqCount);
format_diff(EqCount,[{eq,Rows}|Tail]) ->
    format_diff(EqCount+length(Rows), Tail);
format_diff(EqCount,[{Type, Row}|Tail]) ->
    Fmt = io_lib:format("~s~s ~p~n",
        [format_diff_eq_count(EqCount), format_diff_type(Type), Row]),
    [Fmt | format_diff(0, Tail)].

%%
format_diff_type(del) -> "-";
format_diff_type(ins) -> "+".

%%
format_diff_eq_count(0) ->
    "";
format_diff_eq_count(Count) ->
    [lists:duplicate(Count, $.), $\n].


results(Results) ->
    Expected = lists:duplicate(length(Results), pass),
    ?assertEqual(Expected, Results).

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).
