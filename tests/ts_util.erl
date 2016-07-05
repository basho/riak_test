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
%% @doc A util module for riak_ts basic CREATE TABLE Actions
-module(ts_util).

-export([
    activate_bucket_type/2,
    activate_bucket_type/3,
    assert/3,
    assert_error_regex/3,
    assert_float/3,
    assert_row_sets/2,
    build_cluster/1,
    cluster_and_connect/1,
    create_and_activate_bucket_type/2,
    create_and_activate_bucket_type/3,
    create_and_activate_bucket_type/4,
    create_bucket_type/2,
    create_bucket_type/3,
    create_bucket_type/4,
    create_table/4,
    exclusive_result_from_data/3,
    flat_format/2,
    get_bool/1,
    get_cols/0, get_cols/1,
    get_data/1,
    get_ddl/0, get_ddl/1, get_ddl/2,
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
    maybe_stop_a_node/2,
    remove_last/1,
    results/1,
    single_query/2,
    ts_get/6,
    ts_get/7,
    ts_insert/4,
    ts_insert_no_columns/3,
    ts_put/4,
    ts_put/5,
    ts_query/5,
    ts_query/6
]).

-include_lib("eunit/include/eunit.hrl").

-define(MAXVARCHARLEN,       16).
-define(MAXTIMESTAMP,        trunc(math:pow(2, 63))).
-define(MAXFLOAT,            math:pow(2, 63)).
-define(MULTIPLECLUSTERSIZE, 3).

ts_put(ClusterConn, TestType, DDL, Obj) ->
    Bucket = get_default_bucket(),
    ts_put(ClusterConn, TestType, DDL, Obj, Bucket).
ts_put({Cluster, Conn}, TestType, DDL, Obj, Bucket) ->

    create_table(TestType, Cluster, DDL, Bucket),
    lager:info("2 - writing to bucket ~ts with:~n- ~p", [Bucket, Obj]),
    riakc_ts:put(Conn, Bucket, Obj).

ts_get(ClusterConn, TestType, DDL, Obj, Key, Options) ->
    Bucket = get_default_bucket(),
    ts_get(ClusterConn, TestType, DDL, Obj, Key, Options, Bucket).
ts_get({Cluster, Conn}, TestType, DDL, Obj, Key, Options, Bucket) ->

    create_table(TestType, Cluster, DDL, Bucket),
    lager:info("2 - writing to bucket ~ts with:~n- ~p", [Bucket, Obj]),
    ok = riakc_ts:put(Conn, Bucket, Obj),

    lager:info("3 - reading from bucket ~ts with key ~p", [Bucket, Key]),
    riakc_ts:get(Conn, Bucket, Key, Options).

ts_query(ClusterConn, TestType, DDL, Data, Qry) ->
    Bucket = get_default_bucket(),
    ts_query(ClusterConn, TestType, DDL, Data, Qry, Bucket).
ts_query({Cluster, Conn}, TestType, DDL, Data, Qry, Bucket) ->

    create_table(TestType, Cluster, DDL, Bucket),

    lager:info("2 - writing to bucket ~ts with:~n- ~p", [Bucket, Data]),
    ok = riakc_ts:put(Conn, Bucket, Data),

    single_query(Conn, Qry).

single_query(Conn, Qry) ->
    lager:info("3 - Now run the query ~ts", [Qry]),
    Got = riakc_ts:query(Conn, Qry),
    lager:info("Result is ~p", [Got]),
    Got.

insert_term_format(Data, Acc) when is_binary(Data) ->
    Acc ++ flat_format("'~s',", [Data]);
insert_term_format(Data, Acc) ->
    Acc ++ flat_format("~p,", [Data]).

ts_insert(Conn, Table, Columns, Data) ->
    ColFn = fun(Col, Acc) ->
        Acc ++ flat_format("~s,", [Col])
        end,
    TermFn = fun insert_term_format/2,
    ColClause = string:strip(lists:foldl(ColFn, [], Columns), right, $,),
    ValClause = string:strip(lists:foldl(TermFn, [], tuple_to_list(Data)), right, $,),
    SQL = flat_format("INSERT INTO ~s (~s) VALUES (~s)",
                      [Table, ColClause, ValClause]),
    lager:info("~ts", [SQL]),
    Got = riakc_ts:query(Conn, SQL),
    lager:info("Result is ~p", [Got]),
    Got.

ts_insert_no_columns(Conn, Table, Data) ->
    TermFn = fun insert_term_format/2,
    ValClause = string:strip(lists:foldl(TermFn, [], tuple_to_list(Data)), right, $,),
    SQL = flat_format("INSERT INTO ~s VALUES (~s)",
        [Table, ValClause]),
    lager:info("~ts", [SQL]),
    Got = riakc_ts:query(Conn, SQL),
    lager:info("Result is ~p", [Got]),
    Got.

%%
%% Table and Bucket Type Management
%%

-spec(create_table(normal|n_val_one|no_ddl, [node()], string(), string()) -> ok).
create_table(normal, Cluster, DDL, Bucket) ->
    lager:info("1 - Create and activate the bucket"),
    lager:debug("DDL = ~ts", [DDL]),
    create_and_activate_bucket_type(Cluster, DDL, Bucket);
create_table(n_val_one, Cluster, DDL, Bucket) ->
    lager:info("1 - Creating and activating bucket"),
    lager:debug("DDL = ~ts", [DDL]),
    create_and_activate_bucket_type(Cluster, DDL, Bucket);
create_table(no_ddl, _Cluster, _DDL, _Bucket) ->
    lager:info("1 - NOT Creating or activating bucket - failure test"),
    ok.

-spec(create_bucket_type([node()], string()) -> {ok, term()} | term()).
create_bucket_type(Cluster, DDL) ->
    create_bucket_type(Cluster, DDL, get_default_bucket()).
-spec(create_bucket_type(node()|{[node()],term()}, string(), string()|non_neg_integer()) -> {ok, term()} | term()).
create_bucket_type({Cluster, _Conn}, DDL, Bucket) ->
    create_bucket_type(Cluster, DDL, Bucket);
create_bucket_type(Cluster, DDL, Bucket) ->
    NVal = length(Cluster),
    create_bucket_type(Cluster, DDL, Bucket, NVal).

-spec(create_bucket_type(node()|{[node()],term()}, string(), string(), non_neg_integer()) -> {ok, term()} | term()).
create_bucket_type([Node|_Rest], DDL, Bucket, NVal) when is_integer(NVal) ->
    Props = io_lib:format("{\\\"props\\\": {\\\"n_val\\\": ~s, \\\"table_def\\\": \\\"~s\\\"}}", [integer_to_list(NVal), DDL]),
    rt:admin(Node, ["bucket-type", "create", bucket_to_list(Bucket), lists:flatten(Props)]).

-spec(activate_bucket_type([node()], string()) -> {ok, string()} | term()).
activate_bucket_type(Cluster, Bucket) ->
    activate_bucket_type(Cluster, Bucket, 3).
%% Attempt to activate the bucket type 4 times
activate_bucket_type(Cluster, Bucket, Retries) ->
    [Node|_Rest] = Cluster,
    {ok, Msg} = Result = rt:admin(Node, ["bucket-type", "activate", bucket_to_list(Bucket)]),
    %% Look for a successful message
    case string:str(Msg, "has been activated") of
        0 ->
            lager:error("Could not activate bucket type. Retrying. Result = ~p", [Result]),
            case Retries of
                0 -> Result;
                _ -> timer:sleep(timer:seconds(1)),
                     activate_bucket_type(Cluster, Bucket, Retries-1)
            end;
        _ -> Result
    end.

-spec(create_and_activate_bucket_type([node()]|{[node()],term()}, string()) -> term()).
create_and_activate_bucket_type({Cluster, _Conn}, DDL) ->
    create_and_activate_bucket_type(Cluster, DDL);
create_and_activate_bucket_type(Cluster, DDL) ->
    create_and_activate_bucket_type(Cluster, DDL, get_default_bucket()).

-spec(create_and_activate_bucket_type({[node()],term()}, string(), string()) -> term()).
create_and_activate_bucket_type({Cluster, _Conn}, DDL, Bucket) ->
    create_and_activate_bucket_type(Cluster, DDL, Bucket);
create_and_activate_bucket_type(Cluster, DDL, Bucket)->
    {ok, _} = create_bucket_type(Cluster, DDL, Bucket),
    activate_bucket_type(Cluster, Bucket).
-spec(create_and_activate_bucket_type({[node()],term()}, string(), string(), non_neg_integer()) -> term()).
create_and_activate_bucket_type({Cluster, _Conn}, DDL, Bucket, NVal) ->
    {ok, _} = create_bucket_type(Cluster, DDL, Bucket, NVal),
    activate_bucket_type(Cluster, Bucket).

bucket_to_list(Bucket) when is_binary(Bucket) ->
    binary_to_list(Bucket);
bucket_to_list(Bucket) ->
    Bucket.

%% @ignore
maybe_stop_a_node(delayed_one_down, Cluster) ->
    maybe_stop_a_node(one_down, Cluster);
maybe_stop_a_node(one_down, Cluster) ->
    %% Shutdown the second node, since we connect to the first one
    ok = rt:stop(hd(tl(Cluster)));
maybe_stop_a_node(_, _) ->
    ok.

build_cluster(single)   -> build_c2(1, all_up);
build_cluster(multiple) -> build_c2(?MULTIPLECLUSTERSIZE, all_up);
build_cluster(one_down) -> build_c2(?MULTIPLECLUSTERSIZE, one_down);
build_cluster(delayed_one_down) -> build_c2(?MULTIPLECLUSTERSIZE, all_up).

%% Build a cluster and create a PBC connection to the first node
-spec cluster_and_connect(single|multiple|one_down) -> {[node()], term()}.
cluster_and_connect(ClusterType) ->
    Cluster = [Node|_Rest] = build_cluster(ClusterType),
    Conn = rt:pbc(Node),
    ?assert(is_pid(Conn)),
    {Cluster, Conn}.
%% Just build cluster and stop a node, if necessary
-spec build_c2(non_neg_integer(), all_up|one_down) -> [node()].
build_c2(Size, ClusterType) ->
    lager:info("Building cluster of ~p~n", [Size]),
    build_c2(Size, ClusterType, []).
-spec build_c2(non_neg_integer(), all_up|one_down, list()) -> {[node()], term()}.
build_c2(Size, ClusterType, Config) ->
    rt:set_backend(eleveldb),
    [_Node|Rest] = Cluster = rt:build_cluster(Size, Config),
    maybe_stop_a_node(ClusterType, Rest),
    Cluster.


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
    "CREATE TABLE " ++ Table ++ " ("
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
    "CREATE TABLE " ++ Table ++ " ("
    " myfamily    varchar     not null,"
    " myseries    varchar     not null,"
    " time        timestamp   not null,"
    " myint       sint64      not null,"
    " myfloat     double      not null,"
    " mybool      boolean     not null,"
    " mytimestamp timestamp   not null,"
    " myoptional  sint64,"
    " PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')),"
    " myfamily, myseries, time))";

%% the other DDLs which used to be here but were only used once, each
%% in a corresponding ts_A_create_*_fail module, have been moved to
%% those respective modules

get_ddl(api, Table) ->
    "CREATE TABLE " ++ Table ++ " ("
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
    "CREATE TABLE " ++ Table ++ " ("
    " myfamily      varchar   not null,"
    " myseries      varchar   not null,"
    " time          timestamp not null,"
    " temperature   double,"
    " pressure      double,"
    " precipitation double,"
    " PRIMARY KEY ((myfamily, myseries, quantum(time, 10, 'm')), "
    " myfamily, myseries, time))".


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
    get_s(N - 1, [random:uniform(255) | Acc]).

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

assert_float(String, {_, {Cols, [ValsA]}} = Exp, {_, {Cols, [ValsB]}} = Got) ->
    case assertf2(tuple_to_list(ValsA), tuple_to_list(ValsB)) of
        fail -> lager:info("*****************", []),
            lager:info("Test ~p failed", [String]),
            lager:info("Exp ~p", [Exp]),
            lager:info("Got ~p", [Got]),
            lager:info("*****************", []),
            fail;
        pass -> pass
    end;
assert_float(String, Exp, Got) -> assert(String, Exp, Got).

assertf2([], []) -> pass;
assertf2([H1 | T1], [H2 | T2]) ->
    Diff = H1 - H2,
    Av = (H1 + H2)/2,
    if Diff/Av > ?DELTA -> fail;
        el/=se          -> assertf2(T1, T2)
    end.

assert(_,      X,   X)   -> pass;
assert(String, Exp, Got) -> lager:info("*****************", []),
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
