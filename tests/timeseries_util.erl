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
-module(timeseries_util).

-export([
    activate_bucket/2,
    build_cluster/1,
    confirm_activate/2,
    confirm_create/2,
    confirm_get/6,
    confirm_put/4,
    confirm_select/5,
    create_bucket/3,
    exclusive_result_from_data/3,
    get_bool/1,
    get_bucket/0,
    get_cols/1,
    get_ddl/1,
    get_float/0,
    get_integer/0,
    get_invalid_obj/0,
    get_invalid_qry/1,
    get_optional/2,
    get_string/1,
    get_timestamp/0,
    get_valid_obj/0,
    get_valid_qry/0,
    get_valid_qry_spanning_quanta/0,
    get_valid_select_data/0,
    get_valid_select_data_spanning_quanta/0,
    get_varchar/0,
    maybe_stop_a_node/2,
    remove_last/1
]).

-include_lib("eunit/include/eunit.hrl").

-define(MAXVARCHARLEN,       16).
-define(MAXTIMESTAMP,        trunc(math:pow(2, 63))).
-define(MAXFLOAT,            math:pow(2, 63)).
-define(MULTIPLECLUSTERSIZE, 3).

-spec(confirm_create(atom(), string()) -> {ok, string()} | term()).
confirm_create(ClusterType, DDL) ->

    [Node | _] = build_cluster(ClusterType),

    Props = io_lib:format("{\\\"props\\\": {\\\"n_val\\\": 3, \\\"table_def\\\": \\\"~s\\\"}}", [DDL]),
    rt:admin(Node, ["bucket-type", "create", get_bucket(), lists:flatten(Props)]).

confirm_activate(ClusterType, DDL) ->

    [Node | Rest] = build_cluster(ClusterType),
    ok = maybe_stop_a_node(ClusterType, Rest),
    {ok, _} = create_bucket(Node, DDL, 3),
    activate_bucket(Node, DDL).

confirm_put(ClusterType, TestType, DDL, Obj) ->

    [Node | _]  = build_cluster(ClusterType),
    
    case TestType of
        normal ->
            lager:info("1 - Creating and activating bucket"),
            {ok, _} = create_bucket(Node, DDL, 3),
            {ok, _} = activate_bucket(Node, DDL);
        no_ddl ->
            lager:info("1 - NOT Creating or activating bucket - failure test"),
            ok
    end,
    Bucket = list_to_binary(get_bucket()),
    lager:info("2 - writing to bucket ~p with:~n- ~p", [Bucket, Obj]),
    C = rt:pbc(Node),
    riakc_ts:put(C, Bucket, Obj).

confirm_get(ClusterType, TestType, DDL, Data, Key, Options) ->

    [Node | _]  = build_cluster(ClusterType),

    case TestType of
        normal ->
            lager:info("1 - Creating and activating bucket"),
            {ok, _} = create_bucket(Node, DDL, 3),
            {ok, _} = activate_bucket(Node, DDL);
        n_val_one ->
            lager:info("1 - Creating and activating bucket"),
            {ok, _} = create_bucket(Node, DDL, 1),
            {ok, _} = activate_bucket(Node, DDL);
        no_ddl ->
            lager:info("1 - NOT Creating or activating bucket - failure test"),
            ok
    end,
    Bucket = list_to_binary(get_bucket()),
    lager:info("2 - writing to bucket ~p with:~n- ~p", [Bucket, Data]),
    C = rt:pbc(Node),
    ok = riakc_ts:put(C, Bucket, Data),

    lager:info("3 - reading from bucket ~p with key ~p", [Bucket, Key]),
    riakc_ts:get(C, Bucket, Key, Options).

confirm_select(ClusterType, TestType, DDL, Data, Qry) ->
    
    [Node | _] = build_cluster(ClusterType),
    
    case TestType of
        normal ->
            lager:info("1 - Create and activate the bucket"),
            {ok, _} = create_bucket(Node, DDL, 3),
            {ok, _} = activate_bucket(Node, DDL);
        n_val_one ->
            lager:info("1 - Creating and activating bucket"),
            {ok, _} = create_bucket(Node, DDL, 1),
            {ok, _} = activate_bucket(Node, DDL);
        no_ddl ->
            lager:info("1 - NOT Creating or activating bucket - failure test"),
            ok
    end,
    
    Bucket = list_to_binary(get_bucket()),
    lager:info("2 - writing to bucket ~p with:~n- ~p", [Bucket, Data]),
    C = rt:pbc(Node),
    ok = riakc_ts:put(C, Bucket, Data),
    
    lager:info("3 - Now run the query ~p", [Qry]),
    Got = riakc_ts:query(C, Qry), 
    lager:info("Result is ~p", [Got]),
    Got.

%%
%% Helper funs
%%

activate_bucket(Node, _DDL) ->
    rt:admin(Node, ["bucket-type", "activate", get_bucket()]).

create_bucket(Node, DDL, NVal) when is_integer(NVal) ->
    Props = io_lib:format("{\\\"props\\\": {\\\"n_val\\\": " ++ 
                          integer_to_list(NVal) ++
                          ", \\\"table_def\\\": \\\"~s\\\"}}", [DDL]),
    rt:admin(Node, ["bucket-type", "create", get_bucket(),
                    lists:flatten(Props)]).

%% @ignore
maybe_stop_a_node(one_down, [H | _T]) ->
    ok = rt:stop(H);
maybe_stop_a_node(_, _) ->
    ok.

build_cluster(single)   -> build_c2(1);
build_cluster(multiple) -> build_c2(?MULTIPLECLUSTERSIZE);
build_cluster(one_down)   -> build_c2(?MULTIPLECLUSTERSIZE).

-spec build_c2(non_neg_integer()) -> [node()].
build_c2(Size) ->
    lager:info("Building cluster of ~p~n", [Size]),
    build_c2(Size, []).
-spec build_c2(non_neg_integer(), list()) -> [node()].
build_c2(Size, Config) ->
    rt:set_backend(eleveldb),
    rt:build_cluster(Size, Config).

%% This is also the name of the table
get_bucket() ->
    "GeoCheckin".

get_valid_qry() ->
    "select * from GeoCheckin Where time > 1 and time < 10 and myfamily = 'family1' and myseries ='seriesX'".

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
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    Times = lists:seq(1, 10),
    [[Family, Series, X, get_varchar(), get_float()] || X <- Times].


-define(SPANNING_STEP, (1000*60*5)).

get_valid_qry_spanning_quanta() ->
    StartTime = 1 + ?SPANNING_STEP *  1,
    EndTime   = 1 + ?SPANNING_STEP * 10,
    lists:flatten(
      io_lib:format("select * from GeoCheckin Where time > ~b and time < ~b"
                    " and myfamily = 'family1' and myseries = 'seriesX'",
                    [StartTime, EndTime])).

get_valid_select_data_spanning_quanta() ->
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    Times = lists:seq(1 + ?SPANNING_STEP, 1 + ?SPANNING_STEP * 10, ?SPANNING_STEP),  %% five-minute intervals, to span 15-min buckets
    [[Family, Series, X, get_varchar(), get_float()] || X <- Times].

get_cols(docs) ->
    [<<"myfamily">>,
     <<"myseries">>,
     <<"time">>,
     <<"weather">>,
     <<"temperature">>].

exclusive_result_from_data(Data, Start, Finish) when is_integer(Start)   andalso
                                                     is_integer(Finish)  andalso
                                                     Start  > 0          andalso
                                                     Finish > 0          andalso
                                                     Finish > Start ->
    [list_to_tuple(X) || X <- lists:sublist(Data, Start, Finish - Start + 1)].

remove_last(Data) ->
    lists:reverse(tl(lists:reverse(Data))).

%% a valid DDL - the one used in the documents
get_ddl(docs) ->
    _SQL = "CREATE TABLE GeoCheckin (" ++
    "myfamily    varchar   not null, " ++
    "myseries    varchar   not null, " ++
    "time        timestamp not null, " ++
    "weather     varchar   not null, " ++
    "temperature double, " ++
    "PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')), " ++
    "myfamily, myseries, time))";
%% another valid DDL - one with all the good stuff like
%% different types and optional blah-blah
get_ddl(variety) ->
    _SQL = "CREATE TABLE GeoCheckin (" ++
    "myfamily    varchar     not null, " ++
    "myseries    varchar     not null, " ++
    "time        timestamp   not null, " ++
    "myint       sint64      not null, " ++
    "myfloat     double      not null, " ++
    "mybool      boolean     not null, " ++
    "mytimestamp timestamp   not null, " ++
    "myany       any         not null, " ++
    "myoptional  sint64, " ++
    "PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')), " ++
    "myfamily, myseries, time))";
%% an invalid TS DDL becuz family and series not both in key
get_ddl(shortkey_fail) ->
    _SQL = "CREATE TABLE GeoCheckin (" ++
    "myfamily    varchar   not null, " ++
    "myseries    varchar   not null, " ++
    "time        timestamp not null, " ++
    "weather     varchar   not null, " ++
    "temperature double, " ++
    "PRIMARY KEY ((quantum(time, 15, 'm'), myfamily), " ++
    "time, myfamily))";
%% an invalid TS DDL becuz partition and local keys dont cover the same space
get_ddl(splitkey_fail) ->
    _SQL = "CREATE TABLE GeoCheckin (" ++
    "myfamily    varchar   not null, " ++
    "myseries    varchar   not null, " ++
    "time        timestamp not null, " ++
    "weather     varchar   not null, " ++
    "temperature double, " ++
    "PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')), " ++
    "time, myfamily, myseries, temperature))".

get_valid_obj() ->
    [get_varchar(),
     get_varchar(),
     get_timestamp(),
     get_varchar(),
     get_float()].

get_invalid_obj() ->
    [get_varchar(),
     get_integer(),   % this is the duff field
     get_timestamp(),
     get_varchar(),
     get_float()].

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
