%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015-2106 Basho Technologies, Inc.
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
%% A simple timeseries test to select along the IS [NOT] NULL predicates for
%% all various datatypes.
%%
%% The premise is that if it holds that neither set of IS NULL and IS NOT NULL
%% satisfying result sets are empty, then the count of rows in the sets combined
%% matching the count of rows in the set resulting from querying without the
%% NULL predicates proves that each predicate is correctly implemented.

-module(ts_simple_select_is_null).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(SPANNING_STEP, (1000)).

confirm() ->
    %% will fail if ts_simple_put_all_null_datatypes fails
    DDL =
        "CREATE TABLE GeoCheckin ("
        " myfamily    varchar     not null,"
        " myseries    varchar     not null,"
        " time        timestamp   not null,"
        " myvarchar   varchar             ,"
        " myint       sint64              ,"
        " myfloat     double              ,"
        " mybool      boolean             ,"
        " mytimestamp timestamp           ,"
        " PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')),"
        " myfamily, myseries, time))",
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    N = 11,
    Data = make_data(N, Family, Series, []),
    Cluster = ts_setup:start_cluster(1),
    Table = ts_data:get_default_bucket(),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    Got = ts_ops:put(Cluster, Table, Data),
    ?assertEqual(ok, Got),
    Qry = "SELECT * FROM GeoCheckin WHERE"
          " myfamily='" ++ binary_to_list(Family) ++ "'"
          " AND myseries='" ++ binary_to_list(Series) ++ "'"
          " AND time >= 1000 AND time <= " ++ integer_to_list(N * 1000) ++
          " AND myvarchar IS NULL",
    {ok, {_Fields, Rows}} = ts_ops:query(Cluster, Qry),
    ?assertNotEqual(0, length(Rows)),
    NullableFields = [ "myvarchar", "myint", "myfloat", "mybool", "mytimestamp" ],
    lists:foreach(fun (Field) ->
                query_field(Field, Cluster, Data, Family, Series, N)
        end, NullableFields),
    pass.

query_field(Field, Cluster, Data, Family, Series, N) ->
    RowsAll = query_all(Cluster, Family, Series, N),
    RowsIsNull = query_is_null(Cluster, Data, Family, Series, N, Field),
    RowsIsNotNull = query_is_not_null(Cluster, Data, Family, Series, N, Field),
    ?assertEqual(RowsAll, RowsIsNull + RowsIsNotNull).

query_base(Family, Series, N) ->
    "SELECT * FROM GeoCheckin WHERE"
    " myfamily='" ++ binary_to_list(Family) ++ "'"
    " AND myseries='" ++ binary_to_list(Series) ++ "'"
    " AND time >= 1000 AND time <= " ++ integer_to_list(N * 1000 + ?SPANNING_STEP).

query_all(Cluster, Family, Series, N) ->
    Qry = query_base(Family, Series, N),
    {ok, {_Fields, Rows}} = ts_ops:query(Cluster, Qry),
    RowsN = length(Rows),
    ?assertNotEqual(0, RowsN),
    RowsN.

query_is_null(Cluster, Data, Family, Series, N, Field) ->
    Qry = query_base(Family, Series, N) ++
          " AND " ++ Field ++ " IS NULL",
    {ok, {_Fields, Rows}} = ts_ops:query(Cluster, Qry),
    RowsN = length(Rows),
    %% the number of NULL rows can be determined by any non-key field being NULL
    RowsNull = lists:foldr(fun (El, Acc) ->
                case element(4, El) of
                    [] -> Acc + 1;
                    _ -> Acc
                end
        end, 0, Data),
    ?assertEqual(RowsNull, RowsN),
    RowsN.

query_is_not_null(Cluster, Data, Family, Series, N, Field) ->
    Qry = query_base(Family, Series, N) ++
          " AND " ++ Field ++ " IS NOT NULL",
    {ok, {_Fields, Rows}} = ts_ops:query(Cluster, Qry),
    RowsN = length(Rows),
    %% the number of NULL rows can be determined by any non-key field being NULL
    RowsNotNull = lists:foldr(fun (El, Acc) ->
                case element(4, El) of
                    [] -> Acc;
                    _ -> Acc + 1
                end
        end, 0, Data),
    ?assertEqual(RowsNotNull, RowsN),
    RowsN.

%% insert every even row as null
make_data(0, _, _, Acc) ->
    Acc;
make_data(N, F, S, Acc) when is_integer(N) andalso N rem 2 =:= 1 ->
    NewAcc = {
          F,
          S,
          1 + N * ?SPANNING_STEP,
          <<"">>, %%<< using empty string to stress the difference between NULL and <<"">>
          N,
          N * 1.0,
          N rem 6 =:= 0,
          N
         },
    make_data(N - 1, F, S, [NewAcc | Acc]);
make_data(N, F, S, Acc) when is_integer(N) ->
    NewAcc = {
          F,
          S,
          1 + N * ?SPANNING_STEP,
          [],
          [],
          [],
          [],
          []
         },
    make_data(N - 1, F, S, [NewAcc | Acc]).
