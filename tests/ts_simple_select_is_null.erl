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
-define(TEST_TYPE, normal).

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
    N = 10,
    Data = make_data(N, Family, Series, []),
    ClusterConn = ts_util:cluster_and_connect(single),
    Got = ts_util:ts_put(ClusterConn, ?TEST_TYPE, DDL, Data),
    ?assertEqual(ok, Got),
    Qry = "SELECT * FROM GeoCheckin WHERE"
          " myfamily='" ++ binary_to_list(Family) ++ "'"
          " AND myseries='" ++ binary_to_list(Series) ++ "'"
          " AND time >= 1000 AND time <= " ++ integer_to_list(N * 1000) ++
          " AND myvarchar IS NULL",
    {ok, {_Fields, Rows}} = ts_util:ts_query(ClusterConn, ?TEST_TYPE, DDL, Data, Qry),
    ?assertNotEqual(0, length(Rows)),
    NullableFields = [ "myvarchar", "myint", "myfloat", "mybool", "mytimestamp" ],
    lists:foreach(fun (Field) ->
                query_field(Field, ClusterConn, DDL, Data, Family, Series, N)
        end, NullableFields),
    pass.

query_field(Field, ClusterConn, DDL, Data, Family, Series, N) ->
    RowsAll = query_all(ClusterConn, DDL, Data, Family, Series, N),
    RowsIsNull = query_is_null(ClusterConn, DDL, Data, Family, Series, N, Field),
    RowsIsNotNull = query_is_not_null(ClusterConn, DDL, Data, Family, Series, N, Field),
    ?assertEqual(RowsAll, RowsIsNull + RowsIsNotNull).

query_base(Family, Series, N) ->
    "SELECT * FROM GeoCheckin WHERE"
    " myfamily='" ++ binary_to_list(Family) ++ "'"
    " AND myseries='" ++ binary_to_list(Series) ++ "'"
    " AND time >= 1000 AND time <= " ++ integer_to_list(N * 1000).

query_all(ClusterConn, DDL, Data, Family, Series, N) ->
    Qry = query_base(Family, Series, N),
    {ok, {_Fields, Rows}} = ts_util:ts_query(ClusterConn, ?TEST_TYPE, DDL, Data, Qry),
    RowsN = length(Rows),
    ?assertNotEqual(0, RowsN),
    RowsN.

query_is_null(ClusterConn, DDL, Data, Family, Series, N, Field) ->
    Qry = query_base(Family, Series, N) ++
          " AND " ++ Field ++ " IS NULL",
    {ok, {_Fields, Rows}} = ts_util:ts_query(ClusterConn, ?TEST_TYPE, DDL, Data, Qry),
    RowsN = length(Rows),
    ?assertNotEqual(0, RowsN),
    RowsN.

query_is_not_null(ClusterConn, DDL, Data, Family, Series, N, Field) ->
    Qry = query_base(Family, Series, N) ++
          " AND " ++ Field ++ " IS NOT NULL",
    {ok, {_Fields, Rows}} = ts_util:ts_query(ClusterConn, ?TEST_TYPE, DDL, Data, Qry),
    RowsN = length(Rows),
    ?assertNotEqual(0, RowsN),
    RowsN.

%% insert every even row as null
make_data(0, _, _, Acc) ->
    Acc;
make_data(N, F, S, Acc) when is_integer(N) andalso N rem 2 =:= 1 ->
    NewAcc = {
          F,
          S,
          1 + N * ?SPANNING_STEP,
          list_to_binary("test" ++ integer_to_list(N)),
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
