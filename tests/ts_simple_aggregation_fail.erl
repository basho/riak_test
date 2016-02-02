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

-module(ts_simple_aggregation_fail).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

% Ensure aggregation functions only work on desired data types

confirm() ->
    DDL = ts_util:get_ddl(big),
    Count = 10,
    Data = ts_util:get_valid_big_data(Count),
    TestType = normal,
    Bucket = "GeoCheckin",

    Qry = "SELECT SUM(mybool) FROM " ++ Bucket,
    ClusterConn = {_Cluster, Conn} = ts_util:cluster_and_connect(single),
    Got1 = ts_util:ts_query(ClusterConn, TestType, DDL, Data, Qry, Bucket),
    Expected1 = {error, {1001, <<"invalid_query: \nFunction 'SUM'/1 called with arguments of the wrong type [boolean].">>}},
    Result1 = ts_util:assert("SUM - boolean", Expected1, Got1),

    Qry2 = "SELECT AVG(myfamily) FROM " ++ Bucket,
    Got2 = ts_util:single_query(Conn, Qry2),
    Expected2 = {error, {1001, <<"invalid_query: \nFunction 'AVG'/1 called with arguments of the wrong type [varchar].">>}},
    Result2 = ts_util:assert("AVG - varchar", Expected2, Got2),

    Qry3 = "SELECT MIN(myseries) FROM " ++ Bucket,
    Got3 = ts_util:single_query(Conn, Qry3),
    Expected3 = {error, {1001, <<"invalid_query: \nFunction 'MIN'/1 called with arguments of the wrong type [varchar].">>}},
    Result3 = ts_util:assert("MIN - varchar", Expected3, Got3),

    Qry4 = "SELECT MAX(myseries) FROM " ++ Bucket,
    Got4 = ts_util:single_query(Conn, Qry4),
    Expected4 = {error, {1001, <<"invalid_query: \nFunction 'MAX'/1 called with arguments of the wrong type [varchar].">>}},
    Result4 = ts_util:assert("MIN - varchar", Expected4, Got4),

    Qry5 = "SELECT STDDEV(mybool) FROM " ++ Bucket,
    Got5 = ts_util:single_query(Conn, Qry5),
    Expected5 = {error, {1001, <<"invalid_query: \nFunction 'STDDEV_SAMP'/1 called with arguments of the wrong type [boolean].">>}},
    Result5 = ts_util:assert("STDDEV - boolean", Expected5, Got5),

    Qry6 = "SELECT STDDEV_SAMP(mybool) FROM " ++ Bucket,
    Got6 = ts_util:single_query(Conn, Qry6),
    Expected6 = {error, {1001, <<"invalid_query: \nFunction 'STDDEV_SAMP'/1 called with arguments of the wrong type [boolean].">>}},
    Result6 = ts_util:assert("STDDEV_SAMP - boolean", Expected6, Got6),

    Qry7 = "SELECT STDDEV_POP(time) FROM " ++ Bucket,
    Got7 = ts_util:single_query(Conn, Qry7),
    Expected7 = {error, {1001, <<"invalid_query: \nFunction 'STDDEV_POP'/1 called with arguments of the wrong type [timestamp].">>}},
    Result7 = ts_util:assert("STDDEV_POP - timestamp", Expected7, Got7),

    Qry8 = "SELECT Mean(mybool) FROM " ++ Bucket,
    Got8 = ts_util:single_query(Conn, Qry8),
    Expected8 = {error, {1001, <<"invalid_query: \nFunction 'AVG'/1 called with arguments of the wrong type [boolean].">>}},
    Result8 = ts_util:assert("MEAN - boolean", Expected8, Got8),

    ts_util:results([
             Result1,
             Result2,
             Result3,
             Result4,
             Result5,
             Result6,
             Result7,
             Result8
            ]),

    pass.
