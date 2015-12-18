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

-module(ts_aggregation_fail).

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
    Got = ts_util:ts_query(ClusterConn, TestType, DDL, Data, Qry, Bucket),
    ?assertEqual(Got, some_error_value),

    Qry2 = "SELECT AVG(myfamily) FROM " ++ Bucket,
    Got2 = ts_util:single_query(Conn, Qry2),
    ?assertEqual(Got2, some_error_value),

    Qry3 = "SELECT MIN(myseries) FROM " ++ Bucket,
    Got3 = ts_util:single_query(Conn, Qry3),
    ?assertEqual(Got3, some_error_value),

    Qry4 = "SELECT MAX(mytimestamp) FROM " ++ Bucket,
    Got4 = ts_util:single_query(Conn, Qry4),
    ?assertEqual(Got4, some_error_value),

    Qry5 = "SELECT STDDEV(mybool) FROM " ++ Bucket,
    Got5 = ts_util:single_query(Conn, Qry5),
    ?assertEqual(Got5, some_error_value),
    pass.
