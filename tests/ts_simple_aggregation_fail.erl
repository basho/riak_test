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
    DDL = ts_data:get_ddl(big),
    Count = 10,
    Data = ts_data:get_valid_big_data(Count),
    Bucket = ts_data:get_default_bucket(),
    Qry = "SELECT SUM(mybool) FROM " ++ Bucket,
    Cluster = ts_setup:start_cluster(1),
    {ok,_} = ts_setup:create_bucket_type(Cluster, DDL, Bucket),
    ok = ts_setup:activate_bucket_type(Cluster, Bucket),
    ok = ts_ops:put(Cluster, Bucket, Data),

    Got1 = ts_ops:query(Cluster, Qry),
    Expected1 = {error, {1001, <<".*Function 'SUM' called with arguments of the wrong type [[]boolean[]].*">>}},
    Result1 = ts_data:assert_error_regex("SUM - boolean", Expected1, Got1),

    Qry2 = "SELECT AVG(myfamily) FROM " ++ Bucket,
    Got2 = ts_ops:query(Cluster, Qry2),
    Expected2 = {error, {1001, <<".*Function 'AVG' called with arguments of the wrong type [[]varchar[]].*">>}},
    Result2 = ts_data:assert_error_regex("AVG - varchar", Expected2, Got2),

    Qry3 = "SELECT MIN(myseries) FROM " ++ Bucket,
    Got3 = ts_ops:query(Cluster, Qry3),
    Expected3 = {error, {1001, <<".*Function 'MIN' called with arguments of the wrong type [[]varchar[]].*">>}},
    Result3 = ts_data:assert_error_regex("MIN - varchar", Expected3, Got3),

    Qry4 = "SELECT MAX(myseries) FROM " ++ Bucket,
    Got4 = ts_ops:query(Cluster, Qry4),
    Expected4 = {error, {1001, <<".*Function 'MAX' called with arguments of the wrong type [[]varchar[]].*">>}},
    Result4 = ts_data:assert_error_regex("MIN - varchar", Expected4, Got4),

    Qry5 = "SELECT STDDEV(mybool) FROM " ++ Bucket,
    Got5 = ts_ops:query(Cluster, Qry5),
    Expected5 = {error, {1001, <<".*Function 'STDDEV_SAMP' called with arguments of the wrong type [[]boolean[]].*">>}},
    Result5 = ts_data:assert_error_regex("STDDEV - boolean", Expected5, Got5),

    Qry6 = "SELECT STDDEV_SAMP(mybool) FROM " ++ Bucket,
    Got6 = ts_ops:query(Cluster, Qry6),
    Expected6 = {error, {1001, <<".*Function 'STDDEV_SAMP' called with arguments of the wrong type [[]boolean[]].*">>}},
    Result6 = ts_data:assert_error_regex("STDDEV_SAMP - boolean", Expected6, Got6),

    Qry7 = "SELECT Mean(mybool) FROM " ++ Bucket,
    Got7 = ts_ops:query(Cluster, Qry7),
    Expected7 = {error, {1001, <<".*Function 'AVG' called with arguments of the wrong type [[]boolean[]].*">>}},
    Result7 = ts_data:assert_error_regex("MEAN - boolean", Expected7, Got7),

    ts_data:results([
             Result1,
             Result2,
             Result3,
             Result4,
             Result5,
             Result6,
             Result7
            ]),

    pass.
