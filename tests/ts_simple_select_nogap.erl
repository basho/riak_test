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

%% 1.4.0 shipped with a exception on selecting at quantum boundaries
%% that was exacerbated by the new time parsing and
%% not-entirely-intuitive handling of reduced accuracy times.
%%
%% select * from foo where time > '2016-01-01 05' and time <
%% '2016-01-01 06' e.g. seems like a reasonable query but in effect
%% both time values are the same.

-module(ts_simple_select_nogap).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    Cluster = ts_setup:start_cluster(1),
    %% First try quantum boundaries. 135309600000 is a boundary value
    %% for a 15 minute quantum
    Base = 135309599999,
    try_gap(Cluster, Base, Base),
    try_gap(Cluster, Base, Base + 1),
    try_gap(Cluster, Base + 1, Base + 1),
    try_gap(Cluster, Base + 1, Base + 2),
    try_gap(Cluster, Base + 2, Base + 2),

    %% Now try internal (to a quantum) values
    try_gap(Cluster, 5, 5),
    try_gap(Cluster, 5, 6).

try_gap(Cluster, Lower, Upper) ->
    Table = ts_data:get_default_bucket(),
    DDL = ts_data:get_ddl(),
    Qry = ts_data:flat_format(
        "select * from GeoCheckin "
        "where time > ~B and time < ~B "
        "and myfamily = 'family1' "
        "and myseries ='seriesX' ",
        [Lower, Upper]),
    Expected =
        {error,
            {1001,
                <<"boundaries are equal or adjacent">>}},

    Cluster = ts_setup:start_cluster(1),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    Got = ts_ops:query(Cluster, Qry),
    ts_data:assert_error_regex("No gap between times", Expected, Got).

