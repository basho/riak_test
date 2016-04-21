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

-module(ts_simple_select_where_has_no_upper_bounds_not_allowed).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    TestType = normal,
    DDL = ts_util:get_ddl(),
    Data = [],
    Qry = "select * from GeoCheckin "
          "where time > 10 "
          "and myfamily = 'family1' "
          "and myseries ='seriesX' ",
    Expected =
        {error, {1001, <<"incomplete_where_clause: Where clause has no upper bound.">>}},
    Got = ts_util:ts_query(
            ts_util:cluster_and_connect(single), TestType, DDL, Data, Qry),
    ?assertEqual(Expected, Got),
    pass.

