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

%%% Execute a query where the primary key is not covered
%%% in the where clause.

-module(ts_A_select_missing_field_in_pk_not_allowed).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    DDL = ts_util:get_ddl(),
    Data = ts_util:get_valid_select_data(),
    % query with missing myfamily field
    Query =
        "select * from GeoCheckin "
        "where time > 1 and time < 10",
    Expected =
        {error,
         {1001,
          <<"missing_param: Missing parameter myfamily in where clause.">>}},
    Got = ts_util:ts_query(
            ts_util:cluster_and_connect(single), normal, DDL, Data, Query),
    ?assertEqual(Expected, Got),
    pass.
