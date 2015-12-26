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

-module(ts_A_create_table_via_sql).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    [Node] = Cluster = ts_util:build_cluster(single),
    C = rt:pbc(Node),

    DDL = ts_util:get_ddl(),
    Table = ts_util:get_default_bucket(),

    ok = confirm_create(C, DDL),
    ok = confirm_activate(Cluster, Table),
    ok = confirm_exists(C, Table),
    ok = confirm_get(C, Table),
    pass.

confirm_create(C, DDL) ->
    Expected = {[],[]},
    Got = riakc_ts:query(C, DDL),
    ?assertEqual(Expected, Got),
    io:format("Created table via query:\n  ~s\n", [DDL]),
    ok.

confirm_activate(Cluster, Table) ->
    Result = ts_util:activate_bucket_type(Cluster, Table, _Retries = 1),
    ?assertMatch({ok, _}, Result),
    ok.

confirm_exists(C, Table) ->
    Qry = "DESCRIBE " ++ Table,
    Got = ts_util:single_query(C, Qry),
    Expected =
        {[<<"Column">>,<<"Type">>,<<"Is Null">>,<<"Primary Key">>, <<"Local Key">>],
         [{<<"myfamily">>,  <<"varchar">>,   false,  1,  1},
          {<<"myseries">>,   <<"varchar">>,   false,  2,  2},
          {<<"time">>,       <<"timestamp">>, false,  3,  3},
          {<<"weather">>,    <<"varchar">>,   false, [], []},
          {<<"temperature">>,<<"double">>,    true,  [], []}]},
    ?assertEqual(Expected, Got),
    io:format("DESCRIBE ~s:\n~p\n", [Table, Got]),
    ok.

confirm_get(C, Table) ->
    Data = [[<<"a">>, <<"b">>, 10101010, <<"not bad">>, 42.24]],
    Key = [<<"a">>, <<"b">>, 10101010],
    ?assertMatch(ok, riakc_ts:put(C, Table, Data)),
    ?assertMatch({ok, {_, Data}}, riakc_ts:get(C, Table, Key, [])),
    io:format("Put a record, got it back\n", []),
    ok.
