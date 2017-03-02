%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(ts_simple_blob).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

get_ddl(Table) ->
    "CREATE TABLE " ++ Table ++ " ("
    " name        varchar   not null,"
    " firmware    blob   not null,"
    " version     sint64 not null,"
    " time        timestamp not null,"
    " weather     varchar,"
    " PRIMARY KEY ((name, firmware, version, quantum(time, 15, 'm')), "
    " name, firmware, version, time))".

confirm() ->
    Table = "MyBlob",
    DDL = get_ddl(Table),
    Data = [{<<"fred">>, <<0, 1, 2, 34>>, 5, 1, <<"rainy">>},
            {<<"fred">>, <<0, 1, 2, 34>>, 5, 3, <<"snowy">>},
            {<<"fred">>, <<0, 1, 2, 34>>, 5, 9, <<"generically wet">>}],
    Headers = [{<<"name">>, varchar},
               {<<"firmware">>, blob},
               {<<"version">>, sint64},
               {<<"time">>, timestamp},
               {<<"weather">>, varchar}],

    Qry = "select * from " ++ Table ++ " where time > 0 and time < 50 and name = 'fred' and version = 5 and firmware = 0x00010222",
    Expected =
        {ok, {Headers, Data}},

    Cluster = ts_setup:start_cluster(1),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    ts_ops:put(Cluster, Table, Data),
    Got = ts_ops:query(Cluster, Qry, [{datatypes, true}]),
    ?assertEqual(Expected, Got),
    pass.
