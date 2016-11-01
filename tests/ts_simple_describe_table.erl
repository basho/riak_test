%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015-2016 Basho Technologies, Inc.
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

-module(ts_simple_describe_table).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

%% Test basic table description

confirm() ->
    Table = ts_data:get_default_bucket(),
    DDL = ts_data:get_ddl(),
    Qry = "DESCRIBE " ++ Table,
    Expected =
        {ok, {[<<"Column">>,<<"Type">>,<<"Is Null">>,<<"Primary Key">>, <<"Local Key">>, <<"Interval">>, <<"Unit">>],
            [{<<"myfamily">>,  <<"varchar">>,   false,  1,  1, [], []},
                {<<"myseries">>,   <<"varchar">>,   false,  2,  2, [], []},
                {<<"time">>,       <<"timestamp">>, false,  3,  3, 15, <<"m">>},
                {<<"weather">>,    <<"varchar">>,   false, [], [], [], []},
                {<<"temperature">>,<<"double">>,    true,  [], [], [], []}]}},

    Cluster = ts_setup:start_cluster(1),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    Got = ts_ops:query(Cluster, Qry),

    ?assertEqual(Expected, Got),
    pass.
