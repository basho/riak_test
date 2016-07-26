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

-module(ts_simple_show_tables).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    ClusterConn = {_, Conn} = ts_util:cluster_and_connect(single),

    %% First test no tables
    Got = ts_util:single_query(Conn, "SHOW TABLES"),
    ?assertEqual(
        {ok, {[], []}},
        Got
    ),

    %% Now create a bunch of tables
    Tables = [{<<"Alpha">>}, {<<"Beta">>}, {<<"Gamma">>}, {<<"Delta">>}],
    Create =
        "CREATE TABLE ~s ("
        " frequency    timestamp   not null,"
        " PRIMARY KEY ((quantum(frequency, 15, 'm')),"
        " frequency))",
    lists:foreach(fun({T}) ->
            SQL = ts_util:flat_format(Create, [T]),
            {ok, _} = ts_util:create_and_activate_bucket_type(ClusterConn, SQL, T)
        end,
        Tables),
    Got1 = ts_util:single_query(Conn, "SHOW TABLES"),
    ?assertEqual(
        {ok, {[<<"Table">>], lists:usort(Tables)}},
        Got1
    ),
    pass.
