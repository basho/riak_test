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

-module(ts_simple_activate_table_pass_2).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    Cluster = ts_util:build_cluster(single),
    % individual assert matches to show line numbers in failures
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(Cluster, table_def("^"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(Cluster, table_def("$"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(Cluster, table_def("!"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "!", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "#@#@", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "\\", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "|", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "?", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "%", "series", "time"))
    ),
    % backticks need to be escaped or sh will error
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "\\`", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "{", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "}", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "[", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", "]", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", ";", "series", "time"))
    ),
    ?assertMatch(
        {error_creating_bucket_type, _},
        create_and_activate_bucket_type(
            Cluster, table_def("mytable", ":", "series", "time"))
    ),
    pass.

%%
create_and_activate_bucket_type(Cluster, {TableName, DDL}) ->
    {ok, Out} = ts_util:create_bucket_type(Cluster, DDL, TableName, 3),
    case iolist_to_binary(Out) of
        <<"Error", _/binary>> ->
            {error_creating_bucket_type, Out};
        <<"Cannot create", _/binary>> ->
            {error_creating_bucket_type, Out};
        _ ->
            Retries = 0,
            ts_util:activate_bucket_type(Cluster, TableName, Retries)
    end.

%%
table_def(TableName) ->
    table_def(TableName, "family", "series", "time").

%%
table_def(TableName, FamilyName, SeriesName, TimeName) ->
    {TableName, lists:flatten(io_lib:format(
        "CREATE TABLE ~s ("
        " ~s VARCHAR   NOT NULL,"
        " ~s VARCHAR   NOT NULL,"
        " ~s   TIMESTAMP NOT NULL,"
        " PRIMARY KEY ((~s, ~s, quantum(~s, 15, 'm')), "
        " ~s, ~s, ~s))",
        [TableName,
         FamilyName, SeriesName, TimeName, % column defs
         FamilyName, SeriesName, TimeName,  % partition key
         FamilyName, SeriesName, TimeName]))}. % local key

