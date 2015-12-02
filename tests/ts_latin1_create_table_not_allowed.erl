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

%% Test for creating a latin1 table name. This should fail
%% because of mochijson decoding.

-module(ts_latin1_create_table_not_allowed).

-behavior(riak_test).

-export([confirm/0]).

confirm() ->
    TableDef =
        "CREATE TABLE mytãble ("
        "family VARCHAR   NOT NULL, "
        "series VARCHAR   NOT NULL, "
        "time   TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((family, series, quantum(time, 15, 's')), family, series, time))",
    [Node | _] = ts_util:build_cluster(single),
    {[Node|_] = Cluster,_} = ts_util:cluster_and_connect(single),
    {ok, Out} = ts_util:create_bucket_type(Cluster, TableDef, "mytãble"),
    case binary:match(list_to_binary(Out), <<"invalid json">>) of
        nomatch ->
            {error, "Expecting this to fail, check implications for riak_ql"};
        _ ->
            pass
    end.
