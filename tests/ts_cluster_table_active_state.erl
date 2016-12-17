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

-module(ts_cluster_table_active_state).

-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% Set the maximum wait between CREATE TABLE and subsequent query in milliseconds.
    MaxWait = 1000,
    NodeCount = 3,
    [_Node,ClientNode|_NodesT] = Cluster = ts_setup:start_cluster(NodeCount),
    PBPid = rt:pbc(ClientNode),

    slow_ddl_compilation(Cluster),

    create_and_query(PBPid, 0),
    Waits = [ case Div of
                  0 -> MaxWait;
                  _ -> MaxWait div Div
              end || Div <- [0, 20, 10, 5, 2, 1] ],
    [ create_and_query(PBPid, Wait) || Wait <- Waits ],
    pass.

slow_ddl_compilation(INodes) ->
    %% NOTE: even w/ slow ddl compilation, the test passes on dev hardware
    [ rt_intercept:add(
        Node, {riak_kv_ts_newtype, [{{new_type, 1}, really_delayed_new_type}]}) ||
      Node <- INodes ],
    [rt_intercept:wait_until_loaded(Node) || Node <- INodes].

create_and_query(PBPid, PostCreateWait) ->
    EmptyRes = {ok, {[], []}},
    Table = "ts_cluster_table_active_state_" ++ timestamp_string(),
    CreateSql = create_sql(Table),
    InsertSql = insert_sql(Table),
    CreateRes = riakc_ts:query(PBPid, CreateSql),
    timer:sleep(PostCreateWait),
    InsertRes = riakc_ts:query(PBPid, InsertSql),
    ?assertMatch(EmptyRes,
                 CreateRes),
    ?assertMatch(EmptyRes,
                 InsertRes).

create_sql(Table) ->
    "CREATE TABLE " ++ Table ++
    "(ts TIMESTAMP NOT NULL," ++
    " PRIMARY KEY((QUANTUM(ts, 1, 'd')), ts))".

insert_sql(Table) ->
    "INSERT INTO " ++ Table ++
    " VALUES(" ++ timestamp_string() ++ ")".

timestamp_int() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    MegaSecs * 1000000000000 + Secs * 1000000 + MicroSecs.

timestamp_string() ->
    integer_to_list(timestamp_int()).
