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
%% @doc A module to test riak_ts basic create bucket/put/select cycle
%%      (spanning time quanta), with one node being taken out of the
%%      cluster.

-module(ts_degraded_select_pass_2).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    [NodeSpare | OrigCluster] = rt:deploy_nodes(4),
    OrigClients = [rt:pbc(Node) || Node <- OrigCluster],
    OrigCCNN = lists:zip(OrigCluster, OrigClients),
    ok = rt:join_cluster(OrigCluster),

    DDL = ts_data:get_ddl(),
    ?assertEqual({ok, {[], []}}, riakc_ts:query(hd(OrigClients), DDL)),

    Table = ts_data:get_default_bucket(),
    Data = ts_data:get_valid_select_data_spanning_quanta(),
    ?assertEqual(ok, riakc_ts:put(hd(OrigClients), Table, Data)),

    Qry = ts_data:get_valid_qry_spanning_quanta(),

    ok = check_data(OrigCCNN, Qry, Data),

    {NodeOut, _} = lists:nth(2, OrigCCNN),
    ok = rt:leave(NodeOut),
    DegradedCluster = OrigCluster -- [NodeOut],
    DegradedCCNN = lists:keydelete(NodeOut, 1, OrigCCNN),
    ok = rt:wait_until_no_pending_changes(DegradedCluster),
    ok = check_data(DegradedCCNN, Qry, Data),

    %% add a spare
    ok = rt:join(NodeSpare, hd(DegradedCluster)),
    NewCluster = DegradedCluster ++ [NodeSpare],
    NewCCNN = DegradedCCNN ++ [{NodeSpare, rt:pbc(NodeSpare)}],
    ok = rt:wait_until_nodes_ready(NewCluster),
    ok = rt:wait_until_no_pending_changes(NewCluster),
    ok = rt:wait_until_nodes_agree_about_ownership(NewCluster),
    ok = check_data(NewCCNN, Qry, Data),

    pass.

check_data(CCNN, Qry, Expected) ->
    lists:foreach(
      fun({Node, Client}) ->
              {ok, {_Cols, GotRows0}} = riakc_ts:query(Client, Qry),
              GotRows = lists:sort(GotRows0),
              io:format("Got ~b records at ~p\n", [length(GotRows), Node]),
              ?assertEqual(Expected, GotRows)
      end,
      CCNN).
