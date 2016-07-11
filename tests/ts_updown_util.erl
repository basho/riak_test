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
%%
%% @doc Provide a full set of CT callbacks, to be called 1:1 from the
%%      actual r_t modules, for a comprehensive cycle of table
%%      creation with subsequent SELECT queries on a cluster of three
%%      nodes, downgrading and upgrading each one in this sequence:
%%      uuu -> uud -> udd -> ddd -> ddu -> duu -> uuu.
%%
%%      At each iteration as we down- or upgrade a node, we create a
%%      distinctively named table, from which selects will be run.
%%      This is to check for DDL propagation issues between nodes of
%%      different versions. Next, we execute SELECTs at all nodes,
%%      thus covering both reading:
%%
%%     (a) from upgraded and downgraded nodes when other nodes are,
%%         similarly, upgraded or downgraded;
%%     (b) from both the node the initial writing was done at, as well
%%         as from other nodes.

-module(ts_updown_util).

-include_lib("eunit/include/eunit.hrl").

-export([
         make_config/1,
         maybe_shutdown_client_node/1,
         run_scenario/1
        ]).


-define(CFG(K, C), proplists:get_value(K, C)).

-define(QUERY_NUMBERS, lists:seq(1,10)).

-define(TABLE_STEM, "updown_test_table_").

make_config(Config) ->
    lists:foldl(
      fun(Fun, Cfg) -> Fun(Cfg) end,
      Config,
      [fun make_node_config/1,
       fun make_client_config/1]).

make_node_config(Config) ->
    %% %% get the test meta data from the riak_test runner
    %% TestMetaData = riak_test_runner:metadata(self()),

    %% build the starting (old = upgraded, current) cluster
    Nodes = rt:build_cluster(
              lists:duplicate(3, current)),
    Config ++
        [
         {nodes, lists:zip([1,2,3], Nodes)}
        ].

make_client_config(Config) ->
    UsePreviousClient = proplists:get_value(use_previous_client, Config, false),
    PrevClientNode = maybe_setup_slave_for_previous_client(UsePreviousClient),
    Config ++
        [
         {previous_client_node, PrevClientNode}
        ].

maybe_setup_slave_for_previous_client(true) ->
    %% set up a separate, slave node for the 'previous' version
    %% client, to talk to downgraded nodes
    _ = application:start(crypto),
    Suffix = [crypto:rand_uniform($a, $z) || _ <- [x,x,x,i,x,x,x,i]],
    PrevRiakcNode = list_to_atom("alsoran_"++Suffix++"@127.0.0.1"),
    rt_client:set_up_slave_for_previous_client(PrevRiakcNode);
maybe_setup_slave_for_previous_client(_) ->
    node().

maybe_shutdown_client_node(Config) ->
    case ?CFG(previous_client_node, Config) of
        ThisNode when ThisNode == node() ->
            ok;
        SlaveNode ->
            rt_slave:stop(SlaveNode)
    end.


run_scenario(Config) ->
    %% this is the cycle iterations and what is done when
    ok = create_versioned_table(Config, 1, current),
    ok = create_versioned_table(Config, 2, current),
    ok = create_versioned_table(Config, 3, current),
    ActiveTables0 = [{1,u}, {2,u}, {3,u}],
    Res0 = [],

    Res1 = query_burst(Config, ActiveTables0, Res0),

    ok = do_node_transition(Config, 3, previous),
    ok = create_versioned_table(Config, 3, previous),
    ActiveTables1 = ActiveTables0 ++ [{3,d}],

    Res2 = query_burst(Config, ActiveTables1, Res1),

    ok = do_node_transition(Config, 2, previous),
    ok = create_versioned_table(Config, 2, previous),
    ActiveTables2 = ActiveTables1 ++ [{2,d}],

    Res3 = query_burst(Config, ActiveTables2, Res2),

    ok = do_node_transition(Config, 1, previous),
    ok = create_versioned_table(Config, 1, previous),
    ActiveTables3 = ActiveTables2 ++ [{1,d}],

    Res4 = query_burst(Config, ActiveTables3, Res3),

    %% and upgrade again: set of tables is now complete

    ok = do_node_transition(Config, 1, current),
    Res5 = query_burst(Config, ActiveTables3, Res4),

    ok = do_node_transition(Config, 2, current),
    Res6 = query_burst(Config, ActiveTables3, Res5),

    ok = do_node_transition(Config, 3, current),
    Res7 = query_burst(Config, ActiveTables3, Res6),

    case Res7 of
        [] ->
            pass;
        FailuresDetailed ->
            PrintMe = layout_fails_for_printing(FailuresDetailed),
            ct:fail("Failing queries/queried from/when node was/table:\n~s", [PrintMe])
    end.

layout_fails_for_printing(FF) ->
    lists:flatten(
      [io_lib:format("~b\t~p\t~p\t~p~p\n", [TestNo, QueryingNode, QueryingNodeVsn, TableCreatedNode, WhenNodeWas])
       || {TestNo, QueryingNode, QueryingNodeVsn, {TableCreatedNode, WhenNodeWas}} <- FF]).

query_burst(Config, ActiveTables, Res) ->
    lists:flatten(
      [Res, [run_queries(Config, N, ActiveTables) || N <- ?QUERY_NUMBERS]]).


%% In order to accurately test the DDL compilation, upgrade and
%% propagation during the test cycle, there will be many tables
%% created and populated in the process, with distinctive names
%% reflecting the state of the node they are created on: node id and
%% version (d for previous, u for current), 2 x 3 identical tables in
%% total.
create_versioned_table(Config, NodeNo, Vsn) ->
    Nodes = ?CFG(nodes, Config),
    Data = ?CFG(data, Config),
    Table = make_table_name(NodeNo, Vsn),
    DDL = ts_util:get_ddl(aggregation, Table),
    Client = rt:pbc(?CFG(NodeNo, Nodes)),
    {ok, _} = riakc_ts:query(Client, DDL),
    ok = wait_until_active_table(Client, Table, 5),
    ok = riakc_ts:put(Client, Table, Data).

make_table_name(NodeNo, previous) ->
    lists:flatten([?TABLE_STEM, integer_to_list(NodeNo), "d"]);
make_table_name(NodeNo, current) ->
    lists:flatten([?TABLE_STEM, integer_to_list(NodeNo), "u"]).

wait_until_active_table(_Client, Table, 0) ->
    ct:fail("Table ~s took too long to activate", [Table]),
    not_ok;
wait_until_active_table(Client, Table, N) ->
    case riakc_ts:query(Client, "DESCRIBE "++Table) of
        {ok, _} ->
            ok;
        _ ->
            timer:sleep(1000),
            wait_until_active_table(Client, Table, N-1)
    end.


do_node_transition(Config, N, Version) ->
    Nodes = ?CFG(nodes, Config),
    Node  = ?CFG(N, Nodes),
    %% ToVsn = ?CFG(Version, Config),
    ok = rt:upgrade(Node, Version),
    ok = rt:wait_for_service(Node, riak_kv).


run_queries(Config, TestNo, ActiveTables) ->
    Queries = ?CFG(queries, Config),
    {QueryFmt, Exp} = ?CFG(TestNo, Queries),
    Nodes = ?CFG(nodes, Config),
    ct:log("query ~s", [QueryFmt]),

    lists:foldl(
      fun({_NodeNo, Node}, Failures) ->
              Got = query_all_via_client(    %% maybe select previous/current client vsn, depending on
                      {TestNo, QueryFmt}, Node, Config, ActiveTables),  %% whether Node is downgraded or not
              Failures ++ collect_fails(Exp, Got, TestNo, Node)
      end,
      [],
      Nodes).


%% !. Depending in whether the node at which a query is issued is
%%    upgraded or downgraded, we will use the current or previous
%%    client, respectively (this is optional, controlled by
%%    use_previous_client property in Options arg of
%%    init_per_suite/3).
%% 2. The three nodes will eventually have two tables created on each,
%%    as we progress through iterations ('u' created when that node is
%%    upgraded, and then 'd', following the downgrade): we will
%%    therefore try both, first checking if it exists.
query_all_via_client({TestNo, QueryFmt}, AtNode, Config, ActiveTables) ->
    AtNodeVersionSlot =
        rtdev:node_version(
          rtdev:node_id(AtNode)),
    %% ct:pal("ActiveTables ~p", [ActiveTables]),
    Queries =
        lists:flatten(
          [[[{fmt(QueryFmt, [make_table_name(N, current )]), {N,u}} || lists:member({N, u}, ActiveTables)],
            [{fmt(QueryFmt, [make_table_name(N, previous)]), {N,d}} || lists:member({N, d}, ActiveTables)]]
           || N <- [1,2,3]]),
    [begin
         ct:log("using ~p client with ~p to issue query ~b against ~p", [AtNodeVersionSlot, AtNode, TestNo, TableId]),
         ct:log("\"~s\"", [Query]),
         Client = rt:pbc(AtNode),
         Res =
             case AtNodeVersionSlot of
                 current ->
                     riakc_ts:query(Client, Query);
                 previous ->
                     rpc:call(
                       client_node(AtNodeVersionSlot, Config),
                       riakc_ts, query, [Client, Query])
             end,
         {Res, TableId}
    end || {Query, TableId} <- Queries].

client_node(current, _Config) ->
    node();
client_node(previous, Config) ->
    ?CFG(previous_client_node, Config).


collect_fails(Exp, {Got, TableId}, TestNo, QueryingNode) when not is_list(Exp), not is_list(Got) ->
    QueryingNodeVsn = rtdev:node_version(rtdev:node_id(QueryingNode)),
    case ts_util:assert_float("query " ++ integer_to_list(TestNo), Exp, Got) of
        pass ->
            [];
        fail ->
            ct:log("failed query ~b issued at node ~p (~p)",
                   [TestNo, QueryingNode, QueryingNodeVsn]),
            [{TestNo, QueryingNode, QueryingNodeVsn, TableId}]
    end;

collect_fails(Exp, Got, TestNo, Node) when is_list(Exp), is_list(Got) ->
    lists:append(
      [collect_fails(ExpCase, GotCase, TestNo, Node)
       || {ExpCase, GotCase} <- lists:zip(Exp, Got)]);

collect_fails(Exp, Got, TestNo, Node) when not is_list(Exp) ->
    %% if all expected values are the same, we conveniently do the
    %% duplication here
    collect_fails(
      lists:duplicate(length(Got), Exp), Got, TestNo, Node).


fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).
