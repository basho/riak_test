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
-module(ts_updown_util).

-include_lib("eunit/include/eunit.hrl").

-export([
         make_queries_and_data/0,
         do_node_transition/3,
         create_versioned_table/3,
         run_queries/2
        ]).

%% Many tables will be created, with identical structure, distinctly
%% named depending on the node version (current or previous) used at
%% the time of creation. This is to check for DDL propagation issues.
-define(TABLE_STEM, "updown_test_table_").

-define(TEMPERATURE_COL_INDEX, 4).
-define(PRESSURE_COL_INDEX, 5).
-define(PRECIPITATION_COL_INDEX, 6).

-define(CFG(K, C), proplists:get_value(K, C)).


make_queries_and_data() ->
    %% generate data and hoy it into the cluster
    Count = 10,
    Data = ts_util:get_valid_aggregation_data(Count),
    Column4 = [element(?TEMPERATURE_COL_INDEX,   X) || X <- Data],
    Column5 = [element(?PRESSURE_COL_INDEX,      X) || X <- Data],
    Column6 = [element(?PRECIPITATION_COL_INDEX, X) || X <- Data],

    %% now let's create some queries with their expected results
    Where =
        "WHERE myfamily = 'family1' and myseries = 'seriesX'"
        " and time >= 1 and time <= " ++ integer_to_list(Count),

    %% some preliminaries
    Sum4 = lists:sum([X || X <- Column4, is_number(X)]),
    Sum5 = lists:sum([X || X <- Column5, is_number(X)]),
    Sum6 = lists:sum([X || X <- Column6, is_number(X)]),
    Min4 = lists:min([X || X <- Column4, is_number(X)]),
    Min5 = lists:min([X || X <- Column5, is_number(X)]),
    Max4 = lists:max([X || X <- Column4, is_number(X)]),
    Max5 = lists:max([X || X <- Column5, is_number(X)]),

    C4 = [X || X <- Column4, is_number(X)],
    C5 = [X || X <- Column5, is_number(X)],
    Count4 = length(C4),
    Count5 = length(C5),
    Avg4 = Sum4 / Count4,
    Avg5 = Sum5 / Count5,

    StdDevFun4 = stddev_fun_builder(Avg4),
    StdDevFun5 = stddev_fun_builder(Avg5),
    StdDev4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / Count4),
    StdDev5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / Count5),
    Sample4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / (Count4-1)),
    Sample5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / (Count5-1)),

    %% Query templates: there are ~s placeholders for table in
    %% each. Down in query_all_via_client, the real table name is
    %% substituted.
    QQEE =
        [{"SELECT COUNT(myseries) FROM ~s " ++ Where,
          {[<<"COUNT(myseries)">>], [{Count}]}},

         {"SELECT COUNT(time) FROM ~s " ++ Where,
          {[<<"COUNT(time)">>], [{Count}]}},

         {"SELECT COUNT(pressure), count(temperature), cOuNt(precipitation) FROM ~s " ++ Where,
          {[<<"COUNT(pressure)">>,
            <<"COUNT(temperature)">>,
            <<"COUNT(precipitation)">>],
           [{count_non_nulls(Column5),
             count_non_nulls(Column4),
             count_non_nulls(Column6)}]}},

         {"SELECT SUM(temperature) FROM ~s " ++ Where,
          {[<<"SUM(temperature)">>], [{lists:sum([X || X <- Column4, is_number(X)])}]}},

         {"SELECT SUM(temperature), sum(pressure), sUM(precipitation) FROM ~s " ++ Where,
          {[<<"SUM(temperature)">>, <<"SUM(pressure)">>, <<"SUM(precipitation)">>],
           [{Sum4, Sum5, Sum6}]}},

         {"SELECT MIN(temperature), MIN(pressure) FROM ~s " ++ Where,
          {[<<"MIN(temperature)">>, <<"MIN(pressure)">>], [{Min4, Min5}]}},

         {"SELECT MAX(temperature), MAX(pressure) FROM ~s " ++ Where,
          {[<<"MAX(temperature)">>, <<"MAX(pressure)">>], [{Max4, Max5}]}},

         {"SELECT AVG(temperature), MEAN(pressure) FROM ~s " ++ Where,
          {[<<"AVG(temperature)">>, <<"MEAN(pressure)">>],
           [{Avg4, Avg5}]}},

         {"SELECT STDDEV_POP(temperature), STDDEV_POP(pressure),"
          " STDDEV(temperature), STDDEV(pressure), "
          " STDDEV_SAMP(temperature), STDDEV_SAMP(pressure) FROM ~s " ++ Where,
          {[
            <<"STDDEV_POP(temperature)">>, <<"STDDEV_POP(pressure)">>,
            <<"STDDEV(temperature)">>, <<"STDDEV(pressure)">>,
            <<"STDDEV_SAMP(temperature)">>, <<"STDDEV_SAMP(pressure)">>
           ],
           [{StdDev4, StdDev5, Sample4, Sample5, Sample4, Sample5}]
          }},

         {"SELECT SUM(temperature), MIN(pressure), AVG(pressure) FROM ~s " ++ Where,
          {[<<"SUM(temperature)">>, <<"MIN(pressure)">>, <<"AVG(pressure)">>],
           [{Sum4, Min5, Avg5}]
          }}
        ],

    [
     {data, Data},
     {queries,
      [{N, {Qn, {ok, En}}} || {N, {Qn, En}} <- lists:zip(lists:seq(1,10), QQEE)]}
    ].


%% In order to accurately test the DDL compilation, upgrade and
%% propagation during the test iterations, there will be many tables
%% created and populated in the process, with distinctive names
%% reflecting the state of the node they are created on: node id and
%% version (d for previous, u for current), 2 x 3 identical tables in total.
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
    ToVsn = ?CFG(Version, Config),
    ok = rt:upgrade(Node, ToVsn),
    ok = rt:wait_for_service(Node, riak_kv),
    pass.


run_queries(Config, TestNo) ->
    Queries = ?CFG(queries, Config),
    {QueryFmt, Exp} = ?CFG(TestNo, Queries),
    Nodes = ?CFG(nodes, Config),

    %% try reading data from all nodes: we will thus cover both reading
    %%
    %% (a) from upgraded and downgraded nodes when other nodes are,
    %%     similarly, upgraded or downgraded;
    %%
    %% (b) from both the node the initial writing was done at, as well
    %%     as from other nodes.
    Success =
        lists:all(
          fun({_NodeNo, Node}) ->
                  Got = query_all_via_client(       %% maybe select previous/current client vsn, depending on
                          QueryFmt, Node, Config),  %% whether Node is downgraded or not
                  assert_multi(Exp, Got, TestNo, Node)
          end,
          Nodes),
    if Success ->
            pass;
       el/=se ->
            ct:fail("some queries failed (see above)")
    end.


%% !. Depending in whether the the node at which a query is issued is
%%    upgraded or downgraded, we will use the current or previous
%%    client, respectively.
%% 2. The three nodes will be in variable state as we progress through
%%    iterations (upgraded or downgraded): correspondingly, we will
%%    query from a table created when this particular node was
%%    upgraded or downgraded.
query_all_via_client(QueryFmt, AtNode, Config) ->
    AtNodeVersionSlot =
        rtdev:node_version(
          rtdev:node_id(AtNode)),
    Queries =
        [begin
             Table = make_table_name(N, rtdev:node_version(N)),
             fmt(QueryFmt, [Table])
         end || N <- [1,2,3]],

    [begin
         ct:pal("using ~p client with ~p to issue\n  ~p", [AtNodeVersionSlot, AtNode, Query]),
         Client = rt:pbc(AtNode),
         case AtNodeVersionSlot of
             current ->
                 riakc_ts:query(Client, Query);
             previous ->
                 rpc:call(
                   client_node(AtNodeVersionSlot, Config),
                   riakc_ts, query, [Client, Query])
        end
    end || Query <- Queries].

client_node(current, _Config) ->
    node();
client_node(previous, Config) ->
    ?CFG(previous_client_node, Config).


assert_multi(Exp, Got, TestNo, Node) when not is_list(Exp), not is_list(Got) ->
    case ts_util:assert_float("query " ++ integer_to_list(TestNo), Exp, Got) of
        pass ->
            true;
        fail ->
            ct:log("failed query ~b issued at node ~p (~p)",
                   [TestNo, Node, rtdev:node_version(rtdev:node_id(Node))]),
            false
    end;

assert_multi(Exp, Got, TestNo, Node) when is_list(Exp), is_list(Got) ->
    lists:all(
      fun(X) -> X end,
      [assert_multi(ExpCase, GotCase, TestNo, Node)
       || {ExpCase, GotCase} <- lists:zip(Exp, Got)]);

assert_multi(Exp, Got, TestNo, Node) when not is_list(Exp) ->
    %% if all expected values are the same, we conveniently do the
    %% duplication here
    assert_multi(
      lists:duplicate(length(Got), Exp), Got, TestNo, Node).

%%
%% helper fns
%%
count_non_nulls(Col) ->
    length([V || V <- Col, V =/= []]).

stddev_fun_builder(Avg) ->
    fun(X, Acc) -> Acc + (Avg-X)*(Avg-X) end.

fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).
