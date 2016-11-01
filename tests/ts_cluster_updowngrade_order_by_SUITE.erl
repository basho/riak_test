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
-module(ts_cluster_updowngrade_order_by_SUITE).

-export([
         plain_assert/3
        ]).

-include("ts_updowngrade_test.part").

-define(DATA_RAW,
        [{1, 1000, 2},
         {1, 1001, 1},
         {1, 1002, 4},
         {1, 1003, 3}]).
-define(COLUMNS, [<<"c">>]).
-define(DATA_SORTED_BY_A,
        [{1},
         {2},
         {3},
         {4}]).

-define(DDL,
        "create table ~s "
        "(a sint64 not null,"
        " b timestamp not null,"
        " c sint64 not null,"
        " primary key ((a, quantum(b, 10, s)), a, b))").

-define(SELECT,
        "SELECT c FROM ~s"
        " WHERE a = 1 AND b >= 1000 AND b <= 1005"
        " ORDER BY c").

make_initial_config(Config) ->
    [{use_previous_client, true} | Config].

make_scenarios() ->
    BaseScenarios = [#scenario{table_node_vsn             = TableNodeVsn,
                               query_node_vsn             = QueryNodeVsn,
                               need_table_node_transition = NeedTableNodeTransition,
                               need_query_node_transition = NeedQueryNodeTransition,
                               need_pre_cluster_mixed     = NeedPreClusterMixed,
                               need_post_cluster_mixed    = NeedPostClusterMixed,
                               convert_config_to_previous = fun ts_updown_util:convert_riak_conf_to_previous/1}
                     || TableNodeVsn            <- [current, previous],
                        QueryNodeVsn            <- [current, previous],
                        NeedTableNodeTransition <- [true, false],
                        NeedQueryNodeTransition <- [true],
                        NeedPreClusterMixed     <- [true],
                        NeedPostClusterMixed    <- [true]],
    [add_tests(X) || X <- BaseScenarios].

%% This test will not use config invariants
%% see ts_cluster_updowngrade_select_aggregation_SUITE.erl for an example
%% of how to use them
make_scenario_invariants(Config) ->
    Config.

%% ORDER BY will always work if
%% the query node is 1.5
%% the query node is queried *AFTER* a transition
add_tests(#scenario{query_node_vsn             = current,
                    need_query_node_transition = true} = Scen) ->
    Tests = [
             make_select_order_by_test(select_fails)
            ],
    Scen#scenario{tests = Tests};
add_tests(#scenario{query_node_vsn             = previous,
                    need_query_node_transition = false} = Scen) ->
    Tests = [
             make_select_order_by_test(select_fails)
            ],
    Scen#scenario{tests = Tests};
%% in all other scenarios ORDER BY should work
add_tests(Scen) ->
    Tests = [
             make_select_order_by_test(select_passes)
            ],
    Scen#scenario{tests = Tests}.

make_select_order_by_test(DoesSelectPass) ->
    Create =
        #create{ddl = ?DDL,
                expected = {ok, {[], []}}},
    Insert =
        #insert{data = ?DATA_RAW,
                expected = ok},
    SelExp =
        case DoesSelectPass of
            select_passes ->
                {ok, {?COLUMNS, ?DATA_SORTED_BY_A}};
            select_fails  ->
                {error, <<"Unmatched message">>}
        end,
    Select = #select{qry = ?SELECT,
                     expected   = SelExp,
                     assert_mod = ?MODULE,
                     assert_fun = plain_assert},

    #test_set{testname = "orderby_test",
              create  = Create,
              insert  = Insert,
              selects = [Select]}.

plain_assert(_Title, Exp, Exp) ->
    pass;
plain_assert(_, {error, _}, {error, _}) ->
    pass;
plain_assert(_, _, _) ->
    fail.
