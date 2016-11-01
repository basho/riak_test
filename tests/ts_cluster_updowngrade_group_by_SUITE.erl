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
-module(ts_cluster_updowngrade_group_by_SUITE).

-export([
         sorted_assert/3,
         plain_assert/3
        ]).

-include("ts_updowngrade_test.part").

%% yes this 1.3 error is bonkers and a proper chocolate teapot but that's the error
-define(SELECTERROR, {error, {1020, <<"Used group as a measure of time in 1000group. Only s, m, h and d are allowed.">>}}).

make_initial_config(Config) ->
    [{use_previous_client, true} | Config].

make_scenarios() ->
    BaseScenarios = [#scenario{table_node_vsn             = TableNodeVsn,
                               query_node_vsn             = QueryNodeVsn,
                               need_table_node_transition = NeedTableNodeTransition,
                               need_query_node_transition = NeedQueryNodeTransition,
                               need_pre_cluster_mixed     = NeedPreClusterMixed,
                               need_post_cluster_mixed    = NeedPostClusterMixed,
                               ensure_full_caps     = [{{riak_kv, sql_select_version}, v3}],
                               ensure_degraded_caps = [{{riak_kv, sql_select_version}, v2}],
                               convert_config_to_previous = fun ts_updown_util:convert_riak_conf_to_previous/1}
                     || TableNodeVsn            <- [current, previous],
                        QueryNodeVsn            <- [current, previous],
                        NeedTableNodeTransition <- [true, false],
                        NeedQueryNodeTransition <- [true, false],
                        NeedPreClusterMixed     <- [true, false],
                        NeedPostClusterMixed    <- [true, false]],
    [add_tests(X) || X <- BaseScenarios].

%% This test will not use config invariants
%% see ts_cluster_updowngrade_select_aggregation_SUITE.erl for an example
%% of how to use them
make_scenario_invariants(Config) ->
    Config.

%% GROUP BY will always work for up/downgrades between 1.4 and 1.5 and
%% newer versions
add_tests(Scen) ->
    Tests = [
             make_select_grouped_field_test(select_passes),
             make_group_by_2_test(select_passes)
            ],
    Scen#scenario{tests = Tests}.

make_select_grouped_field_test(DoesSelectPass) ->
    Create = #create{ddl = "CREATE TABLE ~s ("
                     "a SINT64 NOT NULL, "
                     "b SINT64 NOT NULL, "
                     "c TIMESTAMP NOT NULL, "
                     "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c))",
                     expected = {ok, {[], []}}},

    Insert = #insert{data = [{1,B,C} || B <- [1,2,3], C <- [1,2,3]],
                     expected = ok},

    {SelExp, AssertFn}
        = case DoesSelectPass of
              select_passes ->
                  {{ok, {[<<"c">>], [{2},{1},{3}]}}, sorted_assert};
              select_fails  ->
                  {?SELECTERROR, plain_assert}
             end,
    Select = #select{qry = "SELECT c FROM ~s "
                     "WHERE a = 1 AND b = 1 AND c >= 1 AND c <= 1000 "
                     "GROUP BY c",
                     expected   = SelExp,
                     assert_mod = ?MODULE,
                     assert_fun = AssertFn},

    #test_set{testname = "grouped_field_test",
              create  = Create,
              insert  = Insert,
              selects = [Select]}.

make_group_by_2_test(DoesSelectPass) ->
    Create = #create{ddl = "CREATE TABLE ~s ("
                     "a SINT64 NOT NULL, "
                     "b SINT64 NOT NULL, "
                     "c TIMESTAMP NOT NULL, "
                     "d SINT64 NOT NULL, "
                     "e SINT64 NOT NULL, "
                     "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c,d))",
                     expected   = {ok, {[], []}}},

    Insert = #insert{data     = [{1,1,CE,D,CE} || CE <- lists:seq(1,1000),
                                                  D <- [1,2,3]],
                     expected = ok},

    {SelExp, AssertFn}
        = case DoesSelectPass of
              select_passes -> {{ok, {[<<"d">>, <<"AVG(e)">>],
                                      [{2,500.5}, {3,500.5}, {1,500.5}]}},
                                sorted_assert};
              select_fails  -> {?SELECTERROR, plain_assert}
          end,
    Select = #select{qry = "SELECT d, AVG(e) FROM ~s "
                     "WHERE a = 1 AND b = 1 AND c >= 1 AND c <= 1000 "
                     "GROUP BY d",
                     expected = SelExp,
                     assert_mod = ?MODULE,
                     assert_fun = AssertFn},

    #test_set{testname = "group_by_2",
              create  = Create,
              insert  = Insert,
              selects = [Select]}.

sorted_assert(String, {ok, {ECols, Exp}}, {ok, {GCols, Got}}) ->
    Exp2 = lists:sort(Exp),
    Got2 = lists:sort(Got),
    ts_util:assert_float(String, {ECols, Exp2}, {GCols, Got2});
sorted_assert(String, Exp, Got) ->
    ok = log_error(String ++ " banjo", Exp, Got),
    fail.

plain_assert(_String, Exp, Exp) ->
    pass;
plain_assert(String, Exp, Got) ->
    ok = log_error(String ++ "rando", Exp, Got),
    fail.

log_error(String, Exp, Got) ->
    lager:info("*****************", []),
    lager:info("Test ~p failed", [String]),
    lager:info("Exp ~p", [Exp]),
    lager:info("Got ~p", [Got]),
    lager:info("*****************", []),
    ok.
