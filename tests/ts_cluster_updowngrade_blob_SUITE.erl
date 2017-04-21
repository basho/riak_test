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
-module(ts_cluster_updowngrade_blob_SUITE).

-compile(export_all).

-include("ts_updowngrade_test.part").

make_initial_config(Config) ->
    [{use_previous_client, true} | Config].

make_scenarios() ->
    [#scenario{table_node_vsn             = TableNodeVsn,
               query_node_vsn             = QueryNodeVsn,
               need_table_node_transition = NeedTableNodeTransition,
               need_query_node_transition = NeedQueryNodeTransition,
               need_pre_cluster_mixed     = NeedPreClusterMixed,
               need_post_cluster_mixed    = NeedPostClusterMixed,
               ensure_full_caps           = [{{riak_kv, riak_ql_ddl_rec_version}, v2}],
               ensure_degraded_caps       = [{{riak_kv, riak_ql_ddl_rec_version}, v1}],
               convert_config_to_previous = fun ts_updown_util:convert_riak_conf_to_previous/1}
     || TableNodeVsn            <- [current],
        QueryNodeVsn            <- [previous, current],
        NeedTableNodeTransition <- [false],
        NeedQueryNodeTransition <- [false, true],
        NeedPreClusterMixed     <- [false],
        NeedPostClusterMixed    <- [false]].

%%
%% For versions of Riak TS which do not understand what a `blob' is,
%% the DDL will be compiled with `varchar' and should be represented
%% the same way via the API.
make_scenario_invariants(Config) ->
    DDL = "create table ~s " ++
        " (time timestamp not null, " ++
        " firmware blob, primary key ((quantum(time, 15, 's')), time));",

    {SelectVsExpected, Data} = make_queries_and_data(),
    Create = #create{ddl = DDL,   expected = {ok, {[], []}}},
    Insert = #insert{data = Data, expected = ok},
    Selects = [#select{qry        = Q,
                       expected   = E,
                       assert_mod = ?MODULE,
                       assert_fun = plain_assert}
               || {Q, E} <- SelectVsExpected],
    DefaultTestSets = [#test_set{testname   = "basic_blob",
                                 create     = Create,
                                 insert     = Insert,
                                 selects    = Selects}],
    Config ++ [{default_tests, DefaultTestSets}].

plain_assert(_String, Exp, Exp) ->
    pass;
plain_assert(String, Exp, Got) ->
    ok = log_error(String ++ "rando", Exp, Got),
    fail.

make_queries_and_data() ->
    Data = [{1, <<"hello world this is a test of the emergency broadcast system">>},
            {2, <<1, 2, 15, 42, 255>>},
            {3, <<0, 0, 0, 0, 0, 0, 0, 0>>}],
    Results = {[<<"firmware">>],
               lists:map(fun({_Time, Blob}) -> {Blob} end,
                         Data)},

    {
      [{"SELECT firmware FROM ~s WHERE time > 0 AND time < 4",
        {ok, Results}}],
      Data
    }.

log_error(String, Exp, Got) ->
    lager:info("*****************", []),
    lager:info("Test ~p failed", [String]),
    lager:info("Exp ~p", [Exp]),
    lager:info("Got ~p", [Got]),
    lager:info("*****************", []),
    ok.
