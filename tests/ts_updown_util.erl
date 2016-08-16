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
%% @doc Facilities to use in CT-enabled upgrade/downgrade tests.

-module(ts_updown_util).

-export([
         setup/1,
         maybe_shutdown_client_node/1,
         run_scenarios/2,
         run_scenario/2
        ]).

-include_lib("eunit/include/eunit.hrl").
-include("ts_updown_util.hrl").

-type versioned_cluster() :: [{node(), version()}].
%% preparations

setup(Config) ->
    lists:foldl(
      fun(Fun, Cfg) -> Fun(Cfg) end,
      Config,
      [fun setup_cluster/1,
       fun setup_client/1]).

setup_cluster(Config) ->
    %% build the starting (old = upgraded, current) cluster
    Nodes = rt:build_cluster(
              lists:duplicate(3, current)),
    Config ++
        [
         {nodes, Nodes}
        ].

setup_client(Config) ->
    %% By default, we use the client in the 'current' version for all
    %% queries.  Add `{use_previous_client, true}` to the Config arg
    %% when calling it from your_module:init_per_suite to change that
    %% to selectively use old code to connect to downgraded nodes.
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


%% scenarios

-spec run_scenarios(config(), [#scenario{}]) -> [#failure_report{}].
run_scenarios(Config, Scenarios) ->
    Failures =
        lists:foldl(
          fun(Scenario, FF) ->
                  run_scenario(Config, Scenario) ++ FF
          end,
          [], Scenarios),

    Failures.

-spec run_scenario(config(), #scenario{})
                  -> [#failure_report{}].
run_scenario(Config,
             #scenario{table_node_vsn = TableNodeVsn,
                       query_node_vsn = QueryNodeVsn,
                       need_table_node_transition = NeedTableNodeTransition,
                       need_query_node_transition = NeedQueryNodeTransition,
                       need_pre_cluster_mixed = NeedPreClusterMixed,
                       need_post_cluster_mixed = NeedPostClusterMixed,
                       %% for these, we may have invariants in Config:
                       tests = Tests}) ->
    NodesAtVersions0 =
        [{N, rtdev:node_version(rtdev:node_id(N))} || N <- ?CFG(nodes, Config)],

    %% 1. retrieve scenario-invariant data from Config
    TestsToRun = case Tests of
                     [] -> add_timestamps(?CFG(default_tests, Config));
                     Ts -> add_timestamps(Ts)
                 end,

    ct:log("Scenario: table/query_node_vsn: ~p/~p\n"
           "          need_table_node_transition: ~p\n"
           "          need_query_node_transition: ~p\n"
           "          need_pre_cluster_mixed: ~p\n"
           "          need_post_cluster_mixed: ~p\n",
           [TableNodeVsn, QueryNodeVsn,
            NeedTableNodeTransition, NeedQueryNodeTransition,
            NeedPreClusterMixed, NeedPostClusterMixed]),

    %% 2. Pick two nodes for create table and subsequent selects
    {TableNode, NodesAtVersions1} =
        find_or_make_node_at_vsn(NodesAtVersions0, TableNodeVsn, []),
    {QueryNode, NodesAtVersions2} =
        find_or_make_node_at_vsn(NodesAtVersions1, QueryNodeVsn, [TableNode]),

    %% 3. Try to ensure cluster is (not) mixed as hinted but keep the
    %%    interesting nodes at their versions as set in step 1.
    NodesAtVersions3 =
        ensure_cluster(NodesAtVersions2, NeedPreClusterMixed, [TableNode, QueryNode]),

    %% 4. For each table in the test set create table and collect results.
    CreateResults = [make_tables(X, TableNode) || X <- TestsToRun],

    %% 5. For each table in the test insert the data and collect the results
    InsertResults = [insert_data(X, TableNode) || X <- TestsToRun],

    %% 6. possibly do a transition, on none, one of, or both create
    %%    table node and query node
    NodesAtVersions4 =
        if NeedTableNodeTransition ->
                possibly_transition_node(NodesAtVersions3, TableNode,
                                         other_version(TableNodeVsn));
           el/=se ->
                NodesAtVersions3
        end,
    NodesAtVersions5 =
        if NeedQueryNodeTransition ->
                possibly_transition_node(NodesAtVersions4, QueryNode,
                                         other_version(QueryNodeVsn));
           el/=se ->
                NodesAtVersions4
        end,

    %% 7. after transitioning the two relevant nodes, try to bring the
    %%    other nodes to satisfy the mixed/non-mixed hint
    _NodesAtVersions6 =
        ensure_cluster(NodesAtVersions5, NeedPostClusterMixed, [TableNode, QueryNode]),

    %% 8. issue the queries and collect results
    ct:log("Issuing queries at ~p", [QueryNode]),

    SelectResults = [run_selects(X, QueryNode, Config) || X <- TestsToRun],

    Results = lists:flatten(CreateResults ++ InsertResults ++ SelectResults),

    Failures = [#failure_report{cluster = NodesAtVersions2,
                                table_node = TableNode,
                                query_node = QueryNode,
                                did_transition_table_node = NeedTableNodeTransition,
                                did_transition_query_node = NeedQueryNodeTransition,
                                failing_test = Msg,
                                expected = Exp,
                                error    = Got} || #fail{message  = Msg,
                                                         expected = Exp,
                                                         got      = Got}  <- Results],

    case Failures of
        [] -> ok;
        _  -> ct:pal("Failing queries in this scenario:\n"
                     "----------------\n"
                     "~s\n", [layout_fails_for_printing(Failures)])
    end,

    Failures.


query_with_client(Query, Node, Config) ->
    Version = rtdev:node_version(rtdev:node_id(Node)),
    Client = rt:pbc(Node),
    case Version of
        current ->
            riakc_ts:query(Client, Query);
        previous ->
            rpc:call(
              ?CFG(previous_client_node, Config),
              riakc_ts, query, [Client, Query])
    end.


-spec is_cluster_mixed(versioned_cluster()) -> boolean().
is_cluster_mixed(NodesAtVersions) ->
    {_N0, V0} = hd(NodesAtVersions),
    not lists:all(fun({_N, V}) -> V == V0 end, NodesAtVersions).


-spec ensure_cluster(versioned_cluster(), boolean(), [node()])
                    -> versioned_cluster().
%% @doc Massage the cluster if necessary (and if possible) to pick two
%%      nodes of specific versions, optionally ensuring the resulting
%%      cluster is mixed or not.
ensure_cluster(NodesAtVersions0, NeedClusterMixed, ImmutableNodes) ->
    ImmutableVersions =
        [rtdev:node_version(rtdev:node_id(Node)) || Node <- ImmutableNodes],
    IsInherentlyMixed =
        (length(lists:usort(ImmutableVersions)) > 1),
    %% possibly transition some other node to fulfil cluster
    %% homogeneity condition
    NodesAtVersions1 =
        case {is_cluster_mixed(NodesAtVersions0), NeedClusterMixed} of
            {true, true} ->
                NodesAtVersions0;
            {false, false} ->
                NodesAtVersions0;
            {false, true} ->
                %% just flip an uninvolved node
                {_ThirdNode, NodesAtDiffVersions} =
                    find_or_make_node_at_vsn(
                      NodesAtVersions0, other_version(hd(ImmutableVersions)), ImmutableNodes),
                NodesAtDiffVersions;

            %% non-mixed/mixed hints can be honoured only when
            %% TableNodeVsn and QueryNodeVsn are same/not the same:
            {true, false} when not IsInherentlyMixed ->
                %% cluster is mixed even though both relevant nodes
                %% are at same version: align the rest
                ensure_single_version_cluster(
                  NodesAtVersions0, hd(ImmutableVersions), ImmutableNodes);
            {true, false} when IsInherentlyMixed ->
                %% cluster is mixed because both relevant nodes are
                %% not at same version: don't honour the hint
                ct:log("ignoring NeedClusterMixed == false hint because TableNodeVsn /= QueryNodeVsn", []),
                NodesAtVersions0
        end,
    NodesAtVersions1.


-spec find_or_make_node_at_vsn(versioned_cluster(), version(), [node()])
                              -> {node(), versioned_cluster()}.
find_or_make_node_at_vsn(NodesAtVersions0, ReqVersion, ImmutableNodes) ->
    MutableNodes =
        [{N, V} || {N, V} <- NodesAtVersions0, not lists:member(N, ImmutableNodes)],
    case hd(MutableNodes) of
        {Node, Vsn} when Vsn == ReqVersion ->
            {Node, NodesAtVersions0};
        {Node, TransitionMe} ->
            OtherVersion = other_version(TransitionMe),
            ok = transition_node(Node, OtherVersion),
            {Node, lists:keyreplace(Node, 1, NodesAtVersions0, {Node, OtherVersion})}
    end.


-spec ensure_single_version_cluster(versioned_cluster(), version(), [node()])
                                   -> versioned_cluster().
ensure_single_version_cluster(NodesAtVersions, ReqVersion, ImmutableNodes) ->
    Transitionable =
        [N || {N, V} <- NodesAtVersions,
              V /= ReqVersion, not lists:member(N, ImmutableNodes)],
    rt:pmap(fun(N) -> transition_node(N, ReqVersion) end, Transitionable),
    %% recreate NodesAtVersions
    [{N, ReqVersion} || {N, _V} <- NodesAtVersions].


-spec transition_node(node(), version()) -> ok.
transition_node(Node, Version) ->
    ct:log("transitioning node ~p to version '~p'", [Node, Version]),
    case Version of
        previous ->
            ok = rt:upgrade(Node, Version, fun convert_riak_conf_to_1_3/1);
        current ->
            ok = rt:upgrade(Node, Version)
    end,
    ok = rt:wait_for_service(Node, riak_kv).

%%
convert_riak_conf_to_1_3(RiakConfPath) ->
    {ok, Content1} = file:read_file(RiakConfPath),
    Content2 = binary:replace(
        Content1,
        <<"riak_kv.query.timeseries.max_quanta_span">>,
        <<"timeseries_query_max_quanta_span">>, [global]),
    Content3 = binary:replace(
        Content2,
        <<"riak_kv.query.timeseries.max_concurrent_queries">>,
        <<"riak_kv.query.concurrent_queries">>, [global]),
    Content4 = convert_timeout_config_to_1_3(Content3),
    ok = file:write_file(RiakConfPath, Content4).

%%
convert_timeout_config_to_1_3(Content) ->
    Re =  "(riak_kv.query.timeseries.timeout)\s*=\s*([0-9]*)([smh])",
    re:replace(Content, Re, "timeseries_query_timeout_ms = 10000").


-spec possibly_transition_node(versioned_cluster(), node(), version())
                              -> versioned_cluster().
possibly_transition_node(NodesAtVersions, Node, ReqVsn) ->
    case lists:keyfind(Node, 1, NodesAtVersions) of
        ReqVsn ->
            NodesAtVersions;
        _TransitionMe ->
            ok = transition_node(Node, ReqVsn),
            lists:keyreplace(Node, 1, NodesAtVersions,
                             {Node, ReqVsn})
    end.


other_version(current) -> previous;
other_version(previous) -> current.


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


layout_fails_for_printing(FF) ->
    lists:flatten(
      [io_lib:format(
         "  Cluster: ~p\n"
         "TableNode: ~p, transitioned? ~p\n"
         "QueryNode: ~p, transitioned? ~p\n"
         "     Test: ~p\n"
         " Expected: ~p\n"
         "      Got: ~p\n\n",
         [NodesAtVersions,
          TableNode, DidTableNodeTransition,
          QueryNode, DidQueryNodeTransition,
          FailingTest, Expected, Error])
       || #failure_report{cluster = NodesAtVersions,
                          table_node = TableNode,
                          query_node = QueryNode,
                          did_transition_table_node = DidTableNodeTransition,
                          did_transition_query_node = DidQueryNodeTransition,
                          failing_test = FailingTest,
                          expected = Expected,
                          error = Error} <- FF]).

fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).

make_tables(#test_set{create = #create{should_skip = true}}, _TableNode) ->
    pass;
make_tables(#test_set{timestamp = Timestamp,
                      create    = #create{ddl      = DDLFmt,
                                          expected = Exp}}, TableNode) ->
    %% fast machines:
    timer:sleep(1),
    Table = get_table_name(Timestamp),
    DDL = fmt(DDLFmt, [Table]),
    Client1 = rt:pbc(TableNode),
    case riakc_ts:'query'(Client1, DDL) of
        Exp ->
            ok = wait_until_active_table(Client1, Table, 5),
            ct:log("Table ~p created on ~p", [Table, TableNode]),
            pass;
        Error ->
            ct:log("Failed to create table ~p: (~s)", [Table, Error]),
            #fail{message  = make_msg("Creation of ~s failed", [Table]),
                  expected = Exp,
                  got      = Error}
    end.

insert_data(#test_set{insert = #insert{should_skip = true}}, _TableNode) ->
    pass;
insert_data(#test_set{timestamp = Timestamp,
                      insert    = #insert{data     = Data,
                                          expected = Exp}}, TableNode) ->
    Client1 = rt:pbc(TableNode),
    Table = get_table_name(Timestamp),
    case riakc_ts:put(Client1, Table, Data) of
        Exp ->
            ct:log("Table ~p on ~p had ~b records successfully inserted)",
                   [Table, TableNode, length(Data)]),
            pass;
        Error ->
            ct:log("Failed to insert data into ~p (~s)", [Table, Error]),
            #fail{message  = make_msg("Insert of data into ~s failed", [Table]),
                  expected = Exp,
                  got      = Error}
    end.

run_selects(#test_set{timestamp = Timestamp,
                      selects   = Selects}, QueryNode, Config) ->
    QryNos = lists:seq(1, length(Selects)),
    Zip = lists:zip(Selects, QryNos),
    lists:flatten([run_select(S, QN, Timestamp, QueryNode, Config) || {S, QN} <- Zip]).

run_select(#select{should_skip = true}, _QryNo, _Timestamp, _QueryNode, _Config) ->
    pass;
run_select(#select{qry = Q, expected = Exp}, QryNo, Timestamp, QueryNode, Config) ->
    Table = get_table_name(Timestamp),
    SelectQuery = fmt(Q, [Table]),
    Got = query_with_client(SelectQuery, QueryNode, Config),
    case ts_util:assert_float(fmt("Query #~p", [QryNo]), Exp, Got) of
        pass -> pass;
        fail -> #fail{message  = SelectQuery,
                      expected = Exp,
                      got      = Got}
    end.

add_timestamps(TestSets) ->
    [X#test_set{timestamp = make_timestamp()} || X <- TestSets].

make_timestamp() ->
    {_Mega, Sec, Milli} = os:timestamp(),
    fmt("~b~b", [Sec, Milli]).

get_table_name(Timestamp) when is_list(Timestamp) ->
    "updown_test_" ++ Timestamp.

make_msg(Format, Payload) ->
    list_to_binary(fmt(Format, Payload)).
