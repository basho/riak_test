%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016, 2017 Basho Technologies, Inc.
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
%%
%% The purpose if this module is to enable CT test module writers to
%% run a cluster through a succession of upgrade/downgrades
%% (scenarios), each consisting of these three stages:
%%
%%   (1) table creation, via a CREATE TABLE query,
%%   (2) inserting data, via riakc_ts:put,
%%   (3) querying data back, via one or more SELECT queries.
%%
%% Scenarios (see #scenario{} in ts_updown_util.hrl) define which
%% version (previous or current, whichever they are in your
%% ~/.riak_test.config), which nodes, if any, to transitions before
%% CREATE and, separately, before SELECT stages, and which SELECTs to
%% run.  Each of the stages or queries can be expected to fail.
%%
%% Queries to nodes can be made using a client of matching version or
%% the current client (controlled by 'use_previous_client' property in
%% Config).
%%
%% The contents of riak.conf can be manipulated after a node
%% transition using hooks (#scenario.convert_config_to_previous,
%% .convert_config_to_current}).
%%
%% If an upgrade/downgrade involves a change in capabilities, these
%% would need to be listed in #scenario.ensure_full_caps and
%% .ensure_degraded_caps.


-module(ts_updown_util).

-export([
         caps_to_ensure/1,
         convert_riak_conf_to_previous/1,
         maybe_shutdown_client_node/1,
         setup/1,
         run_scenarios/2,
         run_scenario/2
        ]).

-include_lib("eunit/include/eunit.hrl").
-include("ts_updown_util.hrl").

-type versioned_cluster() :: [{node(), version()}].

%% preparations
%% ------------

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
    ct:pal("Setting up ~p for old client", [PrevRiakcNode]),
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
%% ---------

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
             #scenario{tests = Tests} = Scenario) ->
    ct:pal("Scenario: table/query node vsn: ~p/~p\n"
           "          Need table/query node transition: ~p/~p",
           [Scenario#scenario.table_node_vsn,
            Scenario#scenario.query_node_vsn,
            Scenario#scenario.need_table_node_transition,
            Scenario#scenario.need_query_node_transition]),
    PrevRiakcNode =
        ?CFG(previous_client_node, Config),
    NodesAtVersions0 =
        [{N, rtdev:node_version(rtdev:node_id(N))} || N <- ?CFG(nodes, Config)],

    %% 1. retrieve scenario-invariant data from Config
    AllTests =
        add_timestamps(
          lists:append(
            [proplists:get_value(default_tests, Config, []), Tests])),

    {_NodesAtVersions9, _TableNode9, _QueryNode9, Failures} =
        lists:foldl(
          fun(Stage, {NodesAtVersionsI, TableNodeI, QueryNodeI, []}) ->
                  {NodesAtVersionsJ, TableNodeJ, QueryNodeJ, Failures} =
                      Stage(NodesAtVersionsI, TableNodeI, QueryNodeI, PrevRiakcNode,
                            Scenario#scenario{tests = AllTests}),
                  {NodesAtVersionsJ, TableNodeJ, QueryNodeJ, Failures};
             (_Stage, PrevState) ->
                  %% not doing anything because a previous stage has failed
                  PrevState
          end,
          {NodesAtVersions0, node_tbd, node_tbd, []},
          [fun create_stage/5,
           fun insert_stage/5,
           fun select_stage/5]),

    case Failures of
        [] -> ok;
        _  -> ct:pal("Failing queries in this scenario:\n"
                     "----------------\n"
                     "~s\n", [layout_fails_for_printing(Failures, Scenario)])
    end,

    Failures.


%% running tests: stages
%% ---------------------

create_stage(NodesAtVersions0, _TableNode, _QueryNode,
             PrevRiakcNode,
             #scenario{table_node_vsn = TableNodeVsn,
                       query_node_vsn = QueryNodeVsn,
                       need_pre_cluster_mixed  = NeedPreClusterMixed,
                       convert_config_to_previous = ConvertConfigToPreviousFun,
                       convert_config_to_current  = ConvertConfigToCurrentFun,
                       ensure_full_caps     = EnsureFullCaps,
                       ensure_degraded_caps = EnsureDegradedCaps,
                       tests = Tests}) ->
    ct:pal("Creating tables in ~p", [NodesAtVersions0]),
    ConvConfFuns = {ConvertConfigToPreviousFun, ConvertConfigToCurrentFun},
    CapsReqs = {EnsureFullCaps, EnsureDegradedCaps},

    %% 2. Pick two nodes for create table and subsequent selects
    {TableNode, NodesAtVersions1} =
        find_or_make_node_at_vsn(NodesAtVersions0, TableNodeVsn, [], ConvConfFuns),
    {QueryNode, NodesAtVersions2} =
        find_or_make_node_at_vsn(NodesAtVersions1, QueryNodeVsn, [TableNode], ConvConfFuns),

    %% 3. Try to ensure cluster is (not) mixed as hinted but keep the
    %%    interesting nodes at their versions as set in step 1.
    NodesAtVersions3 =
        ensure_cluster(
          NodesAtVersions2, NeedPreClusterMixed, [TableNode, QueryNode],
          ConvConfFuns, CapsReqs),

    ct:pal("Selected Table/Query nodes ~p/~p", [TableNode, QueryNode]),
    %% 4. For each table in the test, issue CREATE TABLE queries
    Fails =
        lists:append(
          [make_tables(X, NodesAtVersions3, TableNode, QueryNode, PrevRiakcNode)
           || X <- Tests]),

    {NodesAtVersions3, TableNode, QueryNode, Fails}.


insert_stage(NodesAtVersions3, TableNode, QueryNode,
             _PrevRiakcNode,  %% no need to fall back to PrevRiakcNode as we use riakc_ts:put
             #scenario{tests = Tests}) ->
    ct:pal("Inserting data in ~p", [NodesAtVersions3]),
    %% 5. For each table in the test, issue INSERT queries
    Fails =
        lists:append(
          [insert_data(X, NodesAtVersions3, TableNode, QueryNode)
           || X <- Tests]),

    {NodesAtVersions3, TableNode, QueryNode, Fails}.


select_stage(NodesAtVersions3, TableNode, QueryNode,
             PrevRiakcNode,
             #scenario{table_node_vsn = TableNodeVsn,
                       query_node_vsn = QueryNodeVsn,
                       need_table_node_transition = NeedTableNodeTransition,
                       need_query_node_transition = NeedQueryNodeTransition,
                       need_post_cluster_mixed = NeedPostClusterMixed,
                       convert_config_to_previous = ConvertConfigToPreviousFun,
                       convert_config_to_current  = ConvertConfigToCurrentFun,
                       ensure_full_caps     = EnsureFullCaps,
                       ensure_degraded_caps = EnsureDegradedCaps,
                       tests = Tests}) ->
    ct:pal("Running selects ~p", [NodesAtVersions3]),
    ConvConfFuns = {ConvertConfigToPreviousFun, ConvertConfigToCurrentFun},
    CapsReqs = {EnsureFullCaps, EnsureDegradedCaps},

    %% 6. possibly do a transition, on none, one of, or both create
    %%    table node and query node
    NodesAtVersions4 =
        if NeedTableNodeTransition ->
                possibly_transition_node(
                  NodesAtVersions3, TableNode, other_version(TableNodeVsn), ConvConfFuns);
           el/=se ->
                NodesAtVersions3
        end,
    NodesAtVersions5 =
        if NeedQueryNodeTransition ->
                possibly_transition_node(
                  NodesAtVersions4, QueryNode, other_version(QueryNodeVsn), ConvConfFuns);
           el/=se ->
                NodesAtVersions4
        end,

    %% 7. after transitioning the two relevant nodes, try to bring the
    %%    other nodes to satisfy the mixed/non-mixed hint
    NodesAtVersions6 =
        ensure_cluster(NodesAtVersions5, NeedPostClusterMixed, [TableNode, QueryNode],
                       ConvConfFuns, CapsReqs),

    %% 8. issue the queries and collect results
    Fails =
        lists:append(
          [run_selects(X, NodesAtVersions6, TableNode, QueryNode, PrevRiakcNode)
           || X <- Tests]),

    {NodesAtVersions6, TableNode, QueryNode, Fails}.


add_timestamps(TestSets) ->
    [X#test_set{timestamp = make_timestamp()} || X <- TestSets].

make_timestamp() ->
    {_Mega, Sec, Milli} = os:timestamp(),
    fmt("~b~b", [Sec, Milli]).


%% running test stages: create, insert, select
%% -------------------------------------------

make_tables(#test_set{create = #create{should_skip = true}}, _, _, _, _) ->
    [];
make_tables(#test_set{testname  = Testname,
                      timestamp = Timestamp,
                      create    = #create{ddl      = DDLFmt,
                                          expected = Exp}},
            NodesAtVersions, TableNode, QueryNode,
            PrevClientNode) ->
    %% fast machines:
    timer:sleep(1),
    Table = get_table_name(Testname, Timestamp),
    DDL = fmt(DDLFmt, [Table]),
    case query_with_client(DDL, TableNode, PrevClientNode) of
        Exp ->
            ok = wait_until_active_table(TableNode, PrevClientNode, Table, 5),
            ct:log("Table ~p created on ~p", [Table, TableNode]),
            [];
        {error, {_No, Error}} ->
            ct:log("Failed to create table ~p: (~s) with ~s", [Table, Error, DDL]),
            [#failure_report{failing_test = make_msg("Creation of ~s failed", [Table]),
                             cluster      = NodesAtVersions,
                             table_node   = TableNode,
                             query_node   = QueryNode,
                             expected     = Exp,
                             got          = Error}]
    end.


insert_data(#test_set{insert = #insert{should_skip = true}}, _, _, _) ->
    [];
insert_data(#test_set{testname  = Testname,
                      timestamp = Timestamp,
                      insert    = #insert{data     = Data,
                                          expected = Exp}},
            NodesAtVersions, TableNode, QueryNode) ->
    Client1 = rt:pbc(TableNode),
    Table = get_table_name(Testname, Timestamp),
    case riakc_ts:put(Client1, Table, Data) of
        Exp ->
            ct:log("Table ~p on ~p had ~b records successfully inserted",
                   [Table, TableNode, length(Data)]),
            [];
        Error ->
            ct:log("Failed to insert data into ~p (~p)", [Table, Error]),
            [#failure_report{message      = make_msg("Insert of data into ~s failed", [Table]),
                             cluster      = NodesAtVersions,
                             table_node   = TableNode,
                             query_node   = QueryNode,
                             expected     = Exp,
                             got          = Error}]
    end.


run_selects(#test_set{testname  = Testname,
                      timestamp = Timestamp,
                      selects   = Selects},
            NodesAtVersions, TableNode, QueryNode,
            PrevClientNode) ->
    QryNos = lists:seq(1, length(Selects)),
    Zip = lists:zip(Selects, QryNos),
    Tablename = get_table_name(Testname, Timestamp),

    RunSelect =
        fun(#select{should_skip = true}, _QryNo) ->
                [];
           (#select{qry        = Q,
                    expected   = Exp,
                    assert_mod = Mod,
                    assert_fun = Fun}, QryNo) ->
                SelectQuery = fmt(Q, [Tablename]),
                Got = query_with_client(SelectQuery, QueryNode, PrevClientNode),
                case Mod:Fun(fmt("Query #~p", [QryNo]), Exp, Got) of
                    pass -> [];
                    fail -> [#failure_report{message    = SelectQuery,
                                             cluster    = NodesAtVersions,
                                             table_node = TableNode,
                                             query_node = QueryNode,
                                             expected   = Exp,
                                             got        = Got}]
                end
        end,
    lists:append(
      [RunSelect(S, QN) || {S, QN} <- Zip]).



%% query wrapper, possibly using a previous version client
%% -------------------------------------------------------

query_with_client(Query, Node, PrevClientNode) ->
    Version = rtdev:node_version(rtdev:node_id(Node)),
    case Version of
        current ->
            Client = rt:pbc(Node),
            riakc_ts:query(Client, Query);
        previous ->
            ConnInfo = proplists:get_value(Node, rt:connection_info([Node])),
            {IP, Port} = proplists:get_value(pb, ConnInfo),
            ct:log("using ~p client to contact node ~p at ~p:~p", [Version, Node, IP, Port]),
            {ok, Client} =
                rpc:call(
                  PrevClientNode,
                  riakc_pb_socket, start_link, [IP, Port, [{auto_reconnect, true}]]),
            rpc:call(
              PrevClientNode,
              riakc_ts, query, [Client, Query])
    end.



layout_fails_for_printing(Failures,
                          #scenario{table_node_vsn = TableNodeVsn,
                                    query_node_vsn = QueryNodeVsn}) ->
    [begin
         DidTableNodeTransition =
             TableNodeVsn /= proplists:get_value(TableNode, NodesAtVersions),
         DidQueryNodeTransition =
             QueryNodeVsn /= proplists:get_value(QueryNode, NodesAtVersions),
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
               FailingTest,
               Expected,
               Got])])
     end || #failure_report{cluster    = NodesAtVersions,
                            table_node = TableNode,
                            query_node = QueryNode,
                            failing_test = FailingTest,
                            expected = Expected,
                            got = Got} <- Failures].


%% cluster preparation & node transitions
%% --------------------------------------

-spec is_cluster_mixed(versioned_cluster()) -> boolean().
is_cluster_mixed(NodesAtVersions) ->
    {_N0, V0} = hd(NodesAtVersions),
    not lists:all(fun({_N, V}) -> V == V0 end, NodesAtVersions).


-spec ensure_cluster(versioned_cluster(), boolean(), [node()],
                     {function(), function()}, {[cap_with_ver()], [cap_with_ver()]}) ->
                            versioned_cluster().
%% @doc Massage the cluster if necessary (and if possible) to pick two
%%      nodes of specific versions, optionally ensuring the resulting
%%      cluster is mixed or not.
ensure_cluster(NodesAtVersions0, NeedClusterMixed, ImmutableNodes,
               ConvConfFuns, {FullCaps, DegradedCaps}) ->
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
                      NodesAtVersions0, other_version(hd(ImmutableVersions)),
                      ImmutableNodes, ConvConfFuns),
                NodesAtDiffVersions;

            %% non-mixed/mixed hints can be honoured only when
            %% TableNodeVsn and QueryNodeVsn are same/not the same:
            {true, false} when not IsInherentlyMixed ->
                %% cluster is mixed even though both relevant nodes
                %% are at same version: align the rest
                ensure_single_version_cluster(
                  NodesAtVersions0, hd(ImmutableVersions), ImmutableNodes, ConvConfFuns);
            {true, false} when IsInherentlyMixed ->
                %% cluster is mixed because both relevant nodes are
                %% not at same version: don't honour the hint
                ct:log("ignoring NeedClusterMixed == false hint because TableNodeVsn /= QueryNodeVsn", []),
                NodesAtVersions0
        end,

    {SomeNode, _Ver} = hd(NodesAtVersions1),
    CapsToCheck =
        case ((current == rtdev:node_version(rtdev:node_id(SomeNode)))
              and not is_cluster_mixed(NodesAtVersions1)) of
            true ->
                FullCaps;
            false ->
                DegradedCaps
        end,
    lists:foreach(
      fun({Node, {Cap, Ver}}) ->
              ok = rt:wait_until_capability(Node, Cap, Ver)
      end,
      [{Node, CapVer} || {Node, _} <- NodesAtVersions1, CapVer <- CapsToCheck]),

    NodesAtVersions1.


-spec find_or_make_node_at_vsn(versioned_cluster(), version(),
                               [node()], {function(), function()}) ->
                                      {node(), versioned_cluster()}.
find_or_make_node_at_vsn(NodesAtVersions0, ReqVersion, ImmutableNodes, ConvConfFuns) ->
    MutableNodes =
        [{N, V} || {N, V} <- NodesAtVersions0, not lists:member(N, ImmutableNodes)],
    case lists:keyfind(ReqVersion, 2, MutableNodes) of
        {SuitableNode, ReqVersion} ->
            {SuitableNode, NodesAtVersions0};
        false ->
            case hd(MutableNodes) of
                {Node, Vsn} when Vsn == ReqVersion ->
                    {Node, NodesAtVersions0};
                {Node, TransitionMe} ->
                    OtherVersion = other_version(TransitionMe),
                    ok = transition_node(Node, OtherVersion, ConvConfFuns),
                    {Node, lists:keyreplace(Node, 1, NodesAtVersions0, {Node, OtherVersion})}
            end
    end.


-spec ensure_single_version_cluster(versioned_cluster(), version(),
                                    [node()], {function(), function()}) ->
                                           versioned_cluster().
ensure_single_version_cluster(NodesAtVersions, ReqVersion, ImmutableNodes, ConvConfFuns) ->
    Transitionable =
        [N || {N, V} <- NodesAtVersions,
              V /= ReqVersion, not lists:member(N, ImmutableNodes)],
    rt:pmap(fun(N) -> transition_node(N, ReqVersion, ConvConfFuns) end,
            Transitionable),
    %% recreate NodesAtVersions
    [{N, ReqVersion} || {N, _V} <- NodesAtVersions].


-spec transition_node(node(), version(), {function(), function()}) -> ok.
transition_node(Node, Version,
                {ConvertConfigToPreviousFun, ConvertConfigToCurrentFun}) ->
    ct:log("transitioning node ~p to version '~p'", [Node, Version]),
    case Version of
        previous ->
            ok = rt:upgrade(Node, Version, ConvertConfigToPreviousFun);
        current ->
            ok = rt:upgrade(Node, Version, ConvertConfigToCurrentFun)
    end,
    ok = rt:wait_for_service(Node, riak_kv).


-spec possibly_transition_node(versioned_cluster(), node(), version(),
                               {function(), function()}) ->
                                      versioned_cluster().
possibly_transition_node(NodesAtVersions, Node, ReqVsn, ConvConfFuns) ->
    case lists:keyfind(Node, 1, NodesAtVersions) of
        ReqVsn ->
            NodesAtVersions;
        _TransitionMe ->
            ok = transition_node(Node, ReqVsn, ConvConfFuns),
            lists:keyreplace(Node, 1, NodesAtVersions,
                             {Node, ReqVsn})
    end.

other_version(current) -> previous;
other_version(previous) -> current.


%% misc functions
%% --------------

get_table_name(Testname, Timestamp) when is_list(Testname) andalso
                                         is_list(Timestamp) ->
    Testname ++ Timestamp.

wait_until_active_table(_TargetNode, _PrevClientNode, Table, 0) ->
    ct:fail("Table ~s took too long to activate", [Table]),
    not_ok;
wait_until_active_table(TargetNode, PrevClientNode, Table, N) ->
    case query_with_client("DESCRIBE "++Table, TargetNode, PrevClientNode) of
        {ok, _} ->
            ok;
        _ ->
            timer:sleep(1000),
            wait_until_active_table(TargetNode, PrevClientNode, Table, N-1)
    end.

make_msg(Format, Payload) ->
    list_to_binary(fmt(Format, Payload)).


get_riak_release_in_slot(VsnSlot) ->
    case rtdev:get_version(VsnSlot) of
        unknown ->
            ct:fail("Failed to determine riak version in '~s' slot", [VsnSlot]);
        Known ->
            case re:run(Known, "riak_ts(?:_ee|)-(\\d+)\\.(\\d+)\\.(\\d+)", [{capture, all_but_first, list}]) of
                {match, [V1, V2, V3]} ->
                    {list_to_integer(V1),
                     list_to_integer(V2),
                     list_to_integer(V3)};
                nomatch ->
                    ct:fail("Failed to parse riak version in '~s' slot", [VsnSlot])
            end
    end.

%% ---------------------------------

%% Dealing with case-by-case upgrade particulars, such as:
%%
%% * newly introduced keys in riak.conf that need to be deleted on
%%   downgrade;
%%
%% * capabilities that need to be ensured before running the tests
%%   (arguments to `wait_until_capability`).

%% We need to comment out those settings which appear in version
%% 1.x. For version 1.x-1 to work with riak.conf initially created in
%% 1.x, the offending settings need to be deleted.  We do it here, by
%% commenting them out.

convert_riak_conf_to_previous(Config) ->
    DatafPath = ?CFG(new_data_dir, Config),
    RiakConfPath = filename:join(DatafPath, "../etc/riak.conf"),
    {ok, Contents0} = file:read_file(RiakConfPath),
    Contents9 =
        lists:foldl(
          fun(KeyToDelete, Contents) ->
                  re:replace(Contents, ["^", KeyToDelete], "#\\1",
                             [global, multiline, {return, list}])
          end,
          Contents0,
          get_riak_conf_new_keys()),
    ok = file:write_file(RiakConfPath, Contents9).

%% When a new release is cut, register newly introduced keys here:
get_riak_conf_new_keys() ->
    %% the current version may have not been tagged yet, so look at
    %% previous version
    case get_riak_release_in_slot(previous) of
        {1, 5, _} ->
            ["riak_kv.query.timeseries.qbuf_inmem_max"];
        {1, 4, _} ->
            ["riak_kv.query.timeseries.max_returned_data_size",
             "riak_kv.query.timeseries.qbuf_soft_watermark",
             "riak_kv.query.timeseries.qbuf_hard_watermark",
             "riak_kv.query.timeseries.qbuf_expire_ms",
             "riak_kv.query.timeseries.qbuf_incomplete_release_ms"]
    end.

%% Wait for these capabilities to settle at these versions at the end
%% of upgrade/downgrade:
caps_to_ensure(full) ->
    case get_riak_release_in_slot(previous) of
        {1, 5, _} ->
            [];  %% no new caps in 1.6 since 1.5
        {1, 4, _} ->
            [{{riak_kv, sql_select_version}, v3},
             {{riak_kv, riak_ql_ddl_rec_version}, v2},
             {{riak_kv, decode_query_results_at_vnode}, true}]
    end;
caps_to_ensure(degraded) ->
    case get_riak_release_in_slot(previous) of
        {1, 5, _} ->
            [];  %% no new caps in 1.6 since 1.5
        {1, 4, _} ->
            [{{riak_kv, sql_select_version}, v2},
             {{riak_kv, riak_ql_ddl_rec_version}, v1},
             {{riak_kv, decode_query_results_at_vnode}, false}]
    end.


%% Keep the old convert_riak_conf_to_previous functions around, for
%% reference and occasional test rerun

%% %% riak.conf created under 1.4 cannot be read by 1.3 because the properties
%% %% have been renamed so they need to be replaced with the 1.3 versions.
%% convert_riak_conf_to_1_3(RiakConfPath) ->
%%     {ok, Content1} = file:read_file(RiakConfPath),
%%     Content2 = binary:replace(
%%         Content1,
%%         <<"riak_kv.query.timeseries.max_quanta_span">>,
%%         <<"timeseries_query_max_quanta_span">>, [global]),
%%     Content3 = binary:replace(
%%         Content2,
%%         <<"riak_kv.query.timeseries.max_concurrent_queries">>,
%%         <<"riak_kv.query.concurrent_queries">>, [global]),
%%     Content4 = convert_timeout_config_to_1_3(Content3),
%%     ok = file:write_file(RiakConfPath, Content4).
%%
%% %% The timeout property needs some special care because 1.3 uses 10000 for
%% %% milliseconds and 1.4 uses 10s for a number and unit.
%% convert_timeout_config_to_1_3(Content) ->
%%     Re =  "(riak_kv.query.timeseries.timeout)\s*=\s*([0-9]*)([smh])",
%%     re:replace(Content, Re, "timeseries_query_timeout_ms = 10000").

fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).
