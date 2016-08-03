% -------------------------------------------------------------------
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
%% @doc A module to test riak_ts cover plan retrieval/usage.

-module(ts_cluster_coverage).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_pb/include/riak_ts_pb.hrl").

confirm() ->
    %% 100 quanta to populate
    QuantaTally = 100,
    %% Each quantum is 15 minutes in milliseconds
    QuantumMS = 15 * 60 * 1000,
    UpperBoundExcl = QuantaTally * QuantumMS,
    TimesGeneration = fun() -> lists:seq(1, UpperBoundExcl-1, 3124) end,
    DDL = ts_util:get_ddl(),
    Data = ts_util:get_valid_select_data(TimesGeneration),
    Nodes = ts_util:build_cluster(multiple),
    Table = ts_util:get_default_bucket(),
    ts_util:create_table(normal, Nodes, DDL, Table),
    ok = riakc_ts:put(rt:pbc(hd(Nodes)), Table, Data),
    %% First test on a small range well within the size of a normal query
    SmallData = lists:filter(fun({_, _, Time, _, _}) ->
                                     Time < (4 * QuantumMS)
                             end, Data),
    test_quanta_range(Table, SmallData, Nodes, 4, QuantumMS),
    %% Now test the full range
    test_quanta_range(Table, Data, Nodes, QuantaTally, QuantumMS),
    test_replacement_quanta(Table, Data, Nodes, QuantaTally, QuantumMS),
    pass.

test_replacement_quanta(Table, ExpectedData, Nodes, NumQuanta, QuantumMS) ->
    AdminPid = rt:pbc(lists:nth(3, Nodes)),
    Qry = ts_util:get_valid_qry(-1, NumQuanta * QuantumMS),
    {ok, CoverageEntries} = riakc_ts:get_coverage(AdminPid, Table, Qry),
    ?assertEqual(NumQuanta, length(CoverageEntries)),

    Results =
        lists:foldl(
          fun({{IP, Port}, Context, TsRange, Description}, Acc) ->
                  %% Replace this chunk. Will result in only one item.
                  {ok, [NewCover]} = riakc_ts:replace_coverage(AdminPid, Table, Qry, Context),

                  %% Validate that the new cover is distinct from the
                  %% old one but shares the same range and
                  %% description. IP2 should equal IP as long as we're
                  %% running devrel, but someday we may have tests
                  %% against true clusters
                  {{IP2, Port2}, Context2, TsRange, Description} = NewCover,
                  ?assertNotEqual(Context, Context2),
                  ?assertNotEqual(Port, Port2),

                  {ok, Pid} = riakc_pb_socket:start_link(
                                binary_to_list(IP2), Port2),
                  {ok, {_, ThisQuantum}} = riakc_ts:query(Pid, Qry, [], Context2),
                  riakc_pb_socket:stop(Pid),

                  %% Open a connection to another node and make
                  %% certain we get no results using this cover
                  %% context. This assumes a common IP, so it only
                  %% works for devrel tests
                  {ok, WrongPid} = riakc_pb_socket:start_link(
                                     binary_to_list(IP), alternate_port(Port2)),
                  make_noise_if_results_not_empty(
                    ThisQuantum,
                    riakc_ts:query(WrongPid, Qry, [], Context2),
                    Port2, Description, Context2
                   ),
                  riakc_pb_socket:stop(WrongPid),

                  %% Let's compare the range data with the
                  %% query results to make sure the latter
                  %% fall within the former
                  check_data_against_range(ThisQuantum, TsRange),
                  %% Now add to the pile and continue
                  ThisQuantum ++ Acc
          end,
          [],
          CoverageEntries),
    ?assertEqual(lists:sort(ExpectedData), lists:sort(Results)).

test_quanta_range(Table, ExpectedData, Nodes, NumQuanta, QuantumMS) ->
    AdminPid = rt:pbc(lists:nth(3, Nodes)),
    OtherPid = rt:pbc(lists:nth(2, Nodes)),
    Qry = ts_util:get_valid_qry(-1, NumQuanta * QuantumMS),
    {ok, CoverageEntries} = riakc_ts:get_coverage(AdminPid, Table, Qry),
    ?assertEqual(NumQuanta, length(CoverageEntries)),

    Results =
        lists:foldl(
          fun({{IP, Port}, Context, TsRange, Description}, Acc) ->
                  {ok, Pid} = riakc_pb_socket:start_link(
                                binary_to_list(IP), Port),
                  {ok, {_, ThisQuantum}} = riakc_ts:query(Pid, Qry, [], Context),
                  riakc_pb_socket:stop(Pid),

                  %% Open a connection to another node and make
                  %% certain we get no results using this cover
                  %% context. This assumes a common IP, so it only
                  %% works for devrel tests
                  {ok, WrongPid} = riakc_pb_socket:start_link(
                                     binary_to_list(IP), alternate_port(Port)),
                  make_noise_if_results_not_empty(
                    ThisQuantum,
                    riakc_ts:query(WrongPid, Qry, [], Context),
                    Port, Description, Context
                   ),
                  riakc_pb_socket:stop(WrongPid),

                  %% Let's compare the range data with the
                  %% query results to make sure the latter
                  %% fall within the former
                  check_data_against_range(ThisQuantum, TsRange),
                  %% Now add to the pile and continue
                  ThisQuantum ++ Acc
          end,
          [],
          CoverageEntries),
    ?assertEqual(lists:sort(ExpectedData), lists:sort(Results)),

    %% Expect {error,{1001,<<"{too_many_subqueries,13}">>}} if NumQuanta > 5
    case NumQuanta > 5 of
        true ->
            ?assertMatch({error, {1001, _}}, riakc_ts:query(OtherPid, Qry));
        false ->
            {ok, {_, StraightQueryResults}} = riakc_ts:query(OtherPid, Qry),
            ?assertEqual(lists:sort(ExpectedData), lists:sort(StraightQueryResults))
    end.

time_within_range(Time, Lower, LowerIncl, Upper, UpperIncl) ->
    if
        Lower > Time ->
            false;
        Upper < Time ->
            false;
        Lower < Time andalso Time < Upper ->
            true;
        Lower == Time ->
            LowerIncl;
        Upper == Time ->
            UpperIncl
    end.

check_data_against_range(Data, {_FieldName, {{Lower, LowerIncl}, {Upper, UpperIncl}}}) ->
    ?assertEqual([], lists:filter(fun({_, _, Time, _, _}) ->
                                          not time_within_range(Time, Lower, LowerIncl,
                                                                Upper, UpperIncl)
                                  end,
                                  Data)).

%% Cheap and easy. ts_util gives us 3 nodes, we know the ports.
alternate_port(10017) ->
    10027;
alternate_port(10027) ->
    10037;
alternate_port(10037) ->
    10017.

make_noise_if_results_not_empty(_RealResults, {ok, {[], []}},
                                _Port, _Description, _Context) ->
    ok;
make_noise_if_results_not_empty(RealResults, {ok, {_, RealResults}},
                                Port, Description, Context) ->
    lager:info("Stopping test, we got results when we didn't want any"),
    ContextUnwrapped = binary_to_term(Context),
    lager:info("Retrieved real data from ~p : ~p~n", [Port, Description]),
    lager:info("Context: ~p~n", [ContextUnwrapped]),
    lager:info("Asked from wrong port: ~p~n", [alternate_port(Port)]),
    lager:info("Got same results from both nodes"),
    throw(got_right_results_from_wrong_vnode);
make_noise_if_results_not_empty(_RealResults, {ok, {_, _BogusResults}},
                                Port, Description, Context) ->
    lager:info("Stopping test, we got results when we didn't want any"),
    ContextUnwrapped = binary_to_term(Context),
    lager:info("Retrieved real data from ~p : ~p~n", [Port, Description]),
    lager:info("Context: ~p~n", [ContextUnwrapped]),
    lager:info("Asked from wrong port: ~p~n", [alternate_port(Port)]),
    lager:info("Got different (non-empty) results from second node"),
    throw(got_wrong_results_from_wrong_vnode).
