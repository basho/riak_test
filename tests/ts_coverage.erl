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

-module(ts_coverage).
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
    SmallData = lists:filter(fun([_, _, Time, _, _]) ->
                                     Time < (4 * QuantumMS)
                             end, Data),
    test_quanta_range(Table, lists_to_tuples(SmallData), Nodes, 4, QuantumMS),
    %% Now test the full range
    test_quanta_range(Table, lists_to_tuples(Data), Nodes, QuantaTally, QuantumMS),
    pass.

    
%% We put data with each record as a list, but in the results it's a tuple
lists_to_tuples(Rows) ->
    lists:map(fun erlang:list_to_tuple/1, Rows).

test_quanta_range(Table, ExpectedData, Nodes, NumQuanta, QuantumMS) ->
    AdminPid = rt:pbc(lists:nth(3, Nodes)),
    OtherPid = rt:pbc(lists:nth(2, Nodes)),
    Qry = ts_util:get_valid_qry(-1, NumQuanta * QuantumMS),
    ?debugFmt("Query: ~ts~n", [Qry]),
    {ok, CoverageEntries} = riakc_ts:get_coverage(AdminPid, Table, Qry),

    ?debugFmt("Found ~B coverage entries~n", [length(CoverageEntries)]),
    Results = 
        lists:foldl(fun(#tscoverageentry{ip=IP, port=Port, cover_context=C}, Acc) ->
                            {ok, Pid} = riakc_pb_socket:start_link(binary_to_list(IP),
                                                                   Port),
                            {_Headers, ThisQuantum} = riakc_ts:query(Pid, Qry, [], C),
                            ThisQuantum ++ Acc
                    end,
                    [],
                    CoverageEntries),
    ?debugFmt("Found ~B results~n", [length(Results)]),
    ?assertEqual(lists:sort(ExpectedData), lists:sort(Results)),

    %% Expect {error,{1001,<<"{too_many_subqueries,13}">>}} if NumQuanta > 5
    case NumQuanta > 5 of
        true ->
            ?assertMatch({error, {1001, _}}, riakc_ts:query(OtherPid, Qry));
        false ->
            {_, StraightQueryResults} = riakc_ts:query(OtherPid, Qry),
            ?assertEqual(lists:sort(ExpectedData), lists:sort(StraightQueryResults))
    end.

