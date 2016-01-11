%% -------------------------------------------------------------------
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

%%% This test asserts that results for small queries are
%%% returned in key-sorted order

-module(ts_cluster_select_pass_3_sorted_on_key).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    TestType = normal,
    DDL = ts_util:get_ddl(),
    Data = ts_util:get_valid_select_data(),
    ShuffledData = shuffle_list(Data),
    Qry = ts_util:get_valid_qry(),
    Expected = {
        ts_util:get_cols(),
        ts_util:exclusive_result_from_data(Data, 2, 9)},
    % write the shuffled TS records but expect the
    % unshuffled records
    Got = ts_util:ts_query(
            ts_util:cluster_and_connect(multiple), TestType, DDL, ShuffledData, Qry),
    ?assertEqual(Expected, Got),
    pass.

%%
shuffle_list(List) ->
    random:seed(),
    RSeqd1 = [{random:uniform(), E} || E <- List],
    RSeqd2 = lists:sort(RSeqd1),
    [E || {_, E} <- RSeqd2].
