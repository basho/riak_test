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

    DDL = ts_data:get_ddl(),
    Data = ts_data:get_valid_select_data(),
    ShuffledData = shuffle_list(Data),
    Qry = ts_data:get_valid_qry(),
    Expected = {ok, {
        ts_data:get_cols(),
        ts_data:exclusive_result_from_data(Data, 2, 9)}},
    % write the shuffled TS records but expect the
    % unshuffled records
    Cluster = ts_setup:start_cluster(3),
    Table = ts_data:get_default_bucket(),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    ok = ts_ops:put(Cluster, Table, ShuffledData),
    Got = ts_ops:query(Cluster, Qry),

    ?assertEqual(Expected, Got),
    pass.

%%
shuffle_list(List) ->
    random:seed(),
    RSeqd1 = [{random:uniform(), E} || E <- List],
    RSeqd2 = lists:sort(RSeqd1),
    [E || {_, E} <- RSeqd2].
