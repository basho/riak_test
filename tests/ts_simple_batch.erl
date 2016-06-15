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

-module(ts_simple_batch).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

%% 900000 is a quantum boundary. Dump several batches in around the
%% boundary, select a much smaller chunk around it
-define(LOWER_DATA, 899000).
-define(LOWER_QRY,  899990).
-define(UPPER_DATA, 901000).
-define(UPPER_QRY,  900050).

confirm() ->
    TestType = normal,
    DDL = ts_util:get_ddl(),
    Qry = ts_util:get_valid_qry(?LOWER_QRY, ?UPPER_QRY),
    Data = ts_util:get_valid_select_data(fun() -> lists:seq(?LOWER_DATA,?UPPER_DATA) end),
    Expected =
        {ts_util:get_cols(small),
         ts_util:exclusive_result_from_data(Data, ?LOWER_QRY-?LOWER_DATA+2, (?LOWER_QRY-?LOWER_DATA)+(?UPPER_QRY-?LOWER_QRY))},
    {[Node], _Pid} = Conn = ts_util:cluster_and_connect(single),


    rt_intercept:add(Node, {riak_kv_eleveldb_backend,
                            [{{batch_put, 4}, batch_put}]}),

    ts_util:ts_put(Conn, TestType, DDL, Data),
    Got = ts_util:ts_query(Conn, TestType, DDL, Data, Qry),
    ?assertEqual(Expected, Got),
    Tally = rpc:call(Node, riak_core_metadata, fold,
                     [fun tally_tallies/2, 0, {riak_test, backend_intercept}]),

    %% 3 batches, n_val=3, 9 total writes to eleveldb
    ?assertEqual(Tally, 9),
    pass.

tally_tallies({_Pid, Vals}, Acc) when is_list(Vals) ->
    Acc + lists:sum(Vals);
tally_tallies({_Pid, Val}, Acc) ->
    Acc + Val.
