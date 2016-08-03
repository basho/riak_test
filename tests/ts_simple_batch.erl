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

%% 900000 is a quantum boundary. Sample records are around 80 bytes.
%% Try to create 3 batches: one below 900000, and two above. (Batches
%% are constrained to roughly 1MB in size, so ~13k records per batch;
%% the batch below 900000 will be terminated by the quantum boundary).
-define(LOWER_DATA, 899000).
-define(LOWER_QRY,  899990).
-define(UPPER_DATA, 920000).
-define(UPPER_QRY,  900050).

confirm() ->
    DDL = ts_util:get_ddl(),
    Qry = ts_util:get_valid_qry(?LOWER_QRY, ?UPPER_QRY),
    Data = ts_util:get_valid_select_data(fun() -> lists:seq(?LOWER_DATA,?UPPER_DATA) end),
    Expected =
        {ts_util:get_cols(small),
         ts_util:exclusive_result_from_data(Data, ?LOWER_QRY-?LOWER_DATA+2, (?LOWER_QRY-?LOWER_DATA)+(?UPPER_QRY-?LOWER_QRY))},
    {[Node], Pid} = ts_util:cluster_and_connect(single),


    rt_intercept:add(Node, {riak_kv_eleveldb_backend,
                            [{{batch_put, 4}, batch_put}]}),

    %% Buried in the bowels of the code path behind ts_util:ts_put/4
    %% is a calculation that n_val is the same as the cluster size. I
    %% want a single node cluster for this test, but n_val of 4, so
    %% I'll duplicate the path here
    Bucket = ts_util:get_default_bucket(),
    {ok, _} = ts_util:create_bucket_type([Node], DDL, Bucket, 4),
    ts_util:activate_bucket_type([Node], Bucket),

    riakc_ts:put(Pid, Bucket, Data),

    Tallies = rpc:call(Node, riak_core_metadata, to_list,
                       [{riak_test, backend_intercept}]),
    FullTally = lists:foldl(fun tally_tallies/2, 0, Tallies),

    %% 3 batches, n_val=4, 12 total writes to eleveldb
    ?assertEqual(12, FullTally),

    {ok, Got} = riakc_ts:query(Pid, Qry),
    ?assertEqual(Expected, Got),

    pass.

tally_tallies({_Pid, Vals}, Acc) when is_list(Vals) ->
    Acc + lists:sum(Vals);
tally_tallies({_Pid, Val}, Acc) ->
    Acc + Val.
