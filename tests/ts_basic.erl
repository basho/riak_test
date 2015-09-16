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
%% @doc A module to test riak_ts basic create bucket/put/select cycle.

-module(ts_basic).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"ts_test_bucket_one">>).
-define(PKEY_P1, <<"sensor">>).
-define(PKEY_P2, <<"time">>).


confirm() ->
    %% io:format("Data to be written: ~p\n", [make_data()]),

    ClusterSize = 3,
    lager:info("Building cluster"),
    _Nodes = [Node1, _Node2, _Node3] =
        build_cluster(
          ClusterSize),

    %% 1. use riak-admin to create a bucket
    TableDef = io_lib:format(
                 "CREATE TABLE ~s "
                 "(~s varchar not null, "
                 " ~s timestamp not null, "
                 " money float not null, "
                 " PRIMARY KEY((quantum(time, 10, s)), time, sensor))",
                 [?BUCKET, ?PKEY_P1, ?PKEY_P2]),
    Props = io_lib:format("{\\\"props\\\": {\\\"n_val\\\": 3, \\\"table_def\\\": \\\"~s\\\"}}", [TableDef]),
    rt:admin(Node1, ["bucket-type", "create", binary_to_list(?BUCKET), lists:flatten(Props)]),

    %% 2. set up a client
    C = rt:pbc(Node1),
    ?assert(is_pid(C)),

    %% 3. put some data
    Data = make_data(),
    riakc_ts:put(C, ?BUCKET, Data),

    %% 4. select
    Query =
        lists:flatten(
          io_lib:format(
            "select * from ~s where ~s > 19 and ~s < 80 and sensor = \"~s\"",
           [?BUCKET, ?PKEY_P2, ?PKEY_P2, ?PKEY_P1])),
    {ok, Selected} = riakc_ts:query(C, Query),
    ?assert(length(Selected) == 60),

    pass.



%% @ignore
%% copied from ensemble_util.erl
-spec build_cluster(non_neg_integer()) -> [node()].
build_cluster(Size) ->
    build_cluster(Size, []).
-spec build_cluster(non_neg_integer(), list()) -> [node()].
build_cluster(Size, Config) ->
    [_Node1|_] = Nodes = rt:deploy_nodes(Size, Config),
    rt:join_cluster(Nodes),
    %% ensemble_util:wait_until_cluster(Nodes),
    %% ensemble_util:wait_for_membership(Node1),
    %%ensemble_util:wait_until_stable(Node1, Size),
    Nodes.


-define(LIFESPAN, 100).
make_data() ->
    lists:foldl(
      fun(T, Q) ->
              [[<<"ZXC11">>,
                ?LIFESPAN - T,
                math:sin(float(T) / 100 * math:pi())] | Q]
      end,
      [], lists:seq(0, ?LIFESPAN)).
