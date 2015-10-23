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
-define(PVAL_P1, <<"ZXC11">>).
-define(TIMEBASE, (10*1000*1000)).

confirm() ->
    io:format("Data to be written: ~p\n", [make_data()]),

    ClusterSize = 1,
    lager:info("Building cluster"),
    rt:set_backend(eleveldb),
    _Nodes = [Node1|_] =
        build_cluster(
          ClusterSize),

    %% 1. use riak-admin to create a bucket
    TableDef = io_lib:format(
                 "CREATE TABLE ~s "
                 "(~s varchar not null, "
                 " ~s timestamp not null, "
                 " score float not null, "
                 " PRIMARY KEY((quantum(time, 10, s)), time, sensor))",
                 [?BUCKET, ?PKEY_P1, ?PKEY_P2]),
    Props = io_lib:format("{\\\"props\\\": {\\\"n_val\\\": 3, \\\"table_def\\\": \\\"~s\\\"}}", [TableDef]),
    rt:admin(Node1, ["bucket-type", "create", binary_to_list(?BUCKET), lists:flatten(Props)]),
    rt:admin(Node1, ["bucket-type", "activate", binary_to_list(?BUCKET)]),

    %% 2. set up a client
    C = rt:pbc(Node1),
    ?assert(is_pid(C)),

    %% 3. put some data
    Data0 = make_data(),
    ResPut = riakc_ts:put(C, ?BUCKET, Data0),
    io:format("Put ~b records: ~p\n", [length(Data0), ResPut]),

    %% 4. delete one
    ElementToDelete = 15,
    DelRecord = [DelSensor, DelTimepoint, _Score] =
        lists:nth(ElementToDelete, Data0),
    DelKey = [DelTimepoint, DelSensor],
    DelNXKey = [DelTimepoint, <<"keke">>],
    %% Data = lists:delete(DelRecord, Data0),
    ResDel = riakc_ts:delete(C, ?BUCKET, DelKey, []),
    ?assertEqual(ResDel, ok),
    io:format("Deleted key ~b (~p): ~p\n", [ElementToDelete, DelRecord, ResDel]),
    ResDelNX = riakc_ts:delete(C, ?BUCKET, DelNXKey, []),
    ?assertEqual(ResDelNX, ok),
    io:format("Not deleted non-existing key: ~p\n", [ResDelNX]),

    %% 5. select
    Query =
        lists:flatten(
          io_lib:format(
            "select * from ~s where ~s > ~b and ~s < ~b and sensor = \"~s\"",
           [?BUCKET, ?PKEY_P2, ?TIMEBASE + 10, ?PKEY_P2, ?TIMEBASE + 20, ?PVAL_P1])),
    io:format("Running query: ~p\n", [Query]),
    {_Columns, Rows} = riakc_ts:query(C, Query),
    io:format("Got ~b rows back\n~p\n", [length(Rows), Rows]),
    ?assertEqual(length(Rows), 10 - 1 - 1),
    {_Columns, Rows} = riakc_ts:query(C, Query),
    io:format("Got ~b rows back again\n", [length(Rows)]),
    ?assertEqual(length(Rows), 10 - 1 - 1),

    pass.


%% @ignore
-spec build_cluster(non_neg_integer()) -> [node()].
build_cluster(Size) ->
    build_cluster(Size, [{yokozuna, [{enabled, true}]}]).
-spec build_cluster(pos_integer(), list()) -> [node()].
build_cluster(Size, Config) ->
    [_Node1|_] = Nodes = rt:deploy_nodes(Size, Config),
    rt:join_cluster(Nodes),
    Nodes.


-define(LIFESPAN, 30).
make_data() ->
    lists:foldl(
      fun(T, Q) ->
              [[?PVAL_P1,
                ?TIMEBASE + ?LIFESPAN - T + 1,
                math:sin(float(T) / 100 * math:pi())] | Q]
      end,
      [], lists:seq(?LIFESPAN, 0, -1)).
