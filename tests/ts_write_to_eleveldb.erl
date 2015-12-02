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

-module(ts_write_to_eleveldb).
-compile({parse_transform, rt_intercept_pt}).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"GeoCheckin">>).

confirm() ->

    lager:info("Building cluster"),
    _Nodes = [Node1|_] = build_cluster(1),

    DDL = ts_util:get_ddl(),
    Props = io_lib:format("{\\\"props\\\": {\\\"n_val\\\": 3, \\\"table_def\\\": \\\"~s\\\"}}", [DDL]),
    rt:admin(Node1, ["bucket-type", "create", binary_to_list(?BUCKET), lists:flatten(Props)]),
    rt:admin(Node1, ["bucket-type", "activate", binary_to_list(?BUCKET)]),

    lager:info("installing interceptor"),
    Self = self(),

    %% Based on experimentation, the path used for puts in ts is async.
    ok = rt_intercept:add(Node1, {riak_kv_eleveldb_backend, [
        {{async_put, 5}, {[Self], fun(Context, Bucket, PrimaryKey, Val, State) ->
            DoingPid = self(),
            Obj = riak_object:from_binary(Bucket, PrimaryKey, Val),
            Values = riak_object:get_value(Obj),
            Self ! {reporting, DoingPid, {Bucket, PrimaryKey, Values}},
            riak_kv_eleveldb_backend_orig:async_put_orig(Context, Bucket, PrimaryKey, Val, State)
        end}}
    ]}),

    C = rt:pbc(Node1),

    lager:info("putting the datas"),
    Data = make_data(),
    ok = riakc_ts:put(C, ?BUCKET, Data),

    lager:info("test pulling the data"),
    % this is to validate our interceptors didn't corrupt or alter the data
    % actually put into eleveldb.
    {_Cols, TupleData} = riakc_ts:query(C,
        "select * from GeoCheckin"
        " where myfamily = 'family1'"
        " and myseries = 'series1'"
        " and time > 0"
        " and time < 100"),
    ?assertEqual(Data, lists:map(fun erlang:tuple_to_list/1, TupleData)),

    Intercepted = receive_intercepts(),
    lager:info("Data Intercepted:"),
    ok = lists:foreach(fun(I) ->
        lager:info("        ~p", [I])
    end, Intercepted),

    lager:info("Data put: ~p", [Data]),

    ?assertEqual(length(Data), length(Intercepted)),
    SimplifiedIntercepted = lists:map(fun(I) ->
        {_Bucket, _Key, ProplistValue} = I,
        lists:map(fun({_, V}) -> V end, ProplistValue)
    end, Intercepted),
    ?assertEqual(Data, SimplifiedIntercepted),

    pass.

build_cluster(Size) ->
    rt:set_backend(eleveldb),
    [_Node1|_] = Nodes = rt:deploy_nodes(Size, []),
    rt:join_cluster(Nodes),
    Nodes.

receive_intercepts() ->
    receive_intercepts([], undefined).

receive_intercepts(Acc, FilterToPid) ->
    receive
        {reporting, MaybePid, Thang} when FilterToPid == undefined orelse MaybePid =:= FilterToPid ->
            receive_intercepts([Thang | Acc], MaybePid);
        {reporting, _, _} ->
            receive_intercepts(Acc, FilterToPid)
    after
        10000 ->
            lists:reverse(Acc)
    end.

-define(DATA_POINTS, 10).
make_data() ->
    TimePoints = lists:seq(1, ?DATA_POINTS),
    lists:map(fun(T) ->
        [<<"family1">>, <<"series1">>, T, <<"weather-like">>, T / 2]
    end, TimePoints).
