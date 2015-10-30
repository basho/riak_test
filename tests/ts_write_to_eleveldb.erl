-module(ts_write_to_eleveldb).
-compile({parse_transform, rt_intercept_pt}).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"GeoCheckin">>).

confirm() ->

    lager:info("Building cluster"),
    [Node | _] = rt:deploy_nodes(_ClusterSize = 1, _Config = []),

    lager:info("Creating and activating bucket"),
    DDL = timeseries_util:get_ddl(docs),
    {ok, _} = timeseries_util:create_bucket(Node, DDL, 3),
    _ = timeseries_util:activate_bucket(Node, DDL),

    lager:info("installing interceoptor"),
    Self = self(),

    %% Based on experimentation, the path used for puts in ts is async.
    ok = rt_intercept:add(Node, {riak_kv_eleveldb_backend, [
        {{async_put, 5}, {[Self], fun(Context, Bucket, PrimaryKey, Val, State) ->
            DoingPid = self(),
            Obj = riak_object:from_binary(Bucket, PrimaryKey, Val),
            Values = riak_object:get_value(Obj),
            Self ! {reporting, DoingPid, {Bucket, PrimaryKey, Values}},
            riak_kv_eleveldb_backend_orig:async_put_orig(Context, Bucket, PrimaryKey, Val, State)
        end}}
    ]}),

    C = rt:pbc(Node),

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
