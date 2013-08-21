%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(secondary_index_tests).
-behavior(riak_test).
-export([confirm/0]).
-export([put_an_object/2, put_an_object/4, int_to_key/1,
        stream_pb/2, stream_pb/3, pb_query/3, http_query/2,
         http_query/3, http_stream/3, int_to_field1_bin/1, url/2]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(BUCKET, <<"2ibucket">>).

confirm() ->
    Nodes = rt:build_cluster(3),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    
    Pid = rt:pbc(hd(Nodes)),
    
    [put_an_object(Pid, N) || N <- lists:seq(0, 20)],
    
    assertExactQuery(Pid, [<<"obj5">>], <<"field1_bin">>, <<"val5">>),
    assertExactQuery(Pid, [<<"obj5">>], <<"field2_int">>, <<"5">>),
    assertRangeQuery(Pid, [<<"obj10">>, <<"obj11">>, <<"obj12">>], <<"field1_bin">>, <<"val10">>, <<"val12">>),
    assertRangeQuery(Pid, [<<"obj10">>, <<"obj11">>, <<"obj12">>], <<"field2_int">>, 10, 12),
    assertRangeQuery(Pid, [<<"obj10">>, <<"obj11">>, <<"obj12">>], <<"$key">>, <<"obj10">>, <<"obj12">>),

    lager:info("Delete an object, verify deletion..."),
    ToDel = [<<"obj5">>, <<"obj11">>],
    [?assertMatch(ok, riakc_pb_socket:delete(Pid, ?BUCKET, K)) || K <- ToDel],
    lager:info("Make sure the tombstone is reaped..."),
    ?assertMatch(ok, rt:wait_until(fun() -> rt:pbc_really_deleted(Pid, ?BUCKET, ToDel) end)),
    
    assertExactQuery(Pid, [], <<"field1_bin">>, <<"val5">>),
    assertExactQuery(Pid, [], <<"field2_int">>, <<"5">>),
    assertRangeQuery(Pid, [<<"obj10">>, <<"obj12">>], <<"field1_bin">>, <<"val10">>, <<"val12">>),
    assertRangeQuery(Pid, [<<"obj10">>, <<"obj12">>], <<"field2_int">>, 10, 12),
    assertRangeQuery(Pid, [<<"obj10">>, <<"obj12">>], <<"$key">>, <<"obj10">>, <<"obj12">>),

    %% Verify the $key index, and riak_kv#367 regression
    assertRangeQuery(Pid, [<<"obj6">>], <<"$key">>, <<"obj6">>, <<"obj6">>),
    assertRangeQuery(Pid, [<<"obj6">>, <<"obj7">>], <<"$key">>, <<"obj6">>, <<"obj7">>),

    %% Verify bignum sort order in sext -- eleveldb only (riak_kv#499)
    TestIdxVal = 1362400142028,
    put_an_object(Pid, TestIdxVal),
    assertRangeQuery(Pid,
                     [<<"obj1362400142028">>],
                     <<"field2_int">>,
                     1000000000000,
                     TestIdxVal),
    pass.

assertExactQuery(Pid, Expected, Index, Value) -> 
    lager:info("Searching Index ~p for ~p", [Index, Value]),
    {ok, ?INDEX_RESULTS{keys=Results}} = riakc_pb_socket:get_index(Pid, ?BUCKET, Index, Value),
    ActualKeys = lists:sort(Results),
    lager:info("Expected: ~p", [Expected]),
    lager:info("Actual  : ~p", [ActualKeys]),
    ?assertEqual(Expected, ActualKeys). 

assertRangeQuery(Pid, Expected, Index, StartValue, EndValue) ->
    lager:info("Searching Index ~p for ~p-~p", [Index, StartValue, EndValue]),
    {ok, ?INDEX_RESULTS{keys=Results}} = riakc_pb_socket:get_index(Pid, ?BUCKET, Index, StartValue, EndValue),
    ActualKeys = lists:sort(Results),
    lager:info("Expected: ~p", [Expected]),
    lager:info("Actual  : ~p", [ActualKeys]),
    ?assertEqual(Expected, ActualKeys).

%% general 2i utility
put_an_object(Pid, N) ->
    Key = int_to_key(N),
    BinIndex = int_to_field1_bin(N),
    put_an_object(Pid, Key, N, BinIndex).

put_an_object(Pid, Key, IntIndex, BinIndex) ->
    lager:info("Putting object ~p", [Key]),
    Indexes = [{"field1_bin", BinIndex},
               {"field2_int", IntIndex}],
    MetaData = dict:from_list([{<<"index">>, Indexes}]),
    Robj0 = riakc_obj:new(?BUCKET, Key),
    Robj1 = riakc_obj:update_value(Robj0, io_lib:format("data~p", [IntIndex])),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    riakc_pb_socket:put(Pid, Robj2).

int_to_key(N) ->
    list_to_binary(io_lib:format("obj~p", [N])).

int_to_field1_bin(N) ->
    list_to_binary(io_lib:format("val~p", [N])).

stream_pb(Pid, Q) ->
    pb_query(Pid, Q, [stream]),
    stream_loop().

stream_pb(Pid, Q, Opts) ->
    pb_query(Pid, Q, [stream|Opts]),
    stream_loop().

stream_loop() ->
    stream_loop(orddict:new()).

stream_loop(Acc) ->
    receive
        {_Ref, {done, undefined}} ->
            {ok, orddict:to_list(Acc)};
        {_Ref, {done, Continuation}} ->
            {ok, orddict:store(continuation, Continuation, Acc)};
        {_Ref, ?INDEX_STREAM_RESULT{terms=undefined, keys=Keys}} ->
            Acc2 = orddict:update(keys, fun(Existing) -> Existing++Keys end, Keys, Acc),
            stream_loop(Acc2);
        {_Ref, ?INDEX_STREAM_RESULT{terms=Results}} ->
            Acc2 = orddict:update(results, fun(Existing) -> Existing++Results end, Results, Acc),
            stream_loop(Acc2);
        {_Ref, {error, <<"{error,timeout}">>}} ->
            {error, timeout};
        {_Ref, Wat} ->
            lager:info("got a wat ~p", [Wat]),
            stream_loop(Acc)
    end.

pb_query(Pid, {Field, Val}, Opts) ->
    riakc_pb_socket:get_index_eq(Pid, ?BUCKET, Field, Val, Opts);
pb_query(Pid, {Field, Start, End}, Opts) ->
    riakc_pb_socket:get_index_range(Pid, ?BUCKET, Field, Start, End, Opts).

http_stream(NodePath, Query, Opts) ->
    http_query(NodePath, Query, [{stream, true} | Opts], stream).

http_query(NodePath, Q) ->
    http_query(NodePath, Q, []).

http_query(NodePath, Query, Opts) ->
    http_query(NodePath, Query, Opts, undefined).

http_query(NodePath, {Field, Value}, Opts, Pid) ->
    QString = opts_to_qstring(Opts, []),
    Flag = case is_integer(Value) of true -> "w"; false -> "s" end,
    Url = url("~s/buckets/~s/index/~s/~"++Flag++"~s", [NodePath, ?BUCKET, Field, Value, QString]),
    http_get(Url, Pid);
http_query(NodePath, {Field, Start, End}, Opts, Pid) ->
    QString = opts_to_qstring(Opts, []),
    Flag = case is_integer(Start) of true -> "w"; false -> "s" end,
    Url = url("~s/buckets/~s/index/~s/~"++Flag++"/~"++Flag++"~s", [NodePath, ?BUCKET, Field, Start, End, QString]),
    http_get(Url, Pid).

url(Format, Elements) ->
    Path = io_lib:format(Format, Elements),
    lists:flatten(Path).

http_get(Url, undefined) ->
    lager:info("getting ~p", [Url]),
    {ok,{{"HTTP/1.1",200,"OK"}, _, Body}} = httpc:request(Url),
    {struct, Result} = mochijson2:decode(Body),
    Result;
http_get(Url, stream) ->
    lager:info("streaming ~p", [Url]),
    {ok, Ref} = httpc:request(get, {Url, []}, [], [{stream, self}, {sync, false}]),
    start_http_stream(Ref).

opts_to_qstring([], QString) ->
    QString;
opts_to_qstring([Opt|Rest], []) ->
    QOpt = opt_to_string("?", Opt),
    opts_to_qstring(Rest, QOpt);
opts_to_qstring([Opt|Rest], QString) ->
    QOpt = opt_to_string("&", Opt),
    opts_to_qstring(Rest, QString++QOpt).

opt_to_string(Sep, {Name, Value}) when is_integer(Value) ->
        io_lib:format(Sep++"~s=~p", [Name, Value]);
opt_to_string(Sep, {Name, Value})->
    io_lib:format(Sep++"~s=~s", [Name, url_encode(Value)]);
opt_to_string(Sep, Name) ->
    io_lib:format(Sep++"~s=~s", [Name, true]).

url_encode(Val) when is_binary(Val) ->
    url_encode(binary_to_list(Val));
url_encode(Val) when is_atom(Val) ->
    url_encode(atom_to_list(Val));
url_encode(Val) ->
    ibrowse_lib:url_encode(Val).

start_http_stream(Ref) ->
    receive
        {http, {Ref, stream_start, Headers}} ->
            Boundary = get_boundary(proplists:get_value("content-type", Headers)),
            http_stream_loop(Ref, orddict:new(), Boundary)
    end.

http_stream_loop(Ref, Acc, {Boundary, BLen}=B) ->
    receive
        {http, {Ref, stream_end, _Headers}} ->
            orddict:to_list(Acc);
        {http, {Ref, stream, <<"\r\n--", Boundary:BLen/bytes, "\r\nContent-Type: application/json\r\n\r\n", Body/binary>>}} ->
            ReverseBoundary = reverse_bin(<<"\r\n--", Boundary:BLen/binary, "--\r\n">>),
            Message = get_message(ReverseBoundary, reverse_bin(Body)),
            {struct, Result} = mochijson2:decode(Message),
            Acc2 = lists:foldl(fun({K, V}, A) -> orddict:update(K, fun(Existing) -> Existing++V end, V, A) end,
                               Acc,
                               Result),
            http_stream_loop(Ref, Acc2, B);
        {http, {Ref, stream, <<"\r\n--", Boundary:BLen/bytes, "--\r\n">>}} ->
            http_stream_loop(Ref, Acc, B);
        Other -> lager:error("Unexpected message ~p", [Other]),
                 {error, unknown_message}
    after 60000 ->
            {error, timeout_local}
    end.

get_boundary("multipart/mixed;boundary=" ++ Boundary) ->
    B = list_to_binary(Boundary),
    {B, byte_size(B)};
get_boundary(_) ->
    undefined.

reverse_bin(Bin) ->
    list_to_binary(lists:reverse(binary_to_list(Bin))).

get_message(Boundary, Body) ->
    BLen = byte_size(Boundary),
    case Body of
        <<Boundary:BLen/binary, Message/binary>> ->
            reverse_bin(Message);
        _ -> reverse_bin(Body)
    end.


