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

-include("include/rt.hrl").

-export([confirm/0]).
-export([confirm/1]).
-export([put_an_object/3, put_an_object/5, int_to_key/1,
         stream_pb/3, stream_pb/4, pb_query/4, http_query/3,
         http_query/4, http_stream/4, int_to_field1_bin/1, url/2,
         assertExactQuery/6, assertRangeQuery/8]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(KEYS(A), [int_to_key(A)]).
-define(KEYS(A,B), [int_to_key(N) || N <- lists:seq(A,B)]).
-define(KEYS(A,B,C), [int_to_key(N) || N <- lists:seq(A,B), C]).
-define(KEYS(A,B,G1,G2), [int_to_key(N) || N <- lists:seq(A,B), G1, G2]).

-define(CONFIG,
        [{riak_kv,
          [{anti_entropy, {off, []}},
           {anti_entropy_build_limit, {100, 500}},
           {anti_entropy_concurrency, 100},
           {anti_entropy_tick, 200}]}]).

confirm() ->
    inets:start(),
    Nodes = rt:build_cluster(3, ?CONFIG),
    run(?MODULE, <<"2i_basic">>, Nodes),
    run(verify_2i_returnterms, <<"2i_returnterms">>, Nodes),
    run(verify_2i_timeout, <<"2i_timeout">>, Nodes),
    run(verify_2i_stream, <<"2i_stream">>, Nodes),
    run(verify_2i_limit, <<"2i_limit">>, Nodes),
    run(verify_2i_aae, <<"2i_aae">>, Nodes).
    
run(Mod, BucketOrBuckets, Nodes) ->
    Buckets = to_list(BucketOrBuckets),
    lager:info("Running test in ~s", [Mod]),
    Exports = Mod:module_info(exports),
    HasSetup = lists:member({setup, 1}, Exports),
    HasCleanup = lists:member({cleanup, 1}, Exports),
    Ctx = #rt_test_context{buckets=Buckets, nodes=Nodes},
    RollbackInfo = HasSetup andalso Mod:setup(Ctx),
    Mod:confirm(Ctx),
    HasCleanup andalso Mod:cleanup(RollbackInfo).

to_list(L) when is_list(L) ->
    L;
to_list(L) ->
    [L].

confirm(#rt_test_context{buckets=[Bucket|_], nodes=Nodes}) ->
    %% First test with sorting non-paginated results off by default 
    SetResult = rpc:multicall(Nodes, application, set_env,
                              [riak_kv, secondary_index_sort_default, false]),
    AOK = [ok || _ <- lists:seq(1, length(Nodes))],
    ?assertMatch({AOK, []}, SetResult),

    PBC = rt:pbc(hd(Nodes)),
    HTTPC = rt:httpc(hd(Nodes)),
    Clients = [{pb, PBC}, {http, HTTPC}],
    
    [put_an_object(PBC, Bucket, N) || N <- lists:seq(0, 20)],
    
    K = fun int_to_key/1,

    assertExactQuery(Clients, Bucket, ?KEYS(5), <<"field1_bin">>, <<"val5">>),
    assertExactQuery(Clients, Bucket, ?KEYS(5), <<"field2_int">>, 5),
    assertExactQuery(Clients, Bucket, ?KEYS(5, 9), <<"field3_int">>, 5),
    assertRangeQuery(Clients, Bucket, ?KEYS(10, 18), <<"field1_bin">>, <<"val10">>, <<"val18">>),
    assertRangeQuery(Clients, Bucket, ?KEYS(12), <<"field1_bin">>, <<"val10">>, <<"val18">>, <<"v...2">>),
    assertRangeQuery(Clients, Bucket, ?KEYS(10, 19), <<"field2_int">>, 10, 19),
    assertRangeQuery(Clients, Bucket, ?KEYS(10, 17), <<"$key">>, <<"obj10">>, <<"obj17">>),
    assertRangeQuery(Clients, Bucket, ?KEYS(12), <<"$key">>, <<"obj10">>, <<"obj17">>, <<"ob..2">>),

    lager:info("Delete an object, verify deletion..."),
    ToDel = [<<"obj05">>, <<"obj11">>],
    [?assertMatch(ok, riakc_pb_socket:delete(PBC, Bucket, KD)) || KD <- ToDel],
    lager:info("Make sure the tombstone is reaped..."),
    ?assertMatch(ok, rt:wait_until(fun() -> rt:pbc_really_deleted(PBC, Bucket, ToDel) end)),
    
    assertExactQuery(Clients, Bucket, [], <<"field1_bin">>, <<"val5">>),
    assertExactQuery(Clients, Bucket, [], <<"field2_int">>, 5),
    assertExactQuery(Clients, Bucket, ?KEYS(6, 9), <<"field3_int">>, 5),
    assertRangeQuery(Clients, Bucket, ?KEYS(10, 18, N /= 11), <<"field1_bin">>, <<"val10">>, <<"val18">>),
    assertRangeQuery(Clients, Bucket, ?KEYS(10), <<"field1_bin">>, <<"val10">>, <<"val18">>, <<"10$">>),
    assertRangeQuery(Clients, Bucket, ?KEYS(10, 19, N /= 11), <<"field2_int">>, 10, 19),
    assertRangeQuery(Clients, Bucket, ?KEYS(10, 17, N /= 11), <<"$key">>, <<"obj10">>, <<"obj17">>),
    assertRangeQuery(Clients, Bucket, ?KEYS(12), <<"$key">>, <<"obj10">>, <<"obj17">>, <<"2">>),

    %% Verify the $key index, and riak_kv#367 regression
    assertRangeQuery(Clients, Bucket, ?KEYS(6), <<"$key">>, <<"obj06">>, <<"obj06">>),
    assertRangeQuery(Clients, Bucket, ?KEYS(6,7), <<"$key">>, <<"obj06">>, <<"obj07">>),

    %% Exercise sort set to true by default
    SetResult2 = rpc:multicall(Nodes, application, set_env,
                               [riak_kv, secondary_index_sort_default, true]),
    ?assertMatch({AOK, []}, SetResult2),
                                                    
    assertExactQuery(Clients, Bucket, ?KEYS(15, 19),
                     <<"field3_int">>, 15, {undefined, true}),
    %% Keys ordered by val index term, since 2i order is {term, key}
    KsVal = [A || {_, A} <-
                  lists:sort([{int_to_field1_bin(N), K(N)} ||
                        N <- lists:seq(0, 20), N /= 11, N /= 5])],
    assertRangeQuery(Clients, Bucket, KsVal,
                     <<"field1_bin">>, <<"val0">>, <<"val9">>, undefined, {undefined, true}),
    assertRangeQuery(Clients, Bucket, ?KEYS(0, 20, N /= 11, N /= 5),
                     <<"field2_int">>, 0, 20, undefined, {undefined, true}),
    assertRangeQuery(Clients, Bucket, ?KEYS(0, 20, N /= 11, N /= 5),
                     <<"$key">>, <<"obj00">>, <<"obj20">>, undefined, {undefined, true}),

    %% Verify bignum sort order in sext -- eleveldb only (riak_kv#499)
    TestIdxVal = 1362400142028,
    put_an_object(PBC, Bucket, TestIdxVal),
    assertRangeQuery(Clients, Bucket,
                     [<<"obj1362400142028">>],
                     <<"field2_int">>,
                     1000000000000,
                     TestIdxVal),

    pass.

assertExactQuery(Clients, Bucket, Expected, Index, Value) ->
    assertExactQuery(Clients, Bucket, Expected, Index, Value, {false, false}),
    assertExactQuery(Clients, Bucket, Expected, Index, Value, {true, true}).

assertExactQuery(Clients, Bucket, Expected, Index, Value, Sorted) when is_list(Clients) ->
    [assertExactQuery(C, Bucket, Expected, Index, Value, Sorted) || C <- Clients];
assertExactQuery({ClientType, Client}, Bucket, Expected, Index, Value,
                 {Sort, ExpectSorted}) -> 
    lager:info("Searching Index ~p for ~p in bucket ~p, sort: ~p ~p with client ~p",
               [Index, Value, Bucket, Sort, ExpectSorted, ClientType]),
    {ok, ?INDEX_RESULTS{keys=Results}} = case ClientType of
        pb ->
             riakc_pb_socket:get_index_eq(Client, Bucket, Index, Value,
                                          [{pagination_sort, Sort} || Sort /= undefined]);
        http ->
            rhc:get_index(Client, Bucket, Index, Value, [{pagination_sort, Sort}])
    end,
            
    ActualKeys = case ExpectSorted of
        true -> Results;
        _ -> lists:sort(Results)
    end,
    lager:info("Expected: ~p", [Expected]),
    lager:info("Actual  : ~p", [Results]),
    lager:info("Sorted  : ~p", [ActualKeys]),
    ?assertEqual(Expected, ActualKeys). 

assertRangeQuery(Clients, Bucket, Expected, Index, StartValue, EndValue) ->
    assertRangeQuery(Clients, Bucket, Expected, Index, StartValue, EndValue, undefined).

assertRangeQuery(Clients, Bucket, Expected, Index, StartValue, EndValue, Re) ->
    assertRangeQuery(Clients, Bucket, Expected, Index, StartValue, EndValue, Re, {false, false}),
    assertRangeQuery(Clients, Bucket, Expected, Index, StartValue, EndValue, Re, {true, true}).
    
assertRangeQuery(Clients, Bucket, Expected, Index, StartValue, EndValue, Re, Sort) when is_list(Clients) ->
    [assertRangeQuery(C, Bucket, Expected, Index, StartValue, EndValue, Re, Sort) || C <- Clients];
assertRangeQuery({ClientType, Client}, Bucket, Expected, Index, StartValue, EndValue, Re,
                 {Sort, ExpectSorted}) ->
    lager:info("Searching Index ~p in bucket ~p for ~p-~p re:~p, sort: ~p, ~p with ~p client",
               [Index, Bucket, StartValue, EndValue, Re, Sort, ExpectSorted, ClientType]),
    {ok, ?INDEX_RESULTS{keys=Results}} = case ClientType of
        pb ->
            riakc_pb_socket:get_index_range(Client, Bucket, Index, StartValue, EndValue,
                                            [{term_regex, Re} || Re /= undefined] ++
                                            [{pagination_sort, Sort} || Sort /= undefined]);
        http ->
            rhc:get_index(Client, Bucket, Index, {StartValue, EndValue},
                                            [{term_regex, Re} || Re /= undefined] ++
                                            [{pagination_sort, Sort}])
    end,
    ActualKeys = case ExpectSorted of
        true -> Results;
        _ -> lists:sort(Results)
    end,
    lager:info("Expected: ~p", [Expected]),
    lager:info("Actual  : ~p", [Results]),
    lager:info("Sorted  : ~p", [ActualKeys]),
    ?assertEqual({Bucket, Expected}, {Bucket, ActualKeys}).

%% general 2i utility
put_an_object(Pid, Bucket, N) ->
    Key = int_to_key(N),
    Data = io_lib:format("data~p", [N]),
    BinIndex = int_to_field1_bin(N),
    Indexes = [{"field1_bin", BinIndex},
               {"field2_int", N},
               % every 5 items indexed together
               {"field3_int", N - (N rem 5)}
              ],
    put_an_object(Pid, Bucket, Key, Data, Indexes).

put_an_object(Pid, Bucket, Key, Data, Indexes) when is_list(Indexes) ->
    lager:debug("Putting object ~p", [Key]),
    MetaData = dict:from_list([{<<"index">>, Indexes}]),
    Robj0 = riakc_obj:new(Bucket, Key),
    Robj1 = riakc_obj:update_value(Robj0, Data),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    riakc_pb_socket:put(Pid, Robj2);
put_an_object(Pid, Bucket, Key, IntIndex, BinIndex) when is_integer(IntIndex), is_binary(BinIndex) ->
    put_an_object(Pid, Bucket, Key, Key, [{"field1_bin", BinIndex},{"field2_int", IntIndex}]).

int_to_key(N) ->
    case N < 100 of
        true ->
            list_to_binary(io_lib:format("obj~2..0B", [N]));
        _ ->
            list_to_binary(io_lib:format("obj~p", [N]))
    end.

int_to_field1_bin(N) ->
    list_to_binary(io_lib:format("val~p", [N])).

stream_pb(Pid, Bucket, Q) ->
    pb_query(Pid, Bucket, Q, [stream]),
    stream_loop().

stream_pb(Pid, Bucket, Q, Opts) ->
    pb_query(Pid, Bucket, Q, [stream|Opts]),
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

pb_query(Pid, Bucket, {Field, Val}, Opts) ->
    riakc_pb_socket:get_index_eq(Pid, Bucket, Field, Val, Opts);
pb_query(Pid, Bucket, {Field, Start, End}, Opts) ->
    riakc_pb_socket:get_index_range(Pid, Bucket, Field, Start, End, Opts).

http_stream(NodePath, Bucket, Query, Opts) ->
    http_query(NodePath, Bucket, Query, [{stream, true} | Opts], stream).

http_query(NodePath, Bucket, Q) ->
    http_query(NodePath, Bucket, Q, []).

http_query(NodePath, Bucket, Query, Opts) ->
    http_query(NodePath, Bucket, Query, Opts, undefined).

http_query(NodePath, Bucket, {Field, Value}, Opts, Pid) ->
    QString = opts_to_qstring(Opts, []),
    Flag = case is_integer(Value) of true -> "w"; false -> "s" end,
    Url = url("~s/buckets/~s/index/~s/~"++Flag++"~s", [NodePath, Bucket, Field, Value, QString]),
    http_get(Url, Pid);
http_query(NodePath, Bucket, {Field, Start, End}, Opts, Pid) ->
    QString = opts_to_qstring(Opts, []),
    Flag = case is_integer(Start) of true -> "w"; false -> "s" end,
    Url = url("~s/buckets/~s/index/~s/~"++Flag++"/~"++Flag++"~s", [NodePath, Bucket, Field, Start, End, QString]),
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
            http_stream_loop(Ref, <<>>, Boundary);
        Other -> lager:error("Unexpected message ~p", [Other]),
                 {error, unknown_message}
    after 60000 ->
            {error, timeout_local} 
    end.

http_stream_loop(Ref, Acc, {Boundary, BLen}=B) ->
    receive
        {http, {Ref, stream, Chunk}} ->
            http_stream_loop(Ref, <<Acc/binary,Chunk/binary>>, B);
        {http, {Ref, stream_end, _Headers}} ->
            Parts = binary:split(Acc,[
                        <<"\r\n--", Boundary:BLen/bytes, "\r\nContent-Type: application/json\r\n\r\n">>,
                        <<"\r\n--", Boundary:BLen/bytes,"--\r\n">>
                        ], [global, trim]),
            lists:foldl(fun(<<>>, Results) -> Results;
                    (Part, Results) ->
                        {struct, Result} = mochijson2:decode(Part),
                        orddict:merge(fun(_K, V1, V2) -> V1 ++ V2 end,
                                    Results, Result)
                end, [], Parts);
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

