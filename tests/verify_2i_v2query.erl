%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
-module(verify_2i_v2query).
-behavior(riak_test).


-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-import(secondary_index_tests, [put_an_object/2, put_an_object/4,
                                int_to_key/1, int_to_field1_bin/1,
                                stream_pb/3, http_query/3, http_stream/3,
                                pb_query/3]).
-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).

confirm() ->
    inets:start(),
    ?assert(eqc:quickcheck(numtests(100, prop_query()))),
    pass.

prop_query() ->
    ?FORALL({Query, Options, Protocol},
            {q(), options(), protocol()},
            begin

                Nodes = rt:build_cluster(3),
                ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

                RiakHttp = rt:http_url(hd(Nodes)),
                PBPid = rt:pbc(hd(Nodes)),

                GetProtocol = fun(http) -> {http, RiakHttp};
                                 (pb) -> {pb, PBPid} end,

                [put_an_object(PBPid, N) || N <- lists:seq(0, 100)],
                [put_an_object(PBPid, int_to_key(N), N, ?FOO) || N <- lists:seq(101, 200)],

                Expected = get_expected(Query, Options),
                Result = execute(GetProtocol(Protocol), Query, Options),
                AllResults = case more_results(Query, Options, Expected) of
                                 true -> get_all_results(Query, Options, Result);
                                 false ->  Result
                             end,
                ?WHENFAIL(
                   begin
                       io:format("Query ~p~n", [Query]),
                       io:format("Options ~p~n", [Options]),
                       io:format("Protocol ~p~n", [Protocol])
                   end,
                   results_equal(Expected, AllResults))
            end).

q() ->
    oneof([range(), eq()]).

range() ->
    ?LET({Field, Start}, {range_field(), range_start()},
         {?BUCKET, Field, range_start(Field, Start), range_end(Field, Start)}).

%% Less than the range size, please
range_start() ->
    ?SUCHTHAT(N, nat(), N < 100).

range_start(<<"$key">>, N) ->
    int_to_key(N);
range_start(<<"field1_bin">>, N) ->
    int_to_field1_bin(N);
range_start(<<"field2_int">>, N) ->
    N.

range_end(<<"$key">>, N) ->
    ?LET(X, choose(N, 100), int_to_key(X));
range_end(<<"field1_bin">>, N) ->
    ?LET(X, choose(N, 100), int_to_field1_bin(X));
range_end(<<"field2_int">>, N) ->
    choose(N, 100).

range_field() ->
    oneof([<<"$key">>, <<"field1_bin">>]).

eq() ->
    ?LET(Field, eq_field(), {?BUCKET, Field, eq(Field)}).

eq_field() ->
    oneof([<<"$bucket">>, <<"$key">>, <<"field1_bin">>, <<"field2_int">>]).

eq(<<"$bucket">>) ->
    ?BUCKET;
eq(<<"$key">>) ->
    ?LET(N, choose(1, 200), int_to_key(N));
eq(<<"field1_bin">>) ->
    ?LET(N, choose(1, 100), oneof([?FOO, int_to_field1_bin(N)]));
eq(<<"field2_int">>) ->
    choose(1, 200).

options() ->
    ?LET({AllOptions, N}, {[stream, max_results(), return_terms()], choose(0, 3)},
         %% get N options from list
         gen_options(AllOptions, N, [])).

gen_options(_, 0, Opts) ->
    Opts;
gen_options(L, N, Opts) ->
    Elem = safe_rand(N),
    L2 = lists:delete(Elem, L),
    gen_options(L2, N-1, [Elem | Opts]).

safe_rand(1) ->
    1;
safe_rand(E) ->
    crypto:rand_uniform(1, E).

max_results() ->
    choose(1, 100).

return_terms() ->
    bool().

protocol() ->
    oneof([pb, http]).

results_equal(Expected, AllResults) ->
    %% TODO
    Expected == AllResults.

get_expected(Query, Options) ->
    %% TODO
    [Query, Options].

execute({http, Http}, Query, Options) ->
    http_query(Http, Query, Options);
execute({pb, Pid}, Query, Options) ->
    pb_query(Pid, Query, Options).

more_results(_Query, _Options, _Expected) ->
    false.

get_all_results(_Query, _Options, _Result) ->
    [].




