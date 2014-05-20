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
-module(verify_2i_timeout).
-behavior(riak_test).

-export([confirm/0]).
-export([confirm/1, setup/1, cleanup/1]).
-import(secondary_index_tests, [put_an_object/3, put_an_object/5, int_to_key/1,
                               stream_pb/4, url/2, http_query/4, http_stream/4]).

-include("include/rt.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(FOO, <<"foo">>).

confirm() ->
    inets:start(),
    Nodes = rt:build_cluster(3),
    Ctx = #rt_test_context{nodes=Nodes, buckets=[<<"2i_timeout">>]},
    setup(Ctx),
    confirm(Ctx).

setup(#rt_test_context{nodes=Nodes}) ->
    lager:debug("Setting ridiculously low 2i timeout"),
    OldVals = [{Node,
                rt:rpc_get_env(Node, [{riak_kv, secondary_index_timeout}])}
                || Node <- Nodes],
    rt:rpc_set_env(Nodes, riak_kv, secondary_index_timeout, 1),
    OldVals.

cleanup(OldVals) ->
    lager:debug("Restoring 2i timeouts"),
    [case OldVal of
         undefined -> rt:rpc_unset_env(Node, riak_kv, secondary_index_timeout);
         _ -> rt:rpc_set_env(Node, riak_kv, secondary_index_timeout, OldVal)
     end
     || {Node, OldVal} <- OldVals].

confirm(#rt_test_context{buckets=[Bucket], nodes=Nodes}) ->
    PBPid = rt:pbc(hd(Nodes)),
    Http = rt:http_url(hd(Nodes)),

    [put_an_object(PBPid, Bucket, N) || N <- lists:seq(0, 100)],
    [put_an_object(PBPid, Bucket, int_to_key(N), N, ?FOO) || N <- lists:seq(101, 200)],

    ExpectedKeys = lists:sort([int_to_key(N) || N <- lists:seq(0, 200)]),
    Query = {<<"$bucket">>, Bucket},
    %% Verifies that the app.config param was used
    ?assertEqual({error, timeout}, stream_pb(PBPid, Bucket, Query, [])),

    %% Override app.config
    {ok, Res} =  stream_pb(PBPid, Bucket, Query, [{timeout, 5000}]),
    ?assertEqual(ExpectedKeys, lists:sort(proplists:get_value(keys, Res, []))),

    {ok, {{_, ErrCode, _}, _, Body}} = httpc:request(url("~s/buckets/~s/index/~s/~s~s",
                                                     [Http, Bucket, <<"$bucket">>, Bucket, []])),

    ?assertEqual(true, ErrCode >= 500),
    ?assertMatch({match, _}, re:run(Body, "request timed out|{error,timeout}")), %% shows the app.config timeout

    HttpRes = http_query(Http, Bucket, Query, [{timeout, 5000}]),
    ?assertEqual(ExpectedKeys, lists:sort(proplists:get_value(<<"keys">>, HttpRes, []))),

    stream_http(Http, Bucket, Query, ExpectedKeys),

    riakc_pb_socket:stop(PBPid),
    pass.

stream_http(Http, Bucket, Query, ExpectedKeys) ->
     Res = http_stream(Http, Bucket, Query, []),
     ?assert(lists:member({<<"error">>,<<"timeout">>}, Res)),
     Res2 = http_stream(Http, Bucket, Query, [{timeout, 5000}]),
     ?assertEqual(ExpectedKeys, lists:sort(proplists:get_value(<<"keys">>, Res2, []))).

