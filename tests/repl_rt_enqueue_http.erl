%% -------------------------------------------------------------------
%%
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
%%

%% This tests that the rt_enqueue feature of riak-2.2.X works. The
%% feature exposes an API function `rt_enqueue' that reads an object
%% from riak,and pops it onto the realtime repl queue. This API can be
%% used, for example, to touch an object that has missed rt repl
%% (dropped?) or to drive some external reconcilliation method (tictac
%% aae difference?)

-module(repl_rt_enqueue_http).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"test-bucket">>).
-define(KEY(I), <<I:32/integer>>).
-define(KEY_ONE, ?KEY(1)).
-define(KEY_TWO, ?KEY(2)).
-define(VAL(I), ?KEY(I)).

confirm() ->
    NumNodes = rt_config:get(num_nodes, 6),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
              %% turn off fullsync
              {fullsync_on_connect, false},
              {fullsync_interval, disabled}
             ]}
           ],

    Nodes = rt:deploy_nodes(NumNodes, Conf, [riak_kv, riak_repl]),
    {ANodes, BNodes} = lists:split(3, Nodes),

    _KeyRange = {First, Last} = {1, 20},

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    lager:info("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    lager:info("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    lager:info("Naming A"),
    repl_util:name_cluster(AFirst, "A"),

    lager:info("Naming B"),
    repl_util:name_cluster(BFirst, "B"),

    connect_clusters(AFirst, BFirst),

    {ok, CA} = riak:client_connect(AFirst),
    {ok, CB} = riak:client_connect(BFirst),
    HTTPCA = rt:httpc(AFirst),

    %% write a bunch of keys to A
    WriteRes = rt:systest_write(AFirst, First, Last, ?TEST_BUCKET, 2),
    ?assertEqual([], WriteRes),

    %% All are on A, right?
    AReadRes = rt:systest_read(AFirst, First, Last, ?TEST_BUCKET, 2),
    ?assertEqual([], AReadRes),

    %% None are on B, right?
    BReadRes = rt:systest_read(BFirst, First, Last, ?TEST_BUCKET, 2),
    assertAllNotFound(BReadRes, First, Last),

    %% check that error notfound if you touch a notfound key
    RTERes1 = rhc:rt_enqueue(HTTPCA, ?TEST_BUCKET, ?KEY((Last*2))),
    ?assertEqual({error, notfound}, RTERes1),

    %% check for error no repl, since it's not ON yet
    RTERes2 = rhc:rt_enqueue(HTTPCA, ?TEST_BUCKET, ?KEY(First)),
    ?assertEqual({error, realtime_not_enabled}, RTERes2),

    %% enable realtime
    enable_rt(AFirst, ANodes),

    %% write new key to A
    Obj3 = riak_object:new(?TEST_BUCKET, ?KEY((Last+1)), ?VAL((Last+1))),
    WriteRes3 = CA:put(Obj3, [{w, 2}]),
    ?assertEqual(ok, WriteRes3),

    %% verify you can read from B now
    ReplRead =
        rt:wait_until(fun() ->
                              {BReadRes3, _} = CB:get(?TEST_BUCKET, ?KEY((Last+1)), [{r, 3}]),
                              lager:info("waiting for 'realtime repl' to repl"),
                              BReadRes3 == ok
                      end, 10, 200),
    ?assertEqual(ok, ReplRead),

    BReReadRes1 = CB:get(?TEST_BUCKET, ?KEY(First), []),
    ?assertEqual({error, notfound}, BReReadRes1),

    %% touch an original key
    EnqRes = rhc:rt_enqueue(HTTPCA, ?TEST_BUCKET, ?KEY(First), []),
    ?assertEqual(ok, EnqRes),

    %% verify read touched from B
    TouchRead =
        rt:wait_until(fun() ->
                              {BReReadResPresent, _} = CB:get(?TEST_BUCKET, ?KEY(First), []),
                              lager:info("waiting for touch to repl"),
                              BReReadResPresent == ok
                      end, 10, 200),
    ?assertEqual(ok, TouchRead),

    %% touch an original key with the PB client
    PBEnqRes = rhc:rt_enqueue(HTTPCA, ?TEST_BUCKET, ?KEY((First+1)), [{r, 2}]),
    ?assertEqual(ok, PBEnqRes),

    TouchRead2 =
        rt:wait_until(fun() ->
                              {BReReadResPresent, _} = CB:get(?TEST_BUCKET, ?KEY((First+1)), []),
                              lager:info("waiting for touch to repl"),
                              BReReadResPresent == ok
                      end, 10, 200),
    ?assertEqual(ok, TouchRead2),

    %% touch an original key with the HTTP client
    HTTPEnqRes = rhc:rt_enqueue(HTTPCA, ?TEST_BUCKET, ?KEY((First+9))),
    ?assertEqual(ok, HTTPEnqRes),

    TouchRead3 =
        rt:wait_until(fun() ->
                              {BReReadResPresent, _} = CB:get(?TEST_BUCKET, ?KEY((First+9)), []),
                              lager:info("waiting for touch to repl"),
                              BReReadResPresent == ok
                      end, 10, 200),
    ?assertEqual(ok, TouchRead3),

    %% But still not object 3, neither repl'd nor touched
    BReReadRes4 = CB:get(?TEST_BUCKET, ?KEY((First+2)), []),
    ?assertEqual({error, notfound}, BReReadRes4),

    pass.

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    lager:info("Connect cluster A:~p to B on port ~p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port).

%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_rt(LeaderA, ANodes) ->
    lager:info("Enabling RT replication: ~p ~p.", [LeaderA, ANodes]),
    repl_util:enable_realtime(LeaderA, "B"),
    repl_util:start_realtime(LeaderA, "B").

assertAllNotFound(RTSysTestReadRes, Start, End) ->
    {Keys, Vals}  = lists:unzip(RTSysTestReadRes),
    ?assertEqual(lists:seq(End, Start, -1), Keys),
    ?assert(lists:all(fun(E) -> {error, notfound} == E end, Vals)).
