%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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
%% -------------------------------------------------------------------
-module(rt_cascading_ensure_ack).
-behavior(riak_test).

%% API
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    State = ensure_ack_setup(),
    _ = ensure_ack_tests(State),
    pass.

ensure_ack_setup() ->
    Clusters = rt_cascading:make_clusters([{"A", 1, ["B"]}, {"B", 1}]),
    lists:flatten([Nodes || {_, Nodes} <- Clusters]).

ensure_ack_tests(Nodes) ->
    [LeaderA, LeaderB] = Nodes,

    lager:info("Nodes:~p, ~p", [LeaderA, LeaderB]),
    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
        <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-rt_test_a">>,

    %% Write some objects to the source cluster (A),
    lager:info("Writing 1 key to ~p, which should RT repl to ~p",
        [LeaderA, LeaderB]),
    ?assertEqual([], repl_util:do_write(LeaderA, 1, 1, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading 1 key written from ~p", [LeaderB]),
    ?assertEqual(0, repl_util:wait_for_reads(LeaderB, 1, 1, TestBucket, 2)),
    lager:info("Checking unacked on ~p", [LeaderA]),
    ?assertEqual(ok, rt:wait_until(fun () -> check_unacked(LeaderA) end)).

check_unacked(LeaderA) ->
    RTQStatus = rpc:call(LeaderA, riak_repl2_rtq, status, []),
    Consumers = proplists:get_value(consumers, RTQStatus),
    case proplists:get_value("B", Consumers) of
        undefined ->
            missing_consumer;
        Consumer ->
            Unacked = proplists:get_value(unacked, Consumer, 0),
            lager:info("unacked: ~p", [Unacked]), 
            Unacked == 0
    end.
