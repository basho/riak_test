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
-module(rt_cascading_unacked_and_queue).
-behavior(riak_test).

%% API
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    LeadersAndClusters = ensure_unacked_and_queue_setup(),
    TestBucket = rt_cascading:generate_test_bucket(),
    _ = ensure_unacked_and_queue_tests(LeadersAndClusters, TestBucket),
    pass.

ensure_unacked_and_queue_setup() ->
    Clusters = rt_cascading:make_clusters([{"n123", 3, ["n456"]}, {"n456", 3, ["n123"]}]),
    %% Determine the actual leader for each cluster
    LeadersAndClusters = lists:map(fun({_Name, Cluster}) ->
                  {repl_util:get_leader(hd(Cluster)), Cluster}
              end, Clusters),
    LeadersAndClusters.

ensure_unacked_and_queue_tests([{N123Leader, N123}, {N456Leader, N456}], TestBucket) ->
    Tests = [

        {"unacked does not increase when there are skips", fun() ->
            rt_cascading:write_n_keys(N123Leader, N456Leader, TestBucket, 1, 10000),

            rt_cascading:write_n_keys(N456Leader, N123Leader, TestBucket, 10001, 20000),

            Res = rt:wait_until(fun() ->
                RTQStatus = rpc:call(N123Leader, riak_repl2_rtq, status, []),

                Consumers = proplists:get_value(consumers, RTQStatus),
                Data = proplists:get_value("n456", Consumers),
                Unacked = proplists:get_value(unacked, Data),
                ?debugFmt("unacked: ~p", [Unacked]),
                0 == Unacked
                                end),
            ?assertEqual(ok, Res)
                                                           end},

        {"after acks, queues are empty", fun() ->
            Nodes = N123 ++ N456,
            Got = lists:map(fun(Node) ->
                rpc:call(Node, riak_repl2_rtq, all_queues_empty, [])
                            end, Nodes),
            Expected = [true || _ <- lists:seq(1, length(Nodes))],
            ?assertEqual(Expected, Got)
                                         end},

        {"after acks, queues truly are empty. Truly", fun() ->
            Nodes = N123 ++ N456,
            Gots = lists:map(fun(Node) ->
                {Node, rpc:call(Node, riak_repl2_rtq, dumpq, [])}
                             end, Nodes),
            lists:map(fun({Node, Got}) ->
                ?debugFmt("Checking data from ~p", [Node]),
                ?assertEqual([], Got)
                      end, Gots)
                                                      end},

        {"dual loads keeps unacked satisfied", fun() ->
            LoadN123Pid = spawn(fun() ->
                {Time, Val} = timer:tc(fun rt_cascading:write_n_keys/5, [N123Leader, N456Leader, TestBucket, 20001, 30000]),
                ?debugFmt("loading 123 to 456 took ~p to get ~p", [Time, Val]),
                Val
                                end),
            LoadN456Pid = spawn(fun() ->
                {Time, Val} = timer:tc(fun rt_cascading:write_n_keys/5, [N456Leader, N123Leader, TestBucket, 30001, 40000]),
                ?debugFmt("loading 456 to 123 took ~p to get ~p", [Time, Val]),
                Val
                                end),
            Exits = rt_cascading:wait_exit([LoadN123Pid, LoadN456Pid], infinity),
            ?assert(lists:all(fun(E) -> E == normal end, Exits)),

            StatusDig = fun(SinkName, Node) ->
                Status = rpc:call(Node, riak_repl2_rtq, status, []),
                Consumers = proplists:get_value(consumers, Status, []),
                ConsumerStats = proplists:get_value(SinkName, Consumers, []),
                proplists:get_value(unacked, ConsumerStats)
                        end,

            N123UnackedRes = rt:wait_until(fun() ->
                Unacked = StatusDig("n456", N123Leader),
                ?debugFmt("Unacked: ~p", [Unacked]),
                0 == Unacked
                                           end),
            ?assertEqual(ok, N123UnackedRes),

            N456Unacked = StatusDig("n123", N456Leader),
            case N456Unacked of
                0 ->
                    ?assert(true);
                _ ->
                    N456Unacked2 = StatusDig("n123", N456Leader),
                    ?debugFmt("Not 0, are they at least decreasing?~n"
                    "    ~p, ~p", [N456Unacked2, N456Unacked]),
                    ?assert(N456Unacked2 < N456Unacked)
            end
                                               end},

        {"after dual load acks, queues are empty", fun() ->
            Nodes = N123 ++ N456,
            Got = lists:map(fun(Node) ->
                rpc:call(Node, riak_repl2_rtq, all_queues_empty, [])
                            end, Nodes),
            Expected = [true || _ <- lists:seq(1, length(Nodes))],
            ?assertEqual(Expected, Got)
                                                   end},

        {"after dual load acks, queues truly are empty. Truly", fun() ->
            Nodes = N123 ++ N456,
            Gots = lists:map(fun(Node) ->
                {Node, rpc:call(Node, riak_repl2_rtq, dumpq, [])}
                             end, Nodes),
            lists:map(fun({Node, Got}) ->
                ?debugFmt("Checking data from ~p", [Node]),
                ?assertEqual([], Got)
                      end, Gots)
                                                                end},

        {"no negative pendings", fun() ->
            Nodes = N123 ++ N456,
            GetPending = fun({sink_stats, SinkStats}) ->
                ConnTo = proplists:get_value(rt_sink_connected_to, SinkStats),
                proplists:get_value(pending, ConnTo)
                         end,
            lists:map(fun(Node) ->
                ?debugFmt("Checking node ~p", [Node]),
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                Sinks = proplists:get_value(sinks, Status),
                lists:map(fun(SStats) ->
                    Pending = GetPending(SStats),
                    ?assertEqual(0, Pending)
                          end, Sinks)
                      end, Nodes)
                                 end}

    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== ensure_unacked_and_queue: ~s =====", [Name]),
        Eval()
                  end, Tests).
