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

-module(verify_tick_change).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    ClusterSize = 4,
    rt_config:set_conf(all, [{"buckets.default.allow_mult", "false"}]),
    NewConfig = [],
    Nodes = rt_cluster:build_cluster(ClusterSize, NewConfig),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    [Node1|_] = Nodes,
    Bucket = <<"systest">>,
    Start = 0, End = 100,
    W = quorum,
    NewTime = 11,

    write_stuff(Nodes, Start, End, Bucket, W, <<>>),
    read_stuff(Nodes, Start, End, Bucket, W, <<>>),

    io:format("Start ticktime daemon on ~p, then wait a few seconds\n",[Node1]),
    rpc:call(Node1, riak_core_net_ticktime, start_set_net_ticktime_daemon,
             [Node1, NewTime]),
    timer:sleep(2*1000),

    io:format("Changing net_ticktime to ~p\n", [NewTime]),
    ok = rt:wait_until(
           fun() ->
                   write_read_poll_check(Nodes, NewTime, Start, End, Bucket, W)
           end),
    lager:info("If we got this far, then we found no inconsistencies\n"),
    [begin
         RemoteTime = rpc:call(Node, net_kernel, get_net_ticktime, []),
         io:format("Node ~p tick is ~p\n", [Node, RemoteTime]),
         ?assertEqual(NewTime, RemoteTime)
     end || Node <- lists:usort([node()|nodes(connected)])],
    io:format("If we got this far, all nodes are using the same tick time\n"),

    pass.

make_common() ->
    list_to_binary(io_lib:format("~p", [now()])).

write_stuff(Nodes, Start, End, Bucket, W, Common) ->
    Nd = lists:nth(length(Nodes), Nodes),
    [] = rt:systest_write(Nd, Start, End, Bucket, W, Common).

read_stuff(Nodes, Start, End, Bucket, W, Common) ->
    Nd = lists:nth(length(Nodes), Nodes),
    [] = rt:systest_read(Nd, Start, End, Bucket, W, Common).

is_set_net_ticktime_done(Nodes, Time) ->
    case lists:usort([(catch rpc:call(Node, net_kernel, get_net_ticktime,[]))
                      || Node <- Nodes]) of
        [Time] ->
            true;
        _ ->
            false
    end.

write_read_poll_check(Nodes, NewTime, Start, End, Bucket, W) ->
    Common = make_common(),
    write_stuff(Nodes, Start, End, Bucket, W, Common),
    read_stuff(Nodes, Start, End, Bucket, W, Common),
    is_set_net_ticktime_done(Nodes, NewTime).
