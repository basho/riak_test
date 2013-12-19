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

%% !!! DO NOT ADD TO GIDDYUP
%%
%% This module is not meant to be used as an automated CI test. It
%% exists to allow easy benchmarking of AAE rebuild times when making
%% changes that may help/hurt rebuild times.
%%
%% !!! DO NOT ADD TO GIDDYUP

-module(benchmark_aae_rebuild).
-export([confirm/0]).

confirm() ->
    NumKeys = 100000,
    Runs = 4,
    Config = [{riak_kv, [{anti_entropy_build_limit, {100, 1000}},
                         {anti_entropy_concurrency, 100},
                         {anti_entropy_tick, 1000},
                         {anti_entropy, {off, []}}]},
              {riak_core, [{vnode_management_timer, 1000},
                           {ring_creation_size, 4}]}],
    Nodes = rt:deploy_nodes(1, Config),
    Node = hd(Nodes),

    lager:info("Write ~b keys w/ AAE disabled", [NumKeys]),
    %% rt:systest_write(Node, NumKeys),
    bulk_load(Node, NumKeys),

    lager:info("Enable AAE and run tree build experiment"),
    enable_aae(Node),
    Sum = lists:foldl(fun(_, Sum) ->
                              Duration = rebuild(Node),
                              Sum + Duration
                      end, time_build(Node), lists:seq(1, Runs)),
    Avg = Sum div Runs,
    lager:info("Average build time: ~b us", [Avg]),
    pass.

time_build(Node) ->
    T0 = erlang:now(),
    rt:wait_until_aae_trees_built([Node]),
    Duration = timer:now_diff(erlang:now(), T0),
    lager:info("Build took ~b us", [Duration]),
    Duration.

rebuild(Node) ->
    rpc:call(Node, application, set_env,
             [riak_kv, anti_entropy_expire, 0]),
    timer:sleep(1500),
    disable_aae(Node),
    rpc:call(Node, application, set_env,
             [riak_kv, anti_entropy_expire, 10000000]),
    rpc:call(Node, ets, delete_all_objects, [ets_riak_kv_entropy]),
    enable_aae(Node),
    time_build(Node).

enable_aae(Node) ->
    rpc:call(Node, riak_kv_entropy_manager, enable, []).

disable_aae(Node) ->
    rpc:call(Node, riak_kv_entropy_manager, disable, []).

bulk_load(Node, NumKeys) ->
    NumWorkers = 8,
    Ranges = partition_range(1, NumKeys, NumWorkers),
    rt:pmap(fun({Start, Stop}) -> 
                    lager:info("Starting worker ~p: ~p/~p~n", [self(), Start, Stop]),
                    PBC = rt:pbc(Node),
                    Keys = [<<N:64/integer>> || N <- lists:seq(Start, Stop)],
                    [ok = rt:pbc_write(PBC, <<"test">>, Key, Key) || Key <- Keys]
            end, Ranges),
    lager:info("Bulk load finished"),
    ok.

partition_range(Start, End, 1) ->
    [{Start, End}];
partition_range(Start, End, Num) ->
    Span = div_ceiling(End - Start, Num),
    [{RS, erlang:min(RS + Span - 1, End)} || RS <- lists:seq(Start, End, Span)].
 
div_ceiling(A, B) ->
    (A + B - 1) div B.
