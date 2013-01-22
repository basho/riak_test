%% -------------------------------------------------------------------
%%
%% Copyright (c) 2010-2012 Basho Technologies, Inc.
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

-module(verify_snmp).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-prereq("curl").

confirm() ->
    %% Bring up a small cluster
    [Node1] = rt:deploy_nodes(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),

    lager:info("Doing some reads and writes to record some stats."),
    
    rt:systest_write(Node1, 10),
    rt:systest_read(Node1, 10),

    Stats = get_stats(Node1),
    Keys = [{vnodeGets,<<"vnode_gets">>},
            {vnodePuts,<<"vnode_puts">>},
            {nodeGets,<<"node_gets">>},
            {nodePuts,<<"node_puts">>},
            {nodeGetTimeMean,<<"node_get_fsm_time_mean">>},
            {nodeGetTimeMedian,<<"node_get_fsm_time_median">>},
            {nodeGetTime95,<<"node_get_fsm_time_95">>},
            {nodeGetTime99,<<"node_get_fsm_time_99">>},
            {nodeGetTime100,<<"node_get_fsm_time_100">>},
            {nodePutTimeMean,<<"node_put_fsm_time_mean">>},
            {nodePutTimeMedian,<<"node_put_fsm_time_median">>},
            {nodePutTime95,<<"node_put_fsm_time_95">>},
            {nodePutTime99,<<"node_put_fsm_time_99">>},
            {nodePutTime100,<<"node_put_fsm_time_100">>}],
    verify_eq(Stats, Keys, Node1),
    pass.

verify_eq(Stats, Keys, Node) ->
    [ begin
          Http = proplists:get_value(StatKey, Stats),
          lager:info("Waiting until ~p matches HTTP value: ~p", [SnmpKey, Http]),
          ?assertEqual(ok, wait_until_stat_matches(Node, SnmpKey, Http))
      end
      || {SnmpKey, StatKey} <- Keys].

get_stats(Node) ->
    StatString = os:cmd(io_lib:format("curl -s -S ~s/stats", [rt:http_url(Node)])),
    {struct, Stats} = mochijson2:decode(StatString),
    Stats.

get_snmp(Node, StatName) ->
    {value, OID} = rpc:call(Node, snmpa, name_to_oid, [StatName]),
    [Stat] = rpc:call(Node, snmpa, get, [snmp_master_agent, [OID ++ [0]]]),
    Stat.

wait_until_stat_matches(Node, SnmpKey, Http) ->
    rt:wait_until(Node,
                  fun(N) ->
                          Snmp = get_snmp(N, SnmpKey),
                          lager:debug("SNMP: ~p is ~p", [SnmpKey, Snmp]),
                          Http == Snmp
                  end).
