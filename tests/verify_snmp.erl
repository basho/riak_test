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

confirm() ->
    %% Bring up a small cluster
    [Node1] = rt:deploy_nodes(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),

    lager:info("perform 5 x  PUT and a GET to increment the stats"),
    lager:info("as the stat system only does calcs for > 5 readings"),

    %% TODO: Add cats/pugs to test other stats
    C = rt:httpc(Node1),
    [rt:httpc_write(C, <<"systest">>, <<X>>, <<"12345">>) || X <- lists:seq(1, 5)],
    [rt:httpc_read(C, <<"systest">>, <<X>>) || X <- lists:seq(1, 5)],
    timer:sleep(10000),
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
    verify_eq(Stats, Keys),
    pass.

verify_eq(Stats, Keys) ->
    [begin
         Http = proplists:get_value(StatKey, Stats),
         Snmp = get_snmp(SnmpKey),
         lager:info("Name: ~p HTTP: ~p SNMP: ~p", [SnmpKey, Http, Snmp]),
         ?assertEqual(Http, Snmp)
     end || {SnmpKey, StatKey} <- Keys].

get_stats(Node) ->
    StatString = os:cmd(io_lib:format("curl -s -S ~s/stats", [rt:http_url(Node)])),
    {struct, Stats} = mochijson2:decode(StatString),
    Stats.

get_snmp(StatName) ->
    {value, OID} = snmpa:name_to_oid(StatName),
    [Stat] = snmpa:get(snmp_master_agent, [OID ++ [0]]),
    Stat.
