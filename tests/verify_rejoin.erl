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
-module(verify_rejoin).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_ITEM_COUNT, 20000).

-define(CFG, 
            [{riak_core,
                [{ring_creation_size, 16}]},
            {riak_kv, 
                [{anti_entropy, {off, []}},
                {tictacaae_active, active},
                {tictacaae_parallelstore, leveled_ko},
                        % if backend not leveled will use parallel key-ordered
                        % store
                {tictacaae_rebuildwait, 4},
                {tictacaae_rebuilddelay, 3600},
                {tictacaae_exchangetick, 10 * 1000}, % 10 seconds
                {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
                {tictacaae_primaryonly, true}]
            }])

confirm() ->
    %% Bring up a 3-node cluster for the test
    Nodes = rt:build_cluster(3, ?CFG),
    [Node1, Node2, Node3] = Nodes,

    rt:systest_write(Node1, 1, ?TEST_ITEM_COUNT),

    lager:info("Have node2 leave and continue to write"),
    rt:leave(Node2),
    rt:systest_write(Node1, ?TEST_ITEM_COUNT + 1, 2 * ?TEST_ITEM_COUNT),
    ?assertEqual(ok, wait_until_unpingable(Node2)),

    lager:info("Have node3 leave and continue to write"),
    rt:leave(Node3),
    rt:systest_write(Node1, 2 * ?TEST_ITEM_COUNT + 1, 3 * ?TEST_ITEM_COUNT),
    ?assertEqual(ok, wait_until_unpingable(Node3)),

    lager:info("Rejoin node2 and continue to write"),
    rt:join(Node2),
    rt:systest_write(Node1, 3 * ?TEST_ITEM_COUNT + 1, 4 * ?TEST_ITEM_COUNT),
    check_joined([Node1, Node2]),

    lager:info("Rejoin node3 and continue to write"),
    rt:join(Node3),
    rt:systest_write(Node1, 4 * ?TEST_ITEM_COUNT + 1, 5 * ?TEST_ITEM_COUNT),
    check_joined([Node1, Node2, Node3]),

    lager:info("Sleeping for manual check of logs")
    timer:sleep(30000),

    lager:info("Check all values read"),
    rt:systest_read(1, 5 * ?TEST_ITEM_COUNT),
    
    pass.


check_joined(Nodes) ->
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),
    ?assertEqual(ok, rt:wait_until_nodes_agree_about_ownership(Nodes)),
