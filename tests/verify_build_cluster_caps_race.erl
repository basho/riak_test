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
-module(verify_build_cluster_caps_race).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

-define(NEVER_LIMIT, {0, 1000000000}).
-define(ONCE_LIMIT, {1, 1000000000}).
-define(STORM_LIMIT, {1000, 10000}). % 1000 / 10 secs

confirm() ->
    %% Deploy a set of new nodes
    lager:info("Deploying nodes"),

    %% Disable gossip by starving it of tokens.
    %% Gossip messages can be triggered by sending 'reset_tokens'
    %% message to riak_core_gossip

    Config = [{riak_core, [{gossip_limit, ?NEVER_LIMIT}]}], 
    [Node1, Node2] = Nodes = rt:deploy_nodes(2, Config),

    %% Speed failure up a bit
    rt_config:set(rt_max_wait_time, timer:seconds(120)),
    rt_config:set(rt_retry_delay, 500),

    lager:info("joining Node 2 to the cluster and limiting gossip..."),
    ok = rt:staged_join(Node2, Node1),
    give_token(Node2), % allow the join message to reach
    ok = rt:plan_and_commit(Node1),

    lager:info("setting bogus capability on both sides"),
    [set_bogus_cap(N) || N <- Nodes],

    %% Make sure the ring is *not* ready on all nodes
    [?assertEqual(false, rt:is_ring_ready(N)) || N <- Nodes],

    lager:info("re-enabling gossip"),
    [set_gossip_limit(N, ?STORM_LIMIT) || N <- Nodes],

    %% Look for a happy cluster
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_nodes_agree_about_ownership(Nodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)),

    pass.

%% gossip(From, To) ->
%%     %% First, give the lucky node a token
%%     ok = give_token(From),

%%     %% Then trigger the gossip
%%     rpc:call(From, riak_core_gossip, send_ring_to, [From, To]).

give_token(From) ->
    set_gossip_limit(From, ?ONCE_LIMIT),
    ok.

set_gossip_limit(Node, Limit) ->
    lager:info("Setting ~p gossip limit to ~p\n", [Node, Limit]),
    ok = rpc:call(Node, application, set_env, [riak_core, gossip_limit, Limit]),
    reset_tokens = rpc:call(Node, erlang, send, [{riak_core_gossip, Node}, reset_tokens]).

set_bogus_cap(Node) ->
    ok = rpc:call(Node, riak_core_capability, register, [{riak_core, herding},[goats,cats],cats]).
