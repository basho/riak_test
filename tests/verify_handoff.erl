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
-module(verify_handoff).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% We've got a separate test for capability negotiation and other mechanisms, so the test here is fairly
%% straightforward: get a list of different versions of nodes and join them into a cluster, making sure that
%% each time our data has been replicated:
confirm() ->
    NTestItems    = 1000,                                   %% How many test items to write/verify?
    NTestNodes    = 3,                                      %% How many nodes to spin up for tests?
    TestMode      = false,                                  %% Set to false for "production tests", true if too slow.
    EncodingTypes = [default, encode_raw, encode_zlib],     %% Usually, you won't want to fiddle with these.

    [run_test(TestMode, NTestItems, NTestNodes, EncodingType) ||
        EncodingType <- EncodingTypes],

    lager:info("Test verify_handoff passed."),
    pass.

run_test(TestMode, NTestItems, NTestNodes, Encoding) ->
    lager:info("Testing handoff (items ~p, encoding: ~p)", [NTestItems, Encoding]),

    %% This resets nodes, cleans up stale directories, etc.:
    lager:info("Cleaning up..."),
    rt:setup_harness(dummy, dummy),

    lager:info("Spinning up test nodes"),
    [RootNode | TestNodes] = Nodes = deploy_test_nodes(TestMode, NTestNodes),

    rt:wait_for_service(RootNode, riak_kv),

    set_handoff_encoding(Encoding, Nodes),

    %% Insert delay into handoff folding to test the efficacy of the
    %% handoff heartbeat addition
    [rt_intercept:add(N, {riak_core_handoff_sender,
                          [{{visit_item, 3}, delayed_visit_item_3}]})
     || N <- Nodes],

    lager:info("Populating root node."),
    rt_systest:write(RootNode, NTestItems),
    %% write one object with a bucket type
    rt_bucket_types:create_and_activate_bucket_type(RootNode, <<"type">>, []),
    %% allow cluster metadata some time to propogate
    rt_systest:write(RootNode, 1, 2, {<<"type">>, <<"bucket">>}, 2),

    %% Test handoff on each node:
    lager:info("Testing handoff for cluster."),
    lists:foreach(fun(TestNode) -> test_handoff(RootNode, TestNode, NTestItems) end, TestNodes),

    %% Prepare for the next call to our test (we aren't polite about it, it's faster that way):
    lager:info("Bringing down test nodes."),
    lists:foreach(fun(N) -> rt_node:brutal_kill(N) end, TestNodes),

    %% The "root" node can't leave() since it's the only node left:
    lager:info("Stopping root node."),
    rt_node:brutal_kill(RootNode).

set_handoff_encoding(default, _) ->
    lager:info("Using default encoding type."),
    true;
set_handoff_encoding(Encoding, Nodes) ->
    lager:info("Forcing encoding type to ~p.", [Encoding]),

    %% Update all nodes (capabilities are not re-negotiated):
    [begin
         rt:update_app_config(Node, override_data(Encoding)),
         assert_using(Node, {riak_kv, handoff_data_encoding}, Encoding)
     end || Node <- Nodes].

override_data(Encoding) ->
    [
     { riak_core,
       [
        { override_capability,
          [
           { handoff_data_encoding,
             [
              {    use, Encoding},
              { prefer, Encoding}
             ]
           }
          ]
        }
       ]}].

%% See if we get the same data back from our new nodes as we put into the root node:
test_handoff(RootNode, NewNode, NTestItems) ->

    lager:info("Waiting for service on new node."),
    rt:wait_for_service(NewNode, riak_kv),

    lager:info("Joining new node with cluster."),
    rt_node:join(NewNode, RootNode),
    ?assertEqual(ok, rt_node:wait_until_nodes_ready([RootNode, NewNode])),
    rt:wait_until_no_pending_changes([RootNode, NewNode]),

    %% See if we get the same data back from the joined node that we added to the root node.
    %%  Note: systest_read() returns /non-matching/ items, so getting nothing back is good:
    lager:info("Validating data after handoff:"),
    Results = rt_systest:read(NewNode, NTestItems), 
    ?assertEqual(0, length(Results)), 
    Results2 = rt_systest:read(RootNode, 1, 2, {<<"type">>, <<"bucket">>}, 2),
    ?assertEqual(0, length(Results2)),
    lager:info("Data looks ok.").

assert_using(Node, {CapabilityCategory, CapabilityName}, ExpectedCapabilityName) ->
    lager:info("assert_using ~p =:= ~p", [ExpectedCapabilityName, CapabilityName]),
    ExpectedCapabilityName =:= rt:capability(Node, {CapabilityCategory, CapabilityName}).

%% For some testing purposes, making these limits smaller is helpful:
deploy_test_nodes(false, N) -> 
    rt_cluster:deploy_nodes(N);
deploy_test_nodes(true,  N) ->
    lager:info("WARNING: Using turbo settings for testing."),
    Config = [{riak_core, [{forced_ownership_handoff, 8},
                           {ring_creation_size, 8},
                           {handoff_concurrency, 8},
                           {vnode_inactivity_timeout, 1000},
                           {handoff_acksync_threshold, 20},
                           {handoff_receive_timeout, 2000},
                           {gossip_limit, {10000000, 60000}}]}],
    rt_cluster:deploy_nodes(N, Config).
