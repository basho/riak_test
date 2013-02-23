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
    NTestItems = 10,        %% How many test items to write/verify?

    lager:info("Deploying mixed set of nodes"),
    Legacy = case lists:member(legacy, rt:versions()) of
        true -> legacy;
        _ -> current   
    end,

    [RootNode | TestNodes] = rt:deploy_nodes([current, Legacy, previous, current]),

    lager:info("Populating root node"),
    rt:wait_for_service(RootNode, riak_kv),
    rt:systest_write(RootNode, NTestItems),

    lager:info("Testing handoff"),
    lists:foreach(fun(TestNode) -> test_handoff(RootNode, TestNode, NTestItems) end, TestNodes),

    lager:info("Test verify_handoff passed."),
    pass.

%%% JFW test_handoff(_, [], _) -> true;
test_handoff(RootNode, NewNode, NTestItems) ->

    lager:info("Waiting for service on new node."),
    rt:wait_for_service(NewNode, riak_kv),

    lager:info("Joining new node with cluster."),
    rt:join(NewNode, RootNode),

    lager:info("Validating data after handoff"),
    rt:systest_read(NewNode, NTestItems).

