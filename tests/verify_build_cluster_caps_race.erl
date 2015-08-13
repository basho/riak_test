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

staged_join(InitiatingNode, DestinationNode) ->
    rpc:call(InitiatingNode, riak_core, staged_join,
             [DestinationNode]).

confirm() ->
    %% Deploy a set of new nodes
    lager:info("Deploying nodes"),

    [Node1, Node2] = rt:deploy_nodes(2),

    configure_intercept(Node2),

    lager:info("joining Node 2 to the cluster..."),
    ?assertMatch({error, _}, staged_join(Node2, Node1)),
    pass.

%% init must return `starting' status for join to fail
configure_intercept(Node) ->
    lager:info("Doing unspeakably evil things to the VM"),
    rt_intercept:add(Node, {init,
                            [{{get_status,0}, get_status}]}).
