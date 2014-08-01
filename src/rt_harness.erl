%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(rt_harness).

-define(HARNESS_MODULE, (rt_config:get(rt_harness))).

-export([
        start/1,
        stop/1,
        deploy_clusters/1,
        clean_data_dir/2,
        deploy_nodes/1,
        spawn_cmd/1,
        spawn_cmd/2,
        cmd/1,
        cmd/2,
        setup_harness/2,
        get_deps/0,
        get_version/0,
        get_backends/0,
        set_backend/1,
        whats_up/0,
        get_ip/1,
        node_id/1,
        node_version/1,
        admin/2,
        riak/2,
        attach/2,
        attach_direct/2,
        console/2,
        update_app_config/2,
        teardown/0,
        set_conf/2,
        set_advanced_conf/2]).

start(Node) ->
    ?HARNESS_MODULE:start(Node).

stop(Node) ->
    ?HARNESS_MODULE:stop(Node).
       
deploy_clusters(ClusterConfigs) ->
    ?HARNESS_MODULE:deploy_clusters(ClusterConfigs).
   
clean_data_dir(Nodes, SubDir) ->
    ?HARNESS_MODULE:clean_data_dir(Nodes, SubDir).

spawn_cmd(Cmd) ->
    ?HARNESS_MODULE:spawn_cmd(Cmd).

spawn_cmd(Cmd, Opts) ->
    ?HARNESS_MODULE:spawn_cmd(Cmd, Opts).

cmd(Cmd) ->
    ?HARNESS_MODULE:cmd(Cmd).

cmd(Cmd, Opts) ->
    ?HARNESS_MODULE:cmd(Cmd, Opts).

deploy_nodes(NodeConfig) ->
    ?HARNESS_MODULE:deploy_nodes(NodeConfig).

setup_harness(Test, Args) ->
    ?HARNESS_MODULE:setup_harness(Test, Args).

get_deps() ->
    ?HARNESS_MODULE:get_deps().
get_version() ->
    ?HARNESS_MODULE:get_version().

get_backends() ->
    ?HARNESS_MODULE:get_backends().

set_backend(Backend) ->
    ?HARNESS_MODULE:set_backend(Backend).

whats_up() ->
    ?HARNESS_MODULE:whats_up().

get_ip(Node) ->
    ?HARNESS_MODULE:get_ip(Node).

node_id(Node) ->
    ?HARNESS_MODULE:node_id(Node).

node_version(N) ->
    ?HARNESS_MODULE:node_version(N).

admin(Node, Args) ->
    ?HARNESS_MODULE:admin(Node, Args).

riak(Node, Args) ->
    ?HARNESS_MODULE:riak(Node, Args).

attach(Node, Expected) ->
    ?HARNESS_MODULE:attach(Node, Expected).

attach_direct(Node, Expected) ->
    ?HARNESS_MODULE:attach_direct(Node, Expected).

console(Node, Expected) ->
    ?HARNESS_MODULE:console(Node, Expected).

update_app_config(Node, Config) ->
    ?HARNESS_MODULE:update_app_config(Node, Config).

teardown() ->
    ?HARNESS_MODULE:teardown().

set_conf(Node, NameValuePairs) ->
    ?HARNESS_MODULE:set_conf(Node, NameValuePairs).

set_advanced_conf(Node, NameValuePairs) ->
    ?HARNESS_MODULE:set_advanced_conf(Node, NameValuePairs).
