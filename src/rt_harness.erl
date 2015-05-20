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
%% @doc rt_harness provides a level of indirection between the modules
%% calling into the harness and the configured harness, resolving the call
%% to the configured harness. Calls such as `rt_harness:start(Node)' will
%% be resolved to the configured harness.
-module(rt_harness).

-define(HARNESS_MODULE, (rt_config:get(rt_harness))).

-export([start/2,
         stop/2,
         deploy_clusters/1,
         clean_data_dir/3,
         deploy_nodes/1,
         available_resources/0,
         spawn_cmd/1,
         spawn_cmd/2,
         cmd/1,
         cmd/2,
         setup/0,
         get_deps/0,
         get_node_logs/0,
         get_node_logs/2,
         get_version/0,
         get_version/1,
         get_backends/0,
         set_backend/1,
         whats_up/0,
         get_ip/1,
         node_id/1,
         node_version/1,
         admin/2,
         admin/3,
         riak/2,
         riak_repl/2,
         run_riak/3,
         attach/2,
         attach_direct/2,
         console/2,
         update_app_config/3,
         teardown/0,
         set_conf/2,
         set_advanced_conf/2,
         update_app_config/2,
         upgrade/2,
         validate_config/1]).

start(Node, Version) ->
    ?HARNESS_MODULE:start(Node, Version).

stop(Node, Version) ->
    ?HARNESS_MODULE:stop(Node, Version).

deploy_clusters(ClusterConfigs) ->
    ?HARNESS_MODULE:deploy_clusters(ClusterConfigs).

clean_data_dir(Node, Version, SubDir) ->
    ?HARNESS_MODULE:clean_data_dir(Node, Version, SubDir).

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

available_resources() ->
    ?HARNESS_MODULE:available_resources().

setup() ->
    ?HARNESS_MODULE:setup_harness().

get_deps() ->
    ?HARNESS_MODULE:get_deps().

get_version() ->
    ?HARNESS_MODULE:get_version().

get_version(Node) ->
    ?HARNESS_MODULE:get_version(Node).

get_backends() ->
    ?HARNESS_MODULE:get_backends().

get_node_logs() ->
    ?HARNESS_MODULE:get_node_logs().

get_node_logs(LogFile, DestDir) ->
    ?HARNESS_MODULE:get_node_logs(LogFile, DestDir).

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

admin(Node, Args, Options) ->
    ?HARNESS_MODULE:admin(Node, Args, Options).

riak(Node, Args) ->
    ?HARNESS_MODULE:riak(Node, Args).

riak_repl(Node, Args) ->
    ?HARNESS_MODULE:riak_repl(Node, Args).

run_riak(Node, Version, Command) ->
    ?HARNESS_MODULE:run_riak(Node, Version, Command).

attach(Node, Expected) ->
    ?HARNESS_MODULE:attach(Node, Expected).

attach_direct(Node, Expected) ->
    ?HARNESS_MODULE:attach_direct(Node, Expected).

console(Node, Expected) ->
    ?HARNESS_MODULE:console(Node, Expected).

update_app_config(Node, Version, Config) ->
    ?HARNESS_MODULE:update_app_config(Node, Version, Config).

teardown() ->
    ?HARNESS_MODULE:teardown().

set_conf(Node, NameValuePairs) ->
    ?HARNESS_MODULE:set_conf(Node, NameValuePairs).

set_advanced_conf(Node, NameValuePairs) ->
    ?HARNESS_MODULE:set_advanced_conf(Node, NameValuePairs).

update_app_config(Node, Config) ->
    ?HARNESS_MODULE:update_app_config(Node, Config).

upgrade(Node, NewVersion) ->
    ?HARNESS_MODULE:upgrade(Node, NewVersion).

validate_config(Versions) ->
    ?HARNESS_MODULE:validate_config(Versions).
