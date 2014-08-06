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
%% @doc behaviour for all test harnesses. 
-module(test_harness).     

-callback start(Node :: node()) -> 'ok'.
-callback stop(Node :: node()) -> 'ok'.
-callback deploy_clusters(ClusterConfigs :: list()) -> list().
-callback clean_data_dir(Nodes :: list(), SubDir :: string()) -> 'ok'.
-callback spawn_cmd(Cmd :: string()) -> Port :: pos_integer().
-callback spawn_cmd(Cmd :: string(), Opts :: list()) -> Port :: pos_integer().
-callback cmd(Cmd :: string()) -> term()|timeout.
-callback cmd(Cmd :: string(), Opts :: [atom()]) -> term()|timeout.
-callback setup_harness(Test :: string(), Args :: list()) -> 'ok'.
-callback get_version() -> term().
-callback get_backends() -> [atom()].
-callback set_backend(Backend :: atom()) -> [atom()].
-callback whats_up() -> string().
-callback get_ip(Node :: node()) -> string().
-callback node_id(Node :: node()) -> NodeMap :: term().
-callback node_version(N :: node()) -> VersionMap :: term().
-callback admin(Node :: node(), Args :: [atom()]) -> {'ok', string()}.
-callback riak(Node :: node(), Args :: [atom()]) -> {'ok', string()}.
-callback attach(Node :: node(), Expected:: list()) -> 'ok'.
-callback attach_direct(Node :: node(), Expected:: list()) -> 'ok'.
-callback console(Node :: node(), Expected:: list()) -> 'ok'.
-callback update_app_config(atom()|node(), Config :: term()) -> 'ok'.
-callback teardown() -> list().
-callback set_conf(atom()|node(), NameValuePairs :: [{string(), string()}]) -> 'ok'.
-callback set_advanced_conf(atom()|node(), NameValuePairs :: [{string(), string()}]) -> 'ok'.


