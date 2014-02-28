%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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

-module(rt_cluster).

-export([properties/0,
         setup/2,
         config/0,
         augment_config/3]).
-export([maybe_wait_for_transfers/2]).

-include("rt.hrl").

%% @doc Default properties used if a riak_test module does not specify
%% a custom properties function.
-spec properties() -> rt_properties().
properties() ->
    #rt_properties{config=config()}.

-spec setup(rt_properties(), proplists:proplist()) ->
                   {ok, rt_properties()} | {error, term()}.
setup(Properties, MetaData) ->
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    RollingUpgrade = proplists:get_value(rolling_upgrade,
                                         MetaData,
                                         Properties#rt_properties.rolling_upgrade),
    Version = Properties#rt_properties.start_version,
    Versions = [{Version, Properties#rt_properties.config} ||
                   _ <- lists:seq(1, Properties#rt_properties.node_count)],
    Nodes = deploy_or_build_cluster(Versions, Properties#rt_properties.make_cluster),

    maybe_wait_for_transfers(Nodes, Properties#rt_properties.wait_for_transfers),
    UpdProperties = Properties#rt_properties{nodes=Nodes,
                                             rolling_upgrade=RollingUpgrade},
    {ok, UpdProperties}.

deploy_or_build_cluster(Versions, true) ->
    rt:build_cluster(Versions);
deploy_or_build_cluster(Versions, false) ->
    rt:deploy_nodes(Versions).

maybe_wait_for_transfers(Nodes, true) ->
    lager:info("Waiting for transfers"),
    rt:wait_until_transfers_complete(Nodes);
maybe_wait_for_transfers(_Nodes, false) ->
    ok.

config() ->
    [{riak_core, [{handoff_concurrency, 11}]},
     {riak_search, [{enabled, true}]},
     {riak_pipe, [{worker_limit, 200}]}].

augment_config(Section, Property, Config) ->
    UpdSectionConfig = update_section(Section, Property, lists:keyfind(Section, 1, Config)),
    lists:keyreplace(Section, 1, Config, UpdSectionConfig).

update_section(Section, Property, false) ->
    {Section, [Property]};
update_section(Section, Property, {Section, SectionConfig}) ->
    {Section, [Property | SectionConfig]}.
