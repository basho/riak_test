%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(verify_job_switch_defaults).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include("job_enable_common.hrl").

%% ===================================================================
%% Test API
%% ===================================================================

confirm() ->
    Configs = [{current, {cuttlefish, ?COMMON_CONFIG}}],
    lager:info("Deploying ~b nodes ...", [erlang:length(Configs)]),
    [Node | _] = Nodes = rt:deploy_nodes(Configs),

    job_enable_common:setup_cluster(Nodes),
    job_enable_common:setup_yokozuna(Nodes),

    [test_job_switch(Node, Class, Enabled)
        || {Class, Enabled} <- ?JOB_CLASS_DEFAULTS],

    pass.

%% ===================================================================
%% Internal
%% ===================================================================

test_job_switch(Node, Class, Enabled) ->
    lager:info("verifying ~p default ~s",
        [Class, job_enable_common:enabled_string(Enabled)]),
    ?assertEqual(Enabled, job_enable_common:get_enabled(Node, Class)),
    ?assertEqual(ok, job_enable_common:set_enabled(Node, Class, not Enabled)),
    ?assertEqual(not Enabled, job_enable_common:get_enabled(Node, Class)).
