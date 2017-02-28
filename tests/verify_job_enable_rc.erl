%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016-2017 Basho Technologies, Inc.
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

%% Verify functionality of async job enable/disable flags in riak.conf.
%% Toggling flags at runtime is tested in verify_job_enable_ac.
-module(verify_job_enable_rc).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include("job_enable_common.hrl").

-define(TEST_ORDER, [true, false]).
-define(TEST_OPS,   [
    ?TOKEN_LIST_BUCKETS,
    ?TOKEN_LIST_BUCKETS_S,
    ?TOKEN_LIST_KEYS,
    ?TOKEN_LIST_KEYS_S,
    ?TOKEN_MAP_REDUCE,
    ?TOKEN_MAP_REDUCE_JS,
    ?TOKEN_SEC_INDEX,
    ?TOKEN_SEC_INDEX_S,
    ?TOKEN_OLD_SEARCH,
    ?TOKEN_YZ_SEARCH
]).

%% ===================================================================
%% Test API
%% ===================================================================

confirm() ->
    %% Allow listing of buckets and keys for testing
    application:set_env(riakc, allow_listing, true),

    Configs = [
        {current, {cuttlefish,
            ?COMMON_CONFIG ++ config(?TEST_OPS, Bool, [])}}
        % make it a 4 node cluster so things get scattered around
        % everything's enabled on the trailing nodes
        || Bool <- ?TEST_ORDER ++ [true, true]],

    lager:info("Deploying ~b nodes ...", [erlang:length(Configs)]),
    Nodes = rt:deploy_nodes(Configs),

    job_enable_common:setup_cluster(Nodes),

    job_enable_common:setup_yokozuna(Nodes),

    [run_test(Operation, ?TEST_ORDER, Nodes) || Operation <- ?TEST_OPS],

    pass.

%% ===================================================================
%% Internal
%% ===================================================================

config([{App, Op} | Operations], Enabled, Result) ->
    Key = lists:flatten(?CUTTLEFISH_KEY(App, Op)),
    Val = job_enable_common:enabled_string(Enabled),
    config(Operations, Enabled, [{Key, Val} | Result]);
config([], _, Result) ->
    Result.

run_test(Operation, [Enabled | Switches], [Node | Nodes]) ->
    [job_enable_common:test_operation(Node, Operation, Enabled, ClientType)
        || ClientType <- [pbc, http] ],
    run_test(Operation, Switches, Nodes);
run_test(_, [], _) ->
    ok.


