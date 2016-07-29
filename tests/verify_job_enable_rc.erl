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
    ?TOKEN_LIST_KEYS,
    ?TOKEN_MAP_REDUCE,
    ?TOKEN_SEC_INDEX
%   ?TOKEN_YZ_SEARCH
]).

%% ===================================================================
%% Test API
%% ===================================================================

confirm() ->
    Configs = [
        {current, {cuttlefish,
            [{"storage_backend", "leveldb"}] ++ config(?TEST_OPS, Bool, [])}}
        % make it a 4 node cluster so things get scattered around
        % everything's enabled on the trailing nodes
        || Bool <- ?TEST_ORDER ++ [true, true]],

    lager:info("Deploying ~b nodes ...", [erlang:length(Configs)]),
    Nodes = rt:deploy_nodes(Configs),

    job_enable_common:setup_cluster(Nodes),

    [run_test(Operation, ?TEST_ORDER, Nodes) || Operation <- ?TEST_OPS],

    pass.

%% ===================================================================
%% Internal
%% ===================================================================

config([Operation | Operations], Bool, Result) ->
    Key = lists:flatten(io_lib:format(?CUTTLEFISH_PREFIX ".~s", [Operation])),
    Val = case Bool of
        true ->
            "on";
        false ->
            "off"
    end,
    config(Operations, Bool, [{Key, Val} | Result]);
config([], _, Result) ->
    Result.

run_test(Operation, [Switch | Switches], [Node | Nodes]) ->
    Enabled = job_enable_common:enabled_string(Switch),
    [begin
        lager:info("Tesing ~s ~s ~s on ~p", [Operation, Type, Enabled, Node]),
        job_enable_common:test_operation(Node, Operation, Switch, Type)
    end || Type <- [pbc, http] ],
    run_test(Operation, Switches, Nodes);
run_test(_, [], _) ->
    ok.


