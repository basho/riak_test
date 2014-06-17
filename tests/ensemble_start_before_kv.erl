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

-module(ensemble_start_before_kv).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->

    NumNodes = 5,
    NVal = 5,

    lager:info("building cluster..."),
    ConfigEnsembleEnabled = ensemble_util:fast_config(NVal),
    Config = lists:map(fun
        ({riak_core, CoreConf}) ->
            {riak_core, lists:keystore(enable_consensus, 1, CoreConf, {enable_consensus, false})};
        (In) ->
            In
    end, ConfigEnsembleEnabled),

    Nodes = rt:build_cluster(NumNodes, Config),
    Node = hd(Nodes),

    lager:info("Stopping kv..."),
    _KvDown = [rpc:call(N, application, stop, [riak_kv]) || N <- Nodes],
    %EnsembleUp = rpc:call(Node, application, start, [riak_ensemble]),

    lager:info("Ensuring consesus is enabled and ensemble is running..."),
    _ = lists:foreach(fun(N) ->
        _ = rpc:call(N, application, set_env, [riak_core, enable_consensus, true]),
        {ok, Root} = rpc:call(N, application, get_env, [riak_core, platform_data_dir]),
        rpc:call(N, application, set_env, [riak_ensemble, data_root, Root]),
        KidConf = {riak_ensemble_sup, {riak_ensemble_sup, start_link, []},
        permanent, 30000, supervisor, [riak_ensemble_sup]},
        _KidStarted = supervisor:start_child({riak_core_sup, N}, KidConf)
    end, Nodes),
    _EnsembleUp = rpc:call(Node, riak_ensemble_manager, enable, []),
    _ = lists:foreach(fun(N) ->
        _Joinified = rpc:call(N, riak_ensemble_manager, join, [Node, N])
    end, Nodes),

    lager:info("ensuring there are no ensemble members..."),
    % ring size 16 cause I never changed it; should likely be a config opt.
    Falses = [false || _ <- lists:seq(1, 16)],
    ok = lists:foreach(fun(N) ->
        Memchecks = rpc:call(N, riak_kv_ensembles, check_membership, []),
        ?assertEqual(Falses, Memchecks)
    end, Nodes),

    pass.

    % disable ensembles initially

%    Config = lists:map(fun
%        ({riak_core, CoreConfig}) ->
%            {riak_core, lists:keystore(enable_consensus, 1, CoreConfig, {enable_consensus, false})};
%        (Else) ->
%            Else
%    end, ConfigWithEnsemble),
%
%    lager:info("Building cluster with consensus disabled"),
%
%    Nodes = rt:deploy_nodes(NumNodes, Config),
%    Node = hd(Nodes),
%    rt:join_cluster(Nodes),
%
%    lager:info("Stopping kv on node ~p", [Node]),
%    _ = rpc:call(Node, application, stop, [riak_kv]),
%
%    lager:info("Starting ensemble on node ~p", [Node]),
%    Got = rpc:call(Node, application, start, [riak_ensemble]),
%
%    lager:info("Got: ~p", [Got]),
%
%    pass.
