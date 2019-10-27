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

-module(ensemble_start_without_aae).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->

    NumNodes = 6,
    NVal = 5,

    Config = ensemble_util:fast_config(NVal, false),
    lager:info("Building cluster with consensus enabled and AAE disabled. Waiting for ensemble to stablize ..."),

    _ = ensemble_util:build_cluster_without_quorum(NumNodes, Config),
    pass.
