%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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

-module(ts_aggregation_cluster).

-export([confirm/0]).

% Test basic aggregation functionality on larger and broken clusters

confirm() ->
    Cluster = ts_aggregation_simple:verify_aggregation(multiple),
    rt:clean_cluster(Cluster),
    %_NewCluster = ts_aggregation_simple:verify_aggregation(one_down),
    pass.