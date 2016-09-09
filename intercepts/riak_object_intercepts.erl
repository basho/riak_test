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
%%-------------------------------------------------------------------

-module(riak_object_intercepts).
-export([skippable_index_specs/1,
         skippable_diff_index_specs/2]).
-include("intercept.hrl").

skippable_index_specs(Obj) ->
    case app_helper:get_env(riak_kv, skip_index_specs, false) of
        false ->
            riak_object_orig:index_specs_orig(Obj);
        true ->
            []
    end.

skippable_diff_index_specs(Obj, OldObj) ->
    case app_helper:get_env(riak_kv, skip_index_specs, false) of
        false ->
            riak_object_orig:diff_index_specs_orig(Obj, OldObj);
        true ->
            []
    end.
