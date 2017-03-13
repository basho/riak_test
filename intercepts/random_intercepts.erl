%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.
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

-module(random_intercepts).
-compile(export_all).
-include("intercept.hrl").
-define(M, random_orig).

last_for_uniform_s(3, Seed) ->
    ?I_INFO("Returning 3 for uniform_s", []),
    {3, Seed};
last_for_uniform_s(Length, Seed) ->
    ?M:uniform_s_orig(Length, Seed).

unstick_random() ->
    code:unstick_dir(code:lib_dir(stdlib) ++ "/ebin").


