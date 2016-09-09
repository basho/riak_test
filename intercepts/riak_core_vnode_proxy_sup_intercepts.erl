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

-module(riak_core_vnode_proxy_sup_intercepts).
-compile(export_all).
-include("intercept.hrl").
-define(M, riak_core_vnode_proxy_sup_orig).

sleep_start_proxies(Mod=riak_kv_vnode) ->
    ?I_INFO("Delaying start of riak_kv_vnode proxies for 3s\n"),
    timer:sleep(3000),
    ?M:start_proxies_orig(Mod);
sleep_start_proxies(Mod) ->
    ?M:start_proxies_orig(Mod).

