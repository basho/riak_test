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
-module(verify_2i).
-behavior(riak_test).

-include("include/rt.hrl").

-export([confirm/0]).

-define(CONFIG,
        [{riak_core,
          [{handoff_concurrency, 100},
           {ring_creation_size, 16},
           {vnode_management_timer, 1000}
          ]},
         {riak_kv,
          [{anti_entropy, {off, []}},
           {anti_entropy_build_limit, {100, 500}},
           {anti_entropy_concurrency, 100},
           {anti_entropy_tick, 200}]}]).

confirm() ->
    inets:start(),
    Nodes = rt:build_cluster(3, ?CONFIG),
    run(secondary_index_tests, <<"2i_basic">>, Nodes),
    run(verify_2i_returnterms, <<"2i_returnterms">>, Nodes),
    run(verify_2i_timeout, <<"2i_timeout">>, Nodes),
    run(verify_2i_stream, <<"2i_stream">>, Nodes),
    run(verify_2i_limit, <<"2i_limit">>, Nodes),
    run(verify_2i_aae, <<"2i_aae">>, Nodes).

run(Mod, BucketOrBuckets, Nodes) ->
    Buckets = to_list(BucketOrBuckets),
    lager:info("Running test in ~s", [Mod]),
    Exports = Mod:module_info(exports),
    HasSetup = lists:member({setup, 1}, Exports),
    HasCleanup = lists:member({cleanup, 1}, Exports),
    Ctx = #rt_test_context{buckets=Buckets, nodes=Nodes},
    RollbackInfo = HasSetup andalso Mod:setup(Ctx),
    Mod:confirm(Ctx),
    HasCleanup andalso Mod:cleanup(RollbackInfo).

to_list(L) when is_list(L) ->
    L;
to_list(L) ->
    [L].
