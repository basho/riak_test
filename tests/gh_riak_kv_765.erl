%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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

%% This module tests the various AAE additions made in
%% https://github.com/basho/riak_kv/pull/765

%% !!! DO NOT ADD TO GIDDYUP
%%
%% This module is not meant to be used as an automated CI test. It
%% exists for development/code review purposes to ensure the changes
%% made in basho/riak_kv#765 work as the pull-request claims.
%%
%% !!! DO NOT ADD TO GIDDYUP

-module(gh_riak_kv_765).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    pass = check_empty_build(),
    pass = check_throttle_and_expiration(),
    pass.

check_empty_build() ->
    Config = [{riak_core, [{vnode_management_timer, 1000},
                           {ring_creation_size, 4}]}],
    Nodes = rt_cluster:build_cluster(1, Config),
    Node = hd(Nodes),
    timer:sleep(2000),
    Self = self(),
    spawn(fun() ->
                  time_build(Node),
                  Self ! done
          end),
    Result = receive
                 done -> pass
             after
                 10000 ->
                     lager:info("Failed. Empty AAE trees were not built instantly"),
                     fail
             end,
    rt_cluster:clean_cluster(Nodes),
    Result.

check_throttle_and_expiration() ->
    Config = [{riak_kv, [{anti_entropy_build_limit, {100, 1000}},
                         {anti_entropy_concurrency, 100},
                         {anti_entropy_tick, 1000},
                         {anti_entropy, {off, []}}]},
              {riak_core, [{vnode_management_timer, 1000},
                           {ring_creation_size, 4}]}],
    Nodes = rt_cluster:build_cluster(1, Config),
    Node = hd(Nodes),
    timer:sleep(2000),

    lager:info("Write 1000 keys"),
    rt:systest_write(Node, 1000),
    enable_aae(Node),
    time_build(Node),
    Duration1 = rebuild(Node, 30000, 1000),
    Duration2 = rebuild(Node, 30000, 5500),
    ?assert(Duration2 > (2 * Duration1)),

    %% Test manual expiration
    lager:info("Disabling automatic expiration"),
    rpc:call(Node, application, set_env,
             [riak_kv, anti_entropy_expire, never]),
    lager:info("Manually expiring hashtree for partition 0"),
    expire_tree(Node, 0),
    pass.

time_build(Node) ->
    T0 = erlang:now(),
    rt:wait_until_aae_trees_built([Node]),
    Duration = timer:now_diff(erlang:now(), T0),
    lager:info("Build took ~b us", [Duration]),
    Duration.

rebuild(Node, Limit, Wait) ->
    rpc:call(Node, application, set_env,
             [riak_kv, anti_entropy_build_throttle, {Limit, Wait}]),
    rpc:call(Node, application, set_env,
             [riak_kv, anti_entropy_expire, 0]),
    timer:sleep(1500),
    disable_aae(Node),
    rpc:call(Node, ets, delete_all_objects, [ets_riak_kv_entropy]),
    enable_aae(Node),
    time_build(Node).

enable_aae(Node) ->
    rpc:call(Node, riak_kv_entropy_manager, enable, []).

disable_aae(Node) ->
    rpc:call(Node, riak_kv_entropy_manager, disable, []).

expire_tree(Node, Partition) ->
    Now = erlang:now(),
    {ok, Tree} = rpc:call(Node, riak_kv_vnode, hashtree_pid, [Partition]),
    rpc:call(Node, riak_kv_index_hashtree, expire, [Tree]),
    rt:wait_until(Node,
                  fun(_) ->
                          Info = rpc:call(Node, riak_kv_entropy_info, compute_tree_info, []),
                          {0, Built} = lists:keyfind(0, 1, Info),
                          Built > Now
                  end),
    ok.
