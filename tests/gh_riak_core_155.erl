%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(gh_riak_core_155).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Node] = rt:build_cluster(1),
  
    %% Generate a valid preflist for our get requests
    rpc:call(Node, riak_core, wait_for_service, [riak_kv]),
    BKey = {<<"bucket">>, <<"value">>},
    DocIdx = riak_core_util:chash_std_keyfun(BKey),
    PL = rpc:call(Node, riak_core_apl, get_apl, [DocIdx, 3, riak_kv]),

    lager:info("Adding delayed start to app.config"),
    NewConfig = [{riak_core, [{delayed_start, 1000}]}],
    rt:update_app_config(Node, NewConfig),

    %% Restart node, add mocks that delay proxy startup, and issue gets.
    %% Gets will come in before proxies started, and should trigger crash.
    rt:stop_and_wait(Node),
    rt:async_start(Node),
    rt:wait_until_pingable(Node),

    rt:load_modules_on_nodes([?MODULE, meck, meck_code, meck_proc,
                             meck_util, meck_expect, meck_code_gen], [Node]),
    ok = rpc:call(Node, ?MODULE, setup_mocks, []),

    lager:info("Installed mocks to delay riak_kv proxy startup"),
    lager:info("Issuing 10000 gets against ~p", [Node]),
    perform_gets(10000, Node, PL, BKey),

    lager:info("Verifying ~p has not crashed", [Node]),
    [begin
         ?assertEqual(pong, net_adm:ping(Node)),
         timer:sleep(1000)
     end || _ <- lists:seq(1,10)],

    lager:info("Test passed"),
    pass.

%% NOTE: Don't call lager in this function.  This function is compiled
%% using the lager version specified by Riak Test's rebar.config but
%% that may not match the version used by Riak where this function is
%% called.
setup_mocks() ->
    error_logger:info_msg("Beginning setup_mocks"),
    meck:new(riak_core_vnode_proxy_sup, [unstick, passthrough, no_link]),
    meck:expect(riak_core_vnode_proxy_sup, start_proxies,
                fun(Mod=riak_kv_vnode) ->
                        error_logger:info_msg("Delaying start of riak_kv_vnode proxies"),
                        timer:sleep(3000),
                        meck:passthrough([Mod]);
                   (Mod) ->
                        meck:passthrough([Mod])
                end),
    error_logger:info_msg("Installed mocks"),
    ok.

perform_gets(Count, Node, PL, BKey) ->
    rpc:call(Node, riak_kv_vnode, get, [PL, BKey, make_ref()]),
    perform_gets2(Count, Node, PL, BKey).

perform_gets2(0, _, _, _) ->
    ok;
perform_gets2(Count, Node, PL, BKey) ->
    rpc:call(Node, riak_kv_vnode, get, [PL, BKey, make_ref()], 1000),
    perform_gets(Count - 1, Node, PL, BKey).
