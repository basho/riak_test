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
-module(replication2_console_tests).
-include_lib("eunit/include/eunit.hrl").

%% This test checks to see if the riak repl *shell script*
%% communicates it's command line args to riak_repl_console
%% correctly. This test needs to be exercised on all supported
%% Riak platforms. This test helped fix a problem on Ubuntu
%% where "riak repl cascades" failed due to a shift error in
%% the script. Hopefully, this script will catch similar errors
%% with future changes to riak repl.
%% Note, this test is more about verifying parameter *arity* in
%% riak_repl_console than verifying all valid combinations
%% of arguments for each
%% command.
%%
%% test flow:
%% riak_test -> riak_repl (shell script) -> intercept
%%   a) if input received by riak repl is correct,
%%      display "pass" to the console. Test will
%%      pass via assert in check_cmd/2.
%%   b) if input received by riap-repl is unexpected
%%      display "fail" to the console, test will fail
%%      via assert in check_cmd/2
%%   c) if interrupt isn't called, "pass" won't be printed
%%      to stdout, test will fail via assert in check_cmd/2

-export([confirm/0]).

confirm() ->
    %% Deploy a node to test against
    lager:info("Deploy node to test riak repl command line"),
    [Node] = rt:deploy_nodes(1, [], [riak_kv, riak_repl]),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node])),
    rt_intercept:add(Node,
                     {riak_repl_console,
                      [
                        {{clustername,1}, verify_clustername},
                        {{modes,1}, verify_modes},
                        {{clusterstats,1}, verify_clusterstats},
                        {{realtime_cascades,1}, verify_realtime_cascades},
                        {{max_fssource_node,1}, verify_max_fssource_node},
                        {{max_fssource_cluster,1}, verify_max_fssource_cluster},
                        {{max_fssink_node,1}, verify_max_fssink_node},
                        {{fullsync,1}, verify_fullsync},
                        {{proxy_get,1}, verify_proxy_get},
                        {{add_nat_map,1}, verify_add_nat_map},
                        {{del_nat_map,1}, verify_del_nat_map},
                        {{show_nat_map,1}, verify_show_nat_map},
                        {{realtime,1}, verify_realtime},
                        {{add_block_provider_redirect,1}, verify_add_block_provider_redirect},
                        {{show_block_provider_redirect,1}, verify_show_block_provider_redirect},
                        {{delete_block_provider_redirect,1}, verify_delete_block_provider_redirect},
                        {{show_local_cluster_id,1}, verify_show_local_cluster_id}
                ]}),

    %% test different parameter arities
    check_cmd(Node, "clusterstats"),
    check_cmd(Node, "clusterstats cluster_mgr"),
    check_cmd(Node, "clusterstats 192.168.1.1:5555"),

    check_cmd(Node, "modes"),
    check_cmd(Node, "modes mode_repl12"),
    check_cmd(Node, "modes mode_repl12 mode_repl13"),

    check_cmd(Node, "clustername"),
    check_cmd(Node, "clustername foo"),

    check_cmd(Node, "realtime cascades"),
    check_cmd(Node, "realtime cascades always"),

    check_cmd(Node, "fullsync max_fssource_node"),
    check_cmd(Node, "fullsync max_fssource_node 99"),

    check_cmd(Node, "fullsync max_fssource_cluster"),
    check_cmd(Node, "fullsync max_fssource_cluster 99"),

    check_cmd(Node, "fullsync max_fssink_node"),
    check_cmd(Node, "fullsync max_fssink_node 99"),

    check_cmd(Node, "fullsync enable foo"),
    check_cmd(Node, "fullsync disable bar"),

    check_cmd(Node, "realtime enable foo"),
    check_cmd(Node, "realtime disable bar"),

    check_cmd(Node, "proxy_get enable foo"),
    check_cmd(Node, "proxy_get disable bar"),

    check_cmd(Node, "nat-map show"),
    check_cmd(Node, "nat-map add 1.2.3.4:4321 192.168.1.1"),
    check_cmd(Node, "nat-map del 1.2.3.4:4321 192.168.1.1"),

    check_cmd(Node, "add-block-provider-redirect a b"),
    check_cmd(Node, "show-block-provider-redirect a"),
    check_cmd(Node, "delete-block-provider-redirect a"),
    check_cmd(Node, "show-local-cluster-id"),

    pass.

check_cmd(Node, Cmd) ->
    lager:info("Testing riak repl ~s on ~s", [Cmd, Node]),
    {ok, Out} = rt:riak_repl(Node, [Cmd]),
    ?assertEqual("pass", Out).

