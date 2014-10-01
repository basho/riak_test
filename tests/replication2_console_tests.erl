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

%% This test checks to see if the riak-repl *shell script*
%% communicates it's command line args to riak_repl_console
%% correctly. This test needs to be exercised on all supported
%% Riak platforms. This test helped fix a problem on Ubuntu
%% where "riak-repl cascades" failed due to a shift error in
%% the script. Hopefully, this script will catch similar errors
%% with future changes to riak-repl.
%% Note, this test is more about verifying parameter *arity* in
%% riak_repl_console than verifying all valid combinations
%% of arguments for each
%% command.
%%
%% test flow:
%% riak_test -> riak_repl (shell script) -> intercept
%%   a) if input received by riak-repl is correct,
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
    lager:info("Deploy node to test riak-repl command line"),
    [Node] = rt:deploy_nodes(1),
    ?assertEqual(ok, (rt:wait_until_nodes_ready([Node]))),

    %% Check for usage printouts on incorrect command arities
    check_usage(Node, "modez"), %% modes takes any number of arguments so this checks the top level
    check_usage(Node, "status ERHT"),
    check_usage(Node, "add-listener Are you"),
    check_usage(Node, "add-nat-listener having a problem"),
    check_usage(Node, "del-listener buying a"),
    check_usage(Node, "add-site home or"),
    check_usage(Node, "del-site something Fret no more"),
    check_usage(Node, "start-fullsync Hi there"),
    check_usage(Node, "cancel-fullsync Belindas"),
    check_usage(Node, "pause-fullsync Im Senor Cardgage"),
    check_usage(Node, "resume-fullsync for Senor Cardgage"),
    check_usage(Node, "resume-fullsync Mortgage"),
    check_usage(Node, "clusterstats We can help you get a leg up"),
    check_usage(Node, "clustername on the pile"),
    check_usage(Node, "connections Low rates"),
    check_usage(Node, "clusters percent signs"),
    check_usage(Node, "connect I dunno"),
    check_usage(Node, "disconnect Bad credit"),
    check_usage(Node, "realtime enable No probalo"),
    check_usage(Node, "realtime disable Home Lawn"),
    check_usage(Node, "realtime start Escrow Re-Financin"),
    check_usage(Node, "realtime stop You name it we got it"),
    check_usage(Node, "realtime cascades Come along down for a free canceltation"),
    check_usage(Node, "fullsync enable with one of our handsome talking experts"),
    check_usage(Node, "fullsync disable One o them said"),
    check_usage(Node, "fullsync start theyd buy me lunch"),
    check_usage(Node, "fullsync stop But I dont see nobody"),
    check_usage(Node, "fullsync source taking me to"),
    check_usage(Node, "fullsync sink Chick-Fil-A"),
    check_usage(Node, "proxy-get enable Senor Cardgage Mortgage"),
    check_usage(Node, "proxy-get disable helped consolidate my whole life"),
    check_usage(Node, "proxy-get redirect show into this tiny box"),
    check_usage(Node, "proxy-get redirect add Thanks Senor Mortgage"),
    check_usage(Node, "proxy-get redirect delete Youre every welcome Valerie"),
    check_usage(Node, "proxy-get redirect cluster-id Act now and see"),
    check_usage(Node, "nat-map show if you can stand"),
    check_usage(Node, "nat-map add to talk to me for"),
    check_usage(Node, "nat-map del more than four seconds"),
    check_usage(Node, "Get a leg up on the pile and refinance your dreams"),

    %% When repl12_mode is active, Version 2 commands are shown, when
    %% repl13_mode is active Version 3 commands are shown.
    lager:info("Checking V2 and V3 usage output"),
    %% rpc:call(Node, riak_repl_console, set_modes, [repl12_mode, repl13_mode]),
    rt:riak_repl(Node, ["modes mode_repl12 mode_repl13"]),
    OutBoth = check_usage(Node, ""),
    assertContains(OutBoth,"Version 2 Commands:"),
    assertContains(OutBoth,"Version 3 Commands:"),

    lager:info("Checking V2-only usage output"),
    rt:riak_repl(Node, ["modes mode_repl12"]),
    Out2 = check_usage(Node, ""),
    assertContains(Out2, "Version 2 Commands:"),
    assertNotContains(Out2, "Version 3 Commands:"),

    lager:info("Checking V3-only usage output"),
    rt:riak_repl(Node, ["modes mode_repl13"]),
    Out3 = check_usage(Node, ""),
    assertContains(Out3, "Version 3 Commands:"),
    assertNotContains(Out3, "Version 2 Commands:"),

    %% Install intercepts so the real commands aren't run
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

    check_cmd(Node, "fullsync source max_workers_per_node"),
    check_cmd(Node, "fullsync source max_workers_per_node 99"),

    check_cmd(Node, "fullsync source max_workers_per_cluster"),
    check_cmd(Node, "fullsync source max_workers_per_cluster 99"),

    check_cmd(Node, "fullsync sink max_workers_per_node"),
    check_cmd(Node, "fullsync sink max_workers_per_node 99"),

    check_cmd(Node, "fullsync enable foo"),
    check_cmd(Node, "fullsync disable bar"),

    check_cmd(Node, "realtime enable foo"),
    check_cmd(Node, "realtime disable bar"),

    check_cmd(Node, "proxy-get enable foo"),
    check_cmd(Node, "proxy-get disable bar"),

    check_cmd(Node, "nat-map show"),
    check_cmd(Node, "nat-map add 1.2.3.4:4321 192.168.1.1"),
    check_cmd(Node, "nat-map del 1.2.3.4:4321 192.168.1.1"),

    check_cmd(Node, "proxy-get redirect add a b"),
    check_cmd(Node, "proxy-get redirect show a"),
    check_cmd(Node, "proxy-get redirect del a"),
    check_cmd(Node, "proxy-get redirect cluster-id"),

    pass.

check_cmd(Node, Cmd) ->
    lager:info("Testing `riak-repl ~s` on ~s", [Cmd, Node]),
    {ok, Out} = rt:riak_repl(Node, [Cmd]),
    ?assertEqual("pass", Out).

check_usage(Node, Cmd) ->
    lager:info("Checking usage output for `riak-repl ~s` on ~s", [Cmd, Node]), 
    {ok, Out} = rt:riak_repl(Node, [Cmd]),
    assertContains(Out, "(^|\n)Usage: "),
    Out.

assertContains(Output, Str) ->
     ?assertEqual(match, (contains(Output, Str))).

assertNotContains(Output, Str) ->
     ?assertEqual(nomatch, (contains(Output, Str))).


contains(Output, Str) ->
    re:run(Output, Str, [{capture, none}, global]).
