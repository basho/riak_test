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

-export([confirm/0]).

confirm() ->
    %% Deploy a node to test against
    lager:info("Deploy node to test riak-repl command line"),
    [NodeA, NodeB] = rt:deploy_nodes(2),
    ?assertEqual(ok, (rt:wait_until_nodes_ready([NodeA, NodeB]))),

    %% Check for usage printouts on incorrect command arities
    check_usage(NodeA, "modez"), %% modes takes any number of arguments so this checks the top level
    check_usage(NodeA, "status ERHT"),
    check_usage(NodeA, "add-listener Are you"),
    check_usage(NodeA, "add-nat-listener having a problem"),
    check_usage(NodeA, "del-listener buying a"),
    check_usage(NodeA, "add-site home or"),
    check_usage(NodeA, "del-site something Fret no more"),
    check_usage(NodeA, "start-fullsync Hi there"),
    check_usage(NodeA, "cancel-fullsync Belindas"),
    check_usage(NodeA, "pause-fullsync Im Senor Cardgage"),
    check_usage(NodeA, "resume-fullsync for Senor Cardgage"),
    check_usage(NodeA, "resume-fullsync Mortgage"),
    check_usage(NodeA, "clusterstats We can help you get a leg up"),
    check_usage(NodeA, "clustername on the pile"),
    check_usage(NodeA, "connections Low rates"),
    check_usage(NodeA, "clusters percent signs"),
    check_usage(NodeA, "connect I dunno"),
    check_usage(NodeA, "disconnect Bad credit"),
    check_usage(NodeA, "realtime enable No probalo"),
    check_usage(NodeA, "realtime disable Home Lawn"),
    check_usage(NodeA, "realtime start Escrow Re-Financin"),
    check_usage(NodeA, "realtime stop You name it we got it"),
    check_usage(NodeA, "realtime cascades Come along down for a free canceltation"),
    check_usage(NodeA, "fullsync enable with one of our handsome talking experts"),
    check_usage(NodeA, "fullsync disable One o them said"),
    check_usage(NodeA, "fullsync start theyd buy me lunch"),
    check_usage(NodeA, "fullsync stop But I dont see nobody"),
    check_usage(NodeA, "fullsync source taking me to"),
    check_usage(NodeA, "fullsync sink Chick-Fil-A"),
    check_usage(NodeA, "proxy-get enable Senor Cardgage Mortgage"),
    check_usage(NodeA, "proxy-get disable helped consolidate my whole life"),
    check_usage(NodeA, "proxy-get redirect show into this tiny box"),
    check_usage(NodeA, "proxy-get redirect add Thanks Senor Mortgage"),
    check_usage(NodeA, "proxy-get redirect delete Youre every welcome Valerie"),
    check_usage(NodeA, "proxy-get redirect cluster-id Act now and see"),
    check_usage(NodeA, "nat-map show if you can stand"),
    check_usage(NodeA, "nat-map add to talk to me for"),
    check_usage(NodeA, "nat-map del more than four seconds"),
    check_usage(NodeA, "Get a leg up on the pile and refinance your dreams"),

    %% When repl12_mode is active, Version 2 commands are shown, when
    %% repl13_mode is active Version 3 commands are shown.
    lager:info("Checking V2 and V3 usage output"),
    %% rpc:call(Node, riak_repl_console, set_modes, [repl12_mode, repl13_mode]),
    rt:riak_repl(NodeA, ["modes set v2=on v3=on"]),
    OutBoth = check_usage(NodeA, ""),
    assertContains(OutBoth,"Version 2 Commands:"),
    assertContains(OutBoth,"Version 3 Commands:"),

    lager:info("Checking V2-only usage output"),
    rt:riak_repl(NodeA, ["modes set v2=on"]),
    Out2 = check_usage(NodeA, ""),
    assertContains(Out2, "Version 2 Commands:"),
    assertNotContains(Out2, "Version 3 Commands:"),

    lager:info("Checking V3-only usage output"),
    rt:riak_repl(NodeA, ["modes set v3=on"]),
    Out3 = check_usage(NodeA, ""),
    assertContains(Out3, "Version 3 Commands:"),
    assertNotContains(Out3, "Version 2 Commands:"),

    %% test different parameter arities
    check_cmd(NodeA, "clusterstats"),
    check_upgrade(NodeA, "clusterstats cluster_mgr"),
    check_upgrade(NodeA, "clusterstats 192.168.1.1:5555"),
    check_cmd(NodeA, "clusterstats --name cluster_mgr"),
    check_cmd(NodeA, "clusterstats --host 192.168.1.1:5555"),

    check_cmd(NodeA, "modes show"),
    check_usage(NodeA, "modes"),
    check_upgrade(NodeA, "modes mode_repl12"),
    check_upgrade(NodeA, "modes mode_repl12 mode_repl13"),

    check_usage(NodeA, "clustername"),
    check_cmd(NodeA, "clustername show"),
    check_upgrade(NodeA, "clustername foo"),
    check_cmd(NodeB, "clustername set name=bar"),

    check_cmd_matches(NodeA, "clusters", "There are no known remote clusters."),
    check_cmd_matches(NodeB, "clusters", "There are no known remote clusters."),

    %% Now connect the clusters-of-one so we can test some commands
    %% about connected clusters.
    {ok, {IPB, PortB}} = rt:rpc_get_env(NodeB, [{riak_core, cluster_mgr}]),
    check_cmd(NodeA, lists:flatten(io_lib:format("connect address=~s:~p", [IPB, PortB]))),

    check_cmd_matches(NodeA, "clusters", "bar"),
    check_cmd_matches(NodeB, "clusters", "There are no known remote clusters."), % NB: Is this ok?

    check_cmd_matches(NodeA, "connections", "bar"),
    check_cmd_matches(NodeB, "connections", "There are no connected sink clusters."),

    check_cmd_matches(NodeA, "", ""),
    %% check_cmd(NodeA, "realtime cascades show"),
    %% check_cmd(NodeA, "realtime cascades enable"),
    %% check_cmd(NodeA, "realtime cascades disable"),

    %% check_cmd(NodeA, "fullsync source max_workers_per_node show"),
    %% check_cmd(NodeA, "fullsync source max_workers_per_node set 99"),

    %% check_cmd(NodeA, "fullsync source max_workers_per_cluster show"),
    %% check_cmd(NodeA, "fullsync source max_workers_per_cluster set 99"),

    %% check_cmd(NodeA, "fullsync sink max_workers_per_node show"),
    %% check_cmd(NodeA, "fullsync sink max_workers_per_node set 99"),

    %% check_cmd(NodeA, "fullsync enable foo"),
    %% check_cmd(NodeA, "fullsync disable bar"),

    %% check_cmd(NodeA, "realtime enable foo"),
    %% check_cmd(NodeA, "realtime disable bar"),

    %% check_cmd(NodeA, "proxy-get enable foo"),
    %% check_cmd(NodeA, "proxy-get disable bar"),

    %% check_cmd(NodeA, "nat-map show"),
    %% check_cmd(NodeA, "nat-map add 1.2.3.4:4321 192.168.1.1"),
    %% check_cmd(NodeA, "nat-map delete 1.2.3.4:4321 192.168.1.1"),

    %% check_cmd(NodeA, "proxy-get redirect add a b"),
    %% check_cmd(NodeA, "proxy-get redirect show a"),
    %% check_cmd(NodeA, "proxy-get redirect delete a"),
    %% check_cmd(NodeA, "proxy-get redirect cluster-id"),

    pass.

check_cmd_matches(Node, Cmd, Matcher) ->
    lager:info("Testing `riak-repl ~s` on ~s", [Cmd, Node]),
    {ok, Out} = rt:riak_repl(Node, [Cmd]),
    assertContains(Out, Matcher).

check_cmd(Node, Cmd) ->
    lager:info("Testing `riak-repl ~s` on ~s", [Cmd, Node]),
    {ok, Out} = rt:riak_repl(Node, [Cmd]),
    assertNotContains(Out, "(^|\n)Usage").

check_upgrade(Node, Cmd) ->
    lager:info("Checking upgrade output for `riak-repl ~s` on ~s", [Cmd, Node]),
    {ok, Out} = rt:riak_repl(Node, [Cmd]),
    assertContains(Out, "The command form `.*` is deprecated.").

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
