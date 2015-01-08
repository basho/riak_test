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
-module(riak_admin_console_tests).
-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

%% This test passes params to the riak-admin shell script on to intercepts
%% that either return ?PASS or ?FAIL (which print out "pass" or "fail" to
%% the console). If an unexpected input is received in Erlang, ?FAIL is
%% returned. This test should (will?) make sure we don't implement
%% any unportable shell code. For example, `riak-repl cascades foo`
%% didn't work on Ubuntu due to an invalid call to shift. Since this test
%% will be run on giddyup and hence many platforms, we should be able
%% to catch these types of bugs earlier.
%% See also: replication2_console_tests.erl for a more detailed
%% description.

%% UNTESTED, as they don't use rpc, or have a non-trivial impl
%%   test
%%   diag
%%   top
%%   wait-for-services
%%   js-reload
%%   reip

%% riak-admin cluster
cluster_tests(Node) ->
    check_admin_cmd(Node, "cluster join dev99@127.0.0.1"),
    check_admin_cmd(Node, "cluster leave"),
    check_admin_cmd(Node, "cluster leave dev99@127.0.0.1"),
    check_admin_cmd(Node, "cluster force-remove dev99@127.0.0.1"),
    check_admin_cmd(Node, "cluster replace dev98@127.0.0.1 dev99@127.0.0.1"),
    check_admin_cmd(Node, "cluster force-replace dev98@127.0.0.1 dev99@127.0.0.1"),
    check_admin_cmd(Node, "cluster resize-ring 42"),
    check_admin_cmd(Node, "cluster resize-ring abort"),
    check_admin_cmd(Node, "cluster plan"),
    check_admin_cmd(Node, "cluster commit"),
    check_admin_cmd(Node, "cluster clear"),
    check_admin_output(Node, "cluster status", "Cluster Status"),
    check_admin_output(Node, "cluster partition-count", "partition-count"),
    check_admin_output(Node, "cluster partitions", "dev1@127\\.0\\.0\\.1"),
    check_admin_output(Node, "cluster partition id=0", "^\\+"),
    check_admin_output(Node, "cluster partition index=0", "\\+").

%% riak-admin bucket_type
bucket_tests(Node) ->
    check_admin_cmd(Node, "bucket-type status foo"),
    check_admin_cmd(Node, "bucket-type activate foo"),
    check_admin_cmd(Node, "bucket-type create foo {\"props\":{[]}}"),
    check_admin_cmd(Node, "bucket-type update foo {\"props\":{[]}}"),
    check_admin_cmd(Node, "bucket-type list").


%% riak-admin security
security_tests(Node) ->
    check_admin_cmd_2x(Node, "security add-user foo"),
    check_admin_cmd_2x(Node, "security add-user foo x1=y1 x2=y2"),
    check_admin_cmd_2x(Node, "security add-group group"),
    check_admin_cmd_2x(Node, "security add-group group x1=y1 x2=y2"),
    check_admin_cmd_2x(Node, "security alter-user foo x1=y1"),
    check_admin_cmd_2x(Node, "security alter-user foo x1=y1 x2=y2"),
    check_admin_cmd_2x(Node, "security alter-group group x1=y1 x2=y2"),
    check_admin_cmd(Node, "security del-user foo"),
    check_admin_cmd(Node, "security del-group group"),
    check_admin_cmd(Node, "security add-source all 192.168.100.0/22 y"),
    check_admin_cmd(Node, "security add-source all 192.168.100.0/22 x x1=y1"),
    check_admin_cmd(Node, "security add-source foo,bar 192.168.100.0/22 x x1=y1"),
    check_admin_cmd(Node, "security add-source foo,bar,baz 192.168.100.0/22 x x1=y1 x2=y2"),
    check_admin_cmd(Node, "security del-source all 192.168.100.0/22"),
    check_admin_cmd(Node, "security del-source x 192.168.100.0/22"),
    check_admin_cmd(Node, "security del-source x,y,z 192.168.100.0/22"),
    check_admin_cmd(Node, "security grant foo on any my_bucket to x"),
    check_admin_cmd(Node, "security grant foo,bar on any my_bucket to x"),
    check_admin_cmd(Node, "security grant foo on any my_bucket to x,y,z"),
    check_admin_cmd(Node, "security grant foo,bar,baz on any my_bucket to y"),
    check_admin_cmd(Node, "security grant foo,bar,baz on foo my_bucket to y"),
    check_admin_cmd(Node, "security revoke foo on any my_bucket from x"),
    check_admin_cmd(Node, "security revoke foo,bar on any my_bucket from x"),
    check_admin_cmd(Node, "security revoke foo on any my_bucket from x,y,z"),
    check_admin_cmd(Node, "security revoke foo,bar,baz on any my_bucket from y"),
    check_admin_cmd(Node, "security revoke foo,bar,baz on foo my_bucket from y"),
    check_admin_cmd(Node, "security print-users"),
    check_admin_cmd(Node, "security print-sources"),
    check_admin_cmd_2x(Node, "security enable"),
    check_admin_cmd_2x(Node, "security disable"),
    check_admin_cmd(Node, "security status"),
    check_admin_cmd(Node, "security print-user foo"),
    check_admin_cmd(Node, "security print-group group"),
    check_admin_cmd(Node, "security print-grants foo"),
    check_admin_cmd(Node, "security ciphers foo").

%% handoff commands
riak_admin_handoff_tests(Node) ->
    check_admin_output(Node, "handoff summary", "Each cell"),
    check_admin_output(Node, "handoff details", "No ongoing").

%% "top level" riak-admin COMMANDS
riak_admin_tests(Node) ->
    check_admin_cmd(Node, "join -f dev99@127.0.0.1"),
    check_admin_cmd(Node, "leave -f"),
    check_admin_cmd(Node, "force-remove -f dev99@127.0.0.1"),
    check_admin_cmd(Node, "force_remove -f dev99@127.0.0.1"),
    check_admin_cmd(Node, "down dev98@127.0.0.1"),
    check_admin_cmd(Node, "status"),
    check_admin_cmd(Node, "vnode-status"),
    check_admin_cmd(Node, "vnode_status"),
    check_admin_cmd(Node, "ringready"),
    check_admin_cmd(Node, "transfers"),
    check_admin_cmd(Node, "member-status"),
    check_admin_cmd(Node, "member_status"),
    check_admin_cmd(Node, "ring-status"),
    check_admin_cmd(Node, "ring_status"),
    check_admin_cmd(Node, "aae-status"),
    check_admin_cmd(Node, "aae_status"),
    check_admin_cmd(Node, "repair_2i status"),
    check_admin_cmd(Node, "repair_2i kill"),
    check_admin_cmd(Node, "repair_2i --speed 5 foo bar baz"),
    check_admin_cmd(Node, "repair-2i status"),
    check_admin_cmd(Node, "repair-2i kill"),
    check_admin_cmd(Node, "repair-2i --speed 5 foo bar baz"),
    check_admin_cmd(Node, "cluster_info foo local"),
    check_admin_cmd(Node, "cluster_info foo local dev99@127.0.0.1"),
    check_admin_cmd(Node, "erl-reload"),
    check_admin_cmd(Node, "erl_reload"),
    check_admin_cmd(Node, "transfer-limit 1"),
    check_admin_cmd(Node, "transfer-limit dev55@127.0.0.1 1"),
    check_admin_cmd(Node, "transfer_limit 1"),
    check_admin_cmd(Node, "transfer_limit dev55@127.0.0.1 1"),
    check_admin_cmd(Node, "reformat-indexes --downgrade"),
    check_admin_cmd(Node, "reformat-indexes 5"),
    check_admin_cmd(Node, "reformat-indexes 6 7"),
    check_admin_cmd(Node, "reformat-indexes 5 --downgrade"),
    check_admin_cmd(Node, "reformat-indexes 6 7 --downgrade"),
    check_admin_cmd(Node, "reformat_indexes --downgrade"),
    check_admin_cmd(Node, "reformat_indexes 5"),
    check_admin_cmd(Node, "reformat_indexes 6 7"),
    check_admin_cmd(Node, "reformat_indexes 5 --downgrade"),
    check_admin_cmd(Node, "reformat_indexes 6 7 --downgrade"),
    check_admin_cmd(Node, "downgrade_objects true"),
    check_admin_cmd(Node, "downgrade_objects true 1"),
    check_admin_cmd(Node, "downgrade_objects true"),
    check_admin_cmd(Node, "downgrade_objects true 1"),
    check_admin_cmd(Node, "js-reload foo bar baz"),
    ok.

confirm() ->
    %% Deploy a node to test against
    lager:info("Deploy node to test riak command line"),
    [Node] = rt:deploy_nodes(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node])),
    rt_intercept:add(Node,
                     {riak_core_console,
                      [
                        {{transfers,1}, verify_console_transfers},
                        {{member_status,1}, verify_console_member_status},
                        {{ring_status,1}, verify_console_ring_status},
                        {{stage_remove,1}, verify_console_stage_remove},
                        {{stage_leave,1}, verify_console_stage_leave},
                        {{stage_replace, 1}, verify_console_stage_replace},
                        {{stage_force_replace, 1}, verify_console_stage_force_replace},
                        {{stage_resize_ring, 1}, verify_console_stage_resize_ring},
                        {{print_staged, 1}, verify_console_print_staged},
                        {{commit_staged, 1}, verify_console_commit_staged},
                        {{clear_staged, 1}, verify_console_clear_staged},
                        {{transfer_limit, 1}, verify_console_transfer_limit},
                        {{add_user, 1}, verify_console_add_user},
                        {{alter_user, 1}, verify_console_alter_user},
                        {{del_user, 1}, verify_console_del_user},
                        {{add_group, 1}, verify_console_add_group},
                        {{alter_group, 1}, verify_console_alter_group},
                        {{del_group, 1}, verify_console_del_group},
                        {{add_source, 1}, verify_console_add_source},
                        {{del_source, 1}, verify_console_del_source},
                        {{grant, 1}, verify_console_grant},
                        {{revoke, 1}, verify_console_revoke},
                        {{print_user,1}, verify_console_print_user},
                        {{print_users,1}, verify_console_print_users},
                        {{print_group,1}, verify_console_print_group},
                        {{print_groups,1}, verify_console_print_groups},
                        {{print_grants,1}, verify_console_print_grants},
                        {{print_sources, 1}, verify_console_print_sources},
                        {{security_enable,1}, verify_console_security_enable},
                        {{security_disable,1}, verify_console_security_disable},
                        {{security_status,1}, verify_console_security_stats},
                        {{ciphers,1}, verify_console_ciphers} ]}),

    rt_intercept:add(Node,
                     {riak_kv_console,
                      [
                        {{join,1}, verify_console_join},
                        {{leave,1}, verify_console_leave},
                        {{remove,1}, verify_console_remove},
                        {{staged_join,1}, verify_console_staged_join},
                        {{down,1}, verify_console_down},
                        {{status,1}, verify_console_status},
                        {{vnode_status,1}, verify_console_vnode_status},
                        {{ringready,1}, verify_console_ringready},
                        {{aae_status,1}, verify_console_aae_status},
                        {{cluster_info, 1}, verify_console_cluster_info},
                        {{reload_code, 1}, verify_console_reload_code},
                        {{repair_2i, 1}, verify_console_repair_2i},
                        {{reformat_indexes, 1}, verify_console_reformat_indexes},
                        {{reformat_objects, 1}, verify_console_reformat_objects},
                        {{bucket_type_status,1}, verify_console_bucket_type_status},
                        {{bucket_type_activate,1}, verify_console_bucket_type_activate},
                        {{bucket_type_create,1}, verify_console_bucket_type_create},
                        {{bucket_type_update,1}, verify_console_bucket_type_update},
                        {{bucket_type_list,1}, verify_console_bucket_type_list}
                      ]}),

    rt_intercept:add(Node,
                     {riak_kv_js_manager,
                      [
                        {{reload,1}, verify_console_reload}
                      ]}),

    rt_intercept:wait_until_loaded(Node),

    riak_admin_tests(Node),
    cluster_tests(Node),
    bucket_tests(Node),
    security_tests(Node),
    riak_admin_handoff_tests(Node),
    pass.

check_admin_cmd(Node, Cmd) ->
    S = string:tokens(Cmd, " "),
    lager:info("Testing riak-admin ~s on ~s", [Cmd, Node]),
    {ok, Out} = rt:admin(Node, S),
    ?assertEqual("pass", Out).

%% Recently we've started calling riak_core_console twice from the
%% same riak-admin invocation; this will result in "passpass" as a
%% return instead of a simple "pass"
check_admin_cmd_2x(Node, Cmd) ->
    S = string:tokens(Cmd, " "),
    lager:info("Testing riak-admin ~s on ~s", [Cmd, Node]),
    {ok, Out} = rt:admin(Node, S),
    ?assertEqual("passpass", Out).

%% Everyone hates regexp but...
check_admin_output(Node, Cmd, Expected) ->
    S = string:tokens(Cmd, " "),
    lager:info("Testing riak-admin ~s on ~s", [Cmd, Node]),
    {ok, Out} = rt:admin(Node, S),
    Result = case re:run(Out, Expected) of
        nomatch -> "fail";
        _ -> "pass"
    end,
    ?assertEqual("pass", Result).
