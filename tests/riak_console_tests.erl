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
-module(riak_console_tests).
-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    %% Deploy a node to test against
    lager:info("Deploy node to test riak command line"),
    [Node] = rt:deploy_nodes(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node])),
    rt_intercept:add(Node,
                     {riak_core_console,
                      [
                        {{stage_remove,1}, verify_console_stage_remove},
                        {{stage_leave,1}, verify_console_stage_leave},
                        {{stage_replace, 1}, verify_console_stage_replace},
                        {{stage_force_replace, 1}, verify_console_stage_force_replace},
                        {{stage_resize_ring, 1}, verify_console_stage_resize_ring},
                        {{print_staged, 1}, verify_console_print_staged},
                        {{commit_staged, 1}, verify_console_commit_staged},
                        {{clear_staged, 1}, verify_console_clear_staged},
                        {{add_user, 1}, verify_console_add_user},
                        {{alter_user, 1}, verify_console_alter_user},
                        {{del_user, 1}, verify_console_del_user},
                        {{add_source, 1}, verify_console_add_source},
                        {{del_source, 1}, verify_console_del_source},
                        {{grant, 1}, verify_console_grant},
                        {{revoke, 1}, verify_console_revoke},
                        {{print_user,1}, verify_console_print_user},
                        {{print_users,1}, verify_console_print_users},
                        {{print_sources, 1}, verify_console_print_sources},
                        {{security_enable,1}, verify_console_security_enable},
                        {{security_disable,1}, verify_console_security_disable},
                        {{security_status,1}, verify_console_security_stats},
                        {{ciphers,1}, verify_console_ciphers} ]}),

    rt_intercept:add(Node,
                     {riak_kv_console,
                      [
                        {{staged_join,1}, verify_console_staged_join},
                        {{bucket_type_status,1}, verify_console_bucket_type_status},
                        {{bucket_type_activate,1}, verify_console_bucket_type_activate},
                        {{bucket_type_create,1}, verify_console_bucket_type_create},
                        {{bucket_type_update,1}, verify_console_bucket_type_update},
                        {{bucket_type_list,1}, verify_console_bucket_type_list}
                      ]}),

    rt_intercept:wait_until_loaded(Node),


    %% riak-admin cluster
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

    %% riak-admin bucket_type
    check_admin_cmd(Node, "bucket-type status foo"),
    check_admin_cmd(Node, "bucket-type activate foo"),
    check_admin_cmd(Node, "bucket-type create foo {\"props\":{[]}}"),
    check_admin_cmd(Node, "bucket-type update foo {\"props\":{[]}}"),
    check_admin_cmd(Node, "bucket-type list"),

    %% riak-admin security
    check_admin_cmd(Node, "security add-user foo"),
    check_admin_cmd(Node, "security add-user foo x1=y1 x2=y2"),
    check_admin_cmd(Node, "security alter-user foo x1=y1"),
    check_admin_cmd(Node, "security alter-user foo x1=y1 x2=y2"),
    check_admin_cmd(Node, "security del-user foo"),

    %% TODO: update add-source docs: users comma sep
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
    check_admin_cmd(Node, "security enable"),
    check_admin_cmd(Node, "security disable"),
    check_admin_cmd(Node, "security status"),
    check_admin_cmd(Node, "security print-user foo"),
    check_admin_cmd(Node, "security ciphers foo"),

     pass.

check_admin_cmd(Node, Cmd) ->
    S = string:tokens(Cmd, " "),
    lager:info("Testing riak-admin ~s on ~s", [Cmd, Node]),
    {ok, Out} = rt:admin(Node, S),
    ?assertEqual("pass", Out).

