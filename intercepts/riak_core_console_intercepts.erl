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
%
%% -------------------------------------------------------------------
-module(riak_core_console_intercepts).
-compile(export_all).
-include("intercept.hrl").

%% See tests/riak_admin_console_tests.erl for more info

-define(M, riak_core_console_orig).


-define(PASS, io:format("pass", [])).
-define(FAIL, io:format("fail", [])).

verify_console_stage_leave(Val) ->
    case Val of
        [] -> ?PASS;
        ["dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_stage_remove(Val) ->
    case Val of
        ["dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_stage_replace(Val) ->
    case Val of
        ["dev98@127.0.0.1","dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_stage_force_replace(Val) ->
    case Val of
        ["dev98@127.0.0.1","dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_stage_resize_ring(Val) ->
    case Val of
        ["abort"] -> ?PASS;
        ["42"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_print_staged(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_commit_staged(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_clear_staged(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_add_user(Val) ->
    case Val of
        ["foo"] -> ?PASS;
        ["foo", "x1=y1", "x2=y2"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_alter_user(Val) ->
    case Val of
        ["foo", "x1=y1"] -> ?PASS;
        ["foo", "x1=y1", "x2=y2"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_del_user(Val) ->
    case Val of
        ["foo"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_add_group(Val) ->
    case Val of
        ["group"] -> ?PASS;
        ["group", "x1=y1", "x2=y2"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_alter_group(Val) ->
    case Val of
        ["group", "x1=y1", "x2=y2"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_del_group(Val) ->
    case Val of
        ["group"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_add_source(Val) ->
    case Val of
        ["all","192.168.100.0/22","x","x1=y1"] -> ?PASS;
        ["all","192.168.100.0/22","y"] -> ?PASS;
        ["foo,bar","192.168.100.0/22","x","x1=y1"] -> ?PASS;
        ["foo,bar,baz","192.168.100.0/22","x","x1=y1","x2=y2"] -> ?PASS;
        _ -> ?FAIL
    end.


verify_console_del_source(Val) ->
    case Val of
        ["all","192.168.100.0/22"] -> ?PASS;
        ["x","192.168.100.0/22"] -> ?PASS;
        ["x,y,z","192.168.100.0/22"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_grant(Val) ->
    case Val of
        ["foo","on","any","my_bucket","to","x"] -> ?PASS;
        ["foo,bar","on","any","my_bucket","to","x"] -> ?PASS;
        ["foo","on","any","my_bucket","to","x,y,z"] -> ?PASS;
        ["foo,bar,baz","on","any","my_bucket","to","y"] -> ?PASS;
        ["foo,bar,baz","on","foo","my_bucket","to","y"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_revoke(Val) ->
    case Val of
        ["foo","on","any","my_bucket","from","x"] -> ?PASS;
        ["foo,bar","on","any","my_bucket","from","x"] -> ?PASS;
        ["foo","on","any","my_bucket","from","x,y,z"] -> ?PASS;
        ["foo,bar,baz","on","any","my_bucket","from","y"] -> ?PASS;
        ["foo,bar,baz","on","foo","my_bucket","from","y"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_print_user(Val) ->
    case Val of
        ["foo"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_print_users(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_print_group(Val) ->
    case Val of
        ["group"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_print_groups(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_print_grants(Val) ->
    case Val of
        ["foo"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_print_sources(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_transfers(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_member_status(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_ring_status(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_transfer_limit(Val) ->
    case Val of
        ["1"] -> ?PASS;
        ["dev55@127.0.0.1", "1"] -> ?PASS;
        _ -> ?FAIL
    end.
