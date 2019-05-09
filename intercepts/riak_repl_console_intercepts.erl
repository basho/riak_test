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

-module(riak_repl_console_intercepts).
-compile([export_all, nowarn_export_all]).
-include("intercept.hrl").

-define(M, riak_repl_console_orig).

%% Hello - if you mess with the riak repl script, this test might help you
%% out. It intercepts (registered) calls to riak_repl_console and checks that
%% parameters are received correctly. Tests using these intercepts will
%% fail if ?PASS *isn't* returned.

%% Please see ./tests/replication2_console_tests.erl for more information!

%% these *strings* are passed back out as IO from the riak repl shell script
%% The IO from this script is used in asserts in
%% replication2_console_tests.erl
-define(PASS, io:format("pass", [])).
-define(FAIL, io:format("fail", [])).

verify_clusterstats(Val) ->
    case Val of
        [] -> ?PASS;
        ["cluster_mgr"] -> ?PASS;
        ["192.168.1.1:5555"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_clustername(Val) ->
    case Val of
        ["foo"] -> ?PASS;
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_max_fssource_node(Val) ->
    case Val of
        "" -> ?PASS;
        ["99"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_max_fssource_cluster(Val) ->
    case Val of
        "" -> ?PASS;
        ["99"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_max_fssink_node(Val) ->
    case Val of
        "" -> ?PASS;
        ["99"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_fullsync(Val) ->
    case Val of
        ["enable","foo"] -> ?PASS;
        ["disable","bar"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_realtime(Val) ->
    case Val of
        ["enable","foo"] -> ?PASS;
        ["disable","bar"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_realtime_cascades(Val) ->
    case Val of
        [] -> ?PASS; %% display current cascades info, no additional
                               %% params
        ["always"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_proxy_get(Val) ->
    case Val of
        ["enable","foo"] -> ?PASS;
        ["disable","bar"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_add_nat_map(Val) ->
    case Val of
        ["1.2.3.4:4321","192.168.1.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_del_nat_map(Val) ->
    case Val of
        ["1.2.3.4:4321","192.168.1.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_show_nat_map(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_modes(Val) ->
     case Val of
        [] -> ?PASS;
        ["mode_repl12"] -> ?PASS;
        ["mode_repl12","mode_repl13"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_add_block_provider_redirect(Val) ->
    case Val of
        ["a","b"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_show_block_provider_redirect(Val) ->
    case Val of
        ["a"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_delete_block_provider_redirect(Val) ->
    case Val of
        ["a"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_show_local_cluster_id(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

