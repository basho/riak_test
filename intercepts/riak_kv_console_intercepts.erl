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
-module(riak_kv_console_intercepts).
-compile(export_all).
-include("intercept.hrl").

%% See tests/riak_admin_console_tests.erl for more info

-define(M, riak_kv_console_orig).


-define(PASS, io:format("pass", [])).
-define(FAIL, io:format("fail", [])).



verify_console_staged_join(Val) ->
  case Val of
        ["dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_join(Val) ->
    case Val of
        ["dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_leave(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_remove(Val) ->
    case Val of
        ["dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_down(Val) ->
    case Val of
        ["dev98@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_status(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_vnode_status(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_ringready(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_repair_2i(Val) ->
    case Val of
        ["status"] -> ?PASS;
        ["kill"] -> ?PASS;
        ["--speed","5","foo","bar","baz"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_aae_status(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_cluster_info(Val) ->
    case Val of
        ["foo","local"] -> ?PASS;
        ["foo","local","dev99@127.0.0.1"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_reload_code(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_reip(Val) ->
   io:format(user, "XXXX ~p~n", [Val]),
   case Val of
        ["a", "b"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_reformat_indexes(Val) ->
   case Val of
        ["--downgrade"] -> ?PASS;
        ["5"] -> ?PASS;
        ["5", "--downgrade"] -> ?PASS;
        ["6", "7"] -> ?PASS;
        ["6", "7", "--downgrade"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_reformat_objects(Val) ->
   case Val of
        ["true"] -> ?PASS;
        ["true","1"] -> ?PASS;
        _ -> ?FAIL
    end.

