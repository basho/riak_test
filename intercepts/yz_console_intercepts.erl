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
-module(yz_console_intercepts).
-compile(export_all).
-include("intercept.hrl").

%% See tests/riak_admin_console_tests.erl for more info

-define(PASS, io:format("pass", [])).
-define(FAIL, io:format("fail", [])).


verify_console_aae_status(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_switch_to_new_search(Val) ->
    case Val of
        [] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_create_schema(Val) ->
    case Val of
        ["foobar", _] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_show_schema(Val) ->
    case Val of
        ["foobar"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_add_to_schema(Val) ->
    case Val of
        ["foobar", "field", "barfoo", "store=true"] -> ?PASS;
        ["foobar", "dynamicfield", "foobaz"] -> ?PASS;
        _ -> ?FAIL
    end.

verify_console_remove_from_schema(Val) ->
    case Val of
        ["foobar", "barfoo"] -> ?PASS;
        _ -> ?FAIL
    end.
