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
-module(riak_kv_js_manager_intercepts).
-compile(export_all).
-include("intercept.hrl").

%% See tests/riak_admin_console_tests.erl for more info

-define(M, riak_kv_js_manager_orig).


-define(PASS, io:format("pass", [])).
-define(FAIL, io:format("fail", [])).

verify_console_reload(Val) ->
    io:format(user, "XXXX ~p~n", [Val]),
  case Val of
        ["foo","bar","baz"] -> ?PASS;
        _ -> ?FAIL
    end.

