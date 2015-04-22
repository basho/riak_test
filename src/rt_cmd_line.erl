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

-module(rt_cmd_line).
-include_lib("eunit/include/eunit.hrl").

-export([admin/2, 
         admin/3,
         riak/2,
         riak_repl/2,
         search_cmd/2,
         attach/2, 
         attach_direct/2,
         console/2
        ]).

-define(HARNESS, (rt_config:get(rt_harness))).

%% @doc Call 'bin/riak-admin' command on `Node' with arguments `Args'
admin(Node, Args) ->
    rt_harness:admin(Node, Args).

%% @doc Call 'bin/riak-admin' command on `Node' with arguments `Args'.
%% The third parameter is a list of options. Valid options are:
%%    * `return_exit_code' - Return the exit code along with the command output
admin(Node, Args, Options) ->
    rt_harness:admin(Node, Args, Options).

%% @doc Call 'bin/riak' command on `Node' with arguments `Args'
riak(Node, Args) ->
    rt_harness:riak(Node, Args).


%% @doc Call 'bin/riak-repl' command on `Node' with arguments `Args'
riak_repl(Node, Args) ->
    rt_harness:riak_repl(Node, Args).

search_cmd(Node, Args) ->
    {ok, Cwd} = file:get_cwd(),
    rpc:call(Node, riak_search_cmd, command, [[Cwd | Args]]).

%% @doc Runs `riak attach' on a specific node, and tests for the expected behavoir.
%%      Here's an example: ```
%%      rt_cmd_line:attach(Node, [{expect, "erlang.pipe.1 \(^D to exit\)"},
%%                       {send, "riak_core_ring_manager:get_my_ring()."},
%%                       {expect, "dict,"},
%%                       {send, [4]}]), %% 4 = Ctrl + D'''
%%      `{expect, String}' scans the output for the existance of the String.
%%         These tuples are processed in order.
%%
%%      `{send, String}' sends the string to the console.
%%         Once a send is encountered, the buffer is discarded, and the next
%%         expect will process based on the output following the sent data.
%%
attach(Node, Expected) ->
    rt_harness:attach(Node, Expected).

%% @doc Runs 'riak attach-direct' on a specific node
%% @see rt_cmd_line:attach/2
attach_direct(Node, Expected) ->
    rt_harness:attach_direct(Node, Expected).

%% @doc Runs `riak console' on a specific node
%% @see rt_cmd_line:attach/2
console(Node, Expected) ->
    rt_harness:console(Node, Expected).
