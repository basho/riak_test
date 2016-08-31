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

-module(riak_pipe_vnode_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_pipe_vnode_orig).

%%% Intercepts

%% @doc Log every command received by the vnode during handoff, so a
%% test can analyze them later. Sends the first element of the `Cmd'
%% tuple (the command type) to the process registered as
%% `riak_test_collector'.
%%
%% Tests using this intercept are expected to start the collector
%% process themselves.
log_handoff_command(Cmd, Sender, State) ->
    try
        riak_test_collector ! element(1, Cmd)
    catch error:badarg when not is_tuple(Cmd) ->
            ?I_INFO("Cmd was not a tuple");
          error:badarg ->
            ?I_INFO("Collector process not registered")
    end,
    ?M:handle_handoff_command_orig(Cmd, Sender, State).

