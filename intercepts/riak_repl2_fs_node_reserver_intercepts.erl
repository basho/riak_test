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

-module(riak_repl2_fs_node_reserver_intercepts).
-compile([export_all, nowarn_export_all]).
-include("intercept.hrl").

-define(M, riak_repl2_fs_node_reserver_orig).

%% @doc Provide an intercept which forces the node reserver to fail when
%%      attempting to reserve a node with a location_down message.
down_reserve({reserve, _Partition}, _From, State) ->
    io:format("down_reserve~n"),
    ?I_INFO("down_reserve~n"),
    {reply, down, State};
down_reserve(Message, From, State) ->
    ?M:handle_call_orig(Message, From, State).
