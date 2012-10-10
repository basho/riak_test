%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(upgrade).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Nodes = rt:build_cluster(["1.0.3", "1.0.3", "1.1.4", current]),
    [Node1, Node2, Node3, _Node4] = Nodes,

    lager:info("Writing 100 keys"),
    rt:systest_write(Node1, 100, 3),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),
    rt:upgrade(Node1, current),
    lager:info("Ensuring keys still exist"),
    rt:stop(Node2),
    rt:stop(Node3),
    rt:systest_read(Node1, 100, 1),
    %% ?assertEqual([], rt:systest_read(Node1, 100, 1)),
    wait_until_readable(Node1, 100),
    pass.

wait_until_readable(Node, N) ->
    rt:wait_until(Node,
                  fun(_) ->
                          [] == rt:systest_read(Node, N, 1)
                  end).


