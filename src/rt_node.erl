%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(rt_node).
-include("rt.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start/1,
         start_and_wait/1,
         async_start/1,
         stop/1,
         stop_and_wait/1,
         upgrade/2,
         upgrade/3,
         slow_upgrade/3,
         join/2,
         staged_join/2,
         plan_and_commit/1,
         do_commit/1,
         leave/1,
         down/2,
         heal/1,
         partition/2,
         remove/2,
         brutal_kill/1]).

-define(HARNESS, (rt_config:get(rt_harness))).

%% @doc Start the specified Riak node
start(Node) ->
    ?HARNESS:start(Node).

%% @doc Start the specified Riak `Node' and wait for it to be pingable
start_and_wait(Node) ->
    start(Node),
    ?assertEqual(ok, rt:wait_until_pingable(Node)).

async_start(Node) ->
    spawn(fun() -> start(Node) end).

%% @doc Stop the specified Riak `Node'.
stop(Node) ->
    lager:info("Stopping riak on ~p", [Node]),
    timer:sleep(10000), %% I know, I know!
    ?HARNESS:stop(Node).
    %%rpc:call(Node, init, stop, []).

%% @doc Stop the specified Riak `Node' and wait until it is not pingable
stop_and_wait(Node) ->
    stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)).

%% @doc Upgrade a Riak `Node' to the specified `NewVersion'.
upgrade(Node, NewVersion) ->
    ?HARNESS:upgrade(Node, NewVersion).

%% @doc Upgrade a Riak `Node' to the specified `NewVersion' and update
%% the config based on entries in `Config'.
upgrade(Node, NewVersion, Config) ->
    ?HARNESS:upgrade(Node, NewVersion, Config).

%% @doc Upgrade a Riak node to a specific version using the alternate
%%      leave/upgrade/rejoin approach
slow_upgrade(Node, NewVersion, Nodes) ->
    lager:info("Perform leave/upgrade/join upgrade on ~p", [Node]),
    lager:info("Leaving ~p", [Node]),
    leave(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    upgrade(Node, NewVersion),
    lager:info("Rejoin ~p", [Node]),
    join(Node, hd(Nodes -- [Node])),
    lager:info("Wait until all nodes are ready and there are no pending changes"),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),
    ok.

%% @doc Have `Node' send a join request to `PNode'
join(Node, PNode) ->
    R = rpc:call(Node, riak_core, join, [PNode]),
    lager:info("[join] ~p to (~p): ~p", [Node, PNode, R]),
    ?assertEqual(ok, R),
    ok.

%% @doc Have `Node' send a join request to `PNode'
staged_join(Node, PNode) ->
    R = rpc:call(Node, riak_core, staged_join, [PNode]),
    lager:info("[join] ~p to (~p): ~p", [Node, PNode, R]),
    ?assertEqual(ok, R),
    ok.

plan_and_commit(Node) ->
    timer:sleep(500),
    lager:info("planning and commiting cluster join"),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            lager:info("plan: ring not ready"),
            timer:sleep(100),
            plan_and_commit(Node);
        {ok, _, _} ->
            lager:info("plan: done"),
            do_commit(Node)
    end.

do_commit(Node) ->
    case rpc:call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            lager:info("commit: plan changed"),
            timer:sleep(100),
            rt:maybe_wait_for_changes(Node),
            plan_and_commit(Node);
        {error, ring_not_ready} ->
            lager:info("commit: ring not ready"),
            timer:sleep(100),
            rt:maybe_wait_for_changes(Node),
            do_commit(Node);
        {error,nothing_planned} ->
            %% Assume plan actually committed somehow
            ok;
        ok ->
            ok
    end.

%% @doc Have the `Node' leave the cluster
leave(Node) ->
    R = rpc:call(Node, riak_core, leave, []),
    lager:info("[leave] ~p: ~p", [Node, R]),
    ?assertEqual(ok, R),
    ok.

%% @doc Have `Node' remove `OtherNode' from the cluster
remove(Node, OtherNode) ->
    ?assertEqual(ok,
                 rpc:call(Node, riak_kv_console, remove, [[atom_to_list(OtherNode)]])).

%% @doc Have `Node' mark `OtherNode' as down
down(Node, OtherNode) ->
    rpc:call(Node, riak_kv_console, down, [[atom_to_list(OtherNode)]]).

%% @doc partition the `P1' from `P2' nodes
%%      note: the nodes remained connected to riak_test@local,
%%      which is how `heal/1' can still work.
partition(P1, P2) ->
    OldCookie = rpc:call(hd(P1), erlang, get_cookie, []),
    NewCookie = list_to_atom(lists:reverse(atom_to_list(OldCookie))),
    [true = rpc:call(N, erlang, set_cookie, [N, NewCookie]) || N <- P1],
    [[true = rpc:call(N, erlang, disconnect_node, [P2N]) || N <- P1] || P2N <- P2],
    rt:wait_until_partitioned(P1, P2),
    {NewCookie, OldCookie, P1, P2}.

%% @doc heal the partition created by call to `partition/2'
%%      `OldCookie' is the original shared cookie
heal({_NewCookie, OldCookie, P1, P2}) ->
    Cluster = P1 ++ P2,
    % set OldCookie on P1 Nodes
    [true = rpc:call(N, erlang, set_cookie, [N, OldCookie]) || N <- P1],
    rt:wait_until_connected(Cluster),
    {_GN, []} = rpc:sbcast(Cluster, riak_core_node_watcher, broadcast),
    ok.

% when you just can't wait
brutal_kill(Node) ->
    rt_cover:maybe_stop_on_node(Node),
    lager:info("Killing node ~p", [Node]),
    OSPidToKill = rpc:call(Node, os, getpid, []),
    %% try a normal kill first, but set a timer to
    %% kill -9 after 5 seconds just in case
    rpc:cast(Node, timer, apply_after,
             [5000, os, cmd, [io_lib:format("kill -9 ~s", [OSPidToKill])]]),
    rpc:cast(Node, os, cmd, [io_lib:format("kill -15 ~s", [OSPidToKill])]),
    ok.
