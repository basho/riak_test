%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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

-module(repl_rand).

-behaviour(eqc_statem).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([initial_state/0, command/1, precondition/2, postcondition/3,
     next_state/3]).
-compile(export_all).

-define(UTIL_MODULE, repl_util).
-define(TEST_MODULE, replication2_pg).
-define(RT_MODULE, rt).
-define(READ_PROC, read_proc).
-define(WRITE_PROC, write_proc).

-define(CLUSTER_CONF, [
        {riak_repl,
        [
            {proxy_get, enabled},
            {fullsync_on_connect, false}
        ]}
    ]).
-define(NUM_WRITES, 1000).
-define(NUM_READS, 1000).

-record(state, {
                realtime_enabled,
                realtime_started}).

%%--------------------------------------------------------------------
%%% riak_test callback
%%--------------------------------------------------------------------

confirm() ->
    AllTests =
        [
            rand_test
        ],
    lager:error("run riak_test with -t Mod:test1 -t Mod:test2"),
    lager:error("The runnable tests in this module are: ~p", [AllTests]),
    ?assert(false).

rand_test() ->
    banner("rand_test", false),
    eqc:quickcheck(?MODULE:test_main()),
    stop_load().


test_main() ->
    setup_replication(),
    start_load(leader()),
    ?FORALL(Cmds, commands(?MODULE),
        ?TRAPEXIT(
           begin
          {History,State,Result} = run_commands(?MODULE, Cmds),
           ?WHENFAIL(io:format("History: ~w\nState: ~w\nResult: ~w\n",
                       [pretty_history(History),State,Result]),
                 aggregate(command_names(Cmds), Result =:= ok))
          end)).
    %stop_load()

initial_state() ->
    #state{realtime_enabled = false,
           realtime_started = false}.

setup_replication() ->

    ets:new(?MODULE, [ordered_set, named_table, public]),

    case ?TEST_MODULE:setup_repl_clusters(?CLUSTER_CONF) of 
       {LeaderA, ANodes, BNodes, _CNodes, _AllNodes} ->
          ets:insert(?MODULE, {leaderA, LeaderA}),
          ets:insert(?MODULE, {aNodes, ANodes}),
          ets:insert(?MODULE, {bNodes, BNodes}),

          lager:info("setup repl cluster succeded: LeaderA=~s, connecting to B", 
            [LeaderA]),

          BPort = get_cluster_mgr_port(BNodes),

          lager:info("connect cluster A:~p to B on port ~p", [LeaderA, BPort]),
          repl_util:connect_cluster(LeaderA, "127.0.0.1", BPort),
          ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B"));

       _ ->
          lager:error("setup repl cluster failed!"),
           ?assert(false)
    end.

%%--------------------------------------------------------------------
%%% Statem callbacks
%%--------------------------------------------------------------------

command(_S) ->
    oneof([
           {call,?UTIL_MODULE,enable_realtime, [leader(), cluster()]},
           {call,?UTIL_MODULE,start_realtime, [leader(), cluster()]},
           {call,?UTIL_MODULE,stop_realtime, [leader(), cluster()]},
           {call,?UTIL_MODULE,disable_realtime, [leader(), cluster()]}

    ]).

precondition(S, {call,_,enable_realtime, [_Leader, _Cluster]}) ->
    (S#state.realtime_enabled =:= false);%% and (S#state.realtime_started =:= false);
precondition(S, {call,_,start_realtime, [_Leader, _Cluster]}) ->
    %(S#state.realtime_enabled =:= true) and 
    (S#state.realtime_started =:= false);
precondition(S, {call,_,stop_realtime, [_Leader, _Cluster]}) ->
    (S#state.realtime_enabled =:= true); % and (S#state.realtime_started =:= true);
precondition(S, {call,_,disable_realtime, [_Leader, _Cluster]}) ->
    (S#state.realtime_enabled =:= true); % and (S#state.realtime_started =:= false);
precondition(_, _) ->
    true.

pretty_history(History) ->
    [{pretty_state(State),Result} || {State,Result} <- History].

pretty_state(#state{realtime_enabled = RealtimeEnabled} = S) ->
    S#state{realtime_enabled = RealtimeEnabled}.

next_state(S, _V, {call,_,enable_realtime,[Leader, _Cluster]}) ->
    lager:debug("got enable_realtime callback, leader:~s, realtime_enabled:~s", 
        [Leader, S#state.realtime_enabled]),
    rt:wait_until_ring_converged(get_aNodes()),
    S#state{realtime_enabled = true};
next_state(S, _V, {call,_,start_realtime,[_Leader, _Cluster]}) ->
    lager:debug("got start_realtime callback"),
    rt:wait_until_ring_converged(get_aNodes()),
    S#state{realtime_started = true};
next_state(S, _V, {call,_,stop_realtime,[_Leader, _Cluster]}) ->
    lager:debug("got stop_realtime callback"), 
    rt:wait_until_ring_converged(get_aNodes()),
    S#state{realtime_started = false};
next_state(S, _V, {call,_,disable_realtime,[Leader, _Cluster]}) ->
    lager:debug("got disable_realtime callback, leader:~s, realtime_enabled:~s", 
        [Leader, S#state.realtime_enabled]),
    rt:wait_until_ring_converged(get_aNodes()),
    S#state{realtime_enabled = false}.

postcondition(_S, {call,_,enable_realtime, [_Leader, _Cluster]}, Result) ->
    lager:debug("postcondition enable_realtime, Result:~s", [Result]),
    Result =:= ok;
postcondition(_S, {call,_,disable_realtime, [_Leader, _Cluster]}, Result) ->
    lager:debug("postcondition disable_realtime, Enabled: ~s", [Result]),
    Result =:= ok;
postcondition(_,_,_) ->
    true.


%%--------------------------------------------------------------------
%%% Generators
%%--------------------------------------------------------------------
cluster() ->
    oneof(['A']).


%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
get_aNodes() ->
    case ets:lookup(?MODULE, aNodes) of
        [] ->
            lager:error("No ANodes in ets!");
        [H|_] ->
            {_,ANodes} = H,
            ANodes
    end.

leader() ->
    case ets:lookup(?MODULE, leaderA) of
        [] ->
            lager:error("No leaderA in ets!");
        [H|_] ->
            {_,LeaderA} = H,
            LeaderA
    end.

get_cluster_mgr_port(Nodes) ->
          
    [FirstNode|_] = Nodes,
    {ok, {_IP, Port}} = rpc:call(FirstNode, application, get_env,
        [riak_core, cluster_mgr]),
    Port.

start_load(Node) ->
    register(?WRITE_PROC, spawn(repl_rand, do_writes, [Node])),
    timer:sleep(1000),
    register(?READ_PROC, spawn(repl_rand, do_reads, [Node])).

stop_load() ->
    exit(whereis(?WRITE_PROC), ok),
    exit(whereis(?READ_PROC), ok).

do_writes(Node) ->
    case rt:systest_write(Node, ?NUM_WRITES) of 
        [] ->
            lager:info("Completed ~s writes to ~s.", [integer_to_list(?NUM_WRITES), Node ]),
            do_writes(Node);
        _ ->
            lager:error('Writes to ~s failed!', [Node])
    end.

do_reads(Node) ->
    case rt:systest_read(Node, ?NUM_READS) of 
        [] ->
            lager:info("Completed ~s reads on ~s.", [integer_to_list(?NUM_READS), Node ]),
            do_reads(Node);
        _ ->
            lager:error('Nothing to read from ~s', [Node]),
            do_reads(Node)
    end.


banner(T) ->
banner(T, false).

banner(T, SSL) ->
    lager:info("----------------------------------------------"),
    lager:info("----------------------------------------------"),
    lager:info("~s, SSL ~s",[T, SSL]),
    lager:info("----------------------------------------------"),
    lager:info("----------------------------------------------").

