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
%%% @copyright (C) 2014, Basho Technologies
%%% @doc
%%% riak_test for kv679 doomstone flavour.
%%%
%%% issue kv679 is a possible dataloss issue, it's basically caused by
%%% the fact that per key logical clocks can go backwards in time in
%%% certain situations. The situation under test here is as follows:
%%% Create value
%%% Delete value (write tombstone (including one fallback) reap tombstone from primaries only)
%%% write new value
%%% fallback hands off and the tombstone dominates the new value.
%%% @end

-module(kv679_tombstone).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"kv679">>).
-define(KEY, <<"test">>).

confirm() ->
    Config = [{riak_core, [{ring_creation_size, 8},
                           {vnode_management_timer, 1000},
                           {handoff_concurrency, 100},
                           {vnode_inactivity_timeout, 1000}]}],

    Nodes = rt:build_cluster(4, Config),

    Clients=[P1, _P2, _P3, _P4] =  create_pb_clients(Nodes),

    %% Get preflist for key
    PL = get_preflist(hd(Nodes)),

    CoordClient = coordinating_client(Clients, PL),

    %% Write key some times
    write_key(CoordClient, [<<"bob">>, <<"phil">>, <<"pete">>]),

    lager:info("wrote key thrice"),

    %% %% take a node that is a primary down
    {NewPL, DeadPrimary, _} = kill_primary(PL),

    lager:info("killed a primary"),

    %% %% This way a tombstone finds its way to a fallback, to later
    %% %% wreak DOOM!!!!
    Client = up_client(DeadPrimary, Clients),
    delete_key(Client),

    lager:info("deleted key, and have tombstone response"),

    %% %% Take down the fallback
    {NewPL2, DeadFallback, _DeadPartition} = kill_fallback(NewPL),

    %% %% Bring the primary back up
    _PL3 = start_node(DeadPrimary, NewPL2),

    lager:info("killed the fallback, and restored the primary"),

    %% %% wait for reaping maybe read for vclock ts and wait until there
    %% %% is not one
    UpClient = up_client(DeadFallback, Clients),
    read_it_and_reap(UpClient),

    lager:info("read repaired, and reaped the tombstone"),

    %% %% write the key again, this will start a new clock, a clock
    %% that is in the past of that lingering tombstone. We use the
    %% same node to get the same clock.
    write_key(CoordClient, [<<"jon">>]),

    %% %% bring up that fallback, and wait for it to hand off
    start_fallback_and_wait_for_handoff(DeadFallback),

    %% Read twice, just in case (repair, then reap)
    Res1 = read_key(P1),

    lager:info("TS? ~p~n", [Res1]),
    Res2 = read_key(P1),

    lager:info("res ~p", [Res2]),

    ?assertMatch({ok, _}, Res2),
    {ok, Obj} = Res2,
    ?assertEqual(<<"jon">>, riakc_obj:get_value(Obj)),

    pass.



%%% Client/Key ops
create_pb_clients(Nodes) ->
    [begin
         C = rt:pbc(N),
         riakc_pb_socket:set_options(C, [queue_if_disconnected]),
         {N, C}
     end || N <- Nodes].

coordinating_client(Clients, Preflist) ->
    {{_, FirstPrimary}, primary} = lists:keyfind(primary, 2, Preflist),
    lists:keyfind(FirstPrimary, 1, Clients).

up_client(DeadNode, Clients) ->
    {value, _, LiveClients} = lists:keytake(DeadNode, 1, Clients),
    hd(LiveClients).

write_key(_, []) ->
    ok;
write_key(Client, [Val | Rest]) ->
    ok = write_key(Client, Val),
    ok = write_key(Client, Rest);
write_key({_, Client}, Val) when is_binary(Val) ->
    Object = case riakc_pb_socket:get(Client, ?BUCKET, ?KEY, []) of
                 {ok, O1} ->
                     riakc_obj:update_value(O1, Val);
                 _ ->
                     riakc_obj:new(?BUCKET, ?KEY, Val)
             end,
    riakc_pb_socket:put(Client, Object).

read_key({_, Client}) ->
    riakc_pb_socket:get(Client, ?BUCKET, ?KEY, []).

delete_key({_, Client}) ->
    riakc_pb_socket:delete(Client, ?BUCKET, ?KEY),

    rt:wait_until(fun() ->
                          case  riakc_pb_socket:get(Client, ?BUCKET, ?KEY, [deletedvclock]) of
                              {error, notfound, VC} ->
                                  lager:info("TSVC ~p~n", [VC]),
                                  true;
                              Res ->
                                  lager:info("no ts yet: ~p~n", [Res]),
                                  false
                          end
                  end).

read_it_and_reap({_, Client}) ->
    rt:wait_until(fun() ->
                          case  riakc_pb_socket:get(Client, ?BUCKET, ?KEY, [deletedvclock]) of
                              {error, notfound} ->
                                  true;
                              Res ->
                                  lager:info("not reaped ts yet: ~p~n", [Res]),
                                  false
                          end
                  end).

%%% Node ops

start_node(Node, Preflist) ->
    rt:start_and_wait(Node),
    wait_for_new_pl(Preflist, Node).

get_preflist(Node) ->
    Chash = rpc:call(Node, riak_core_util, chash_key, [{?BUCKET, ?KEY}]),
    UpNodes = rpc:call(Node, riak_core_node_watcher, nodes, [riak_kv]),
    PL = rpc:call(Node, riak_core_apl, get_apl_ann, [Chash, 3, UpNodes]),
    PL.

kill_primary(Preflist) ->
    kill_and_wait(Preflist, primary).

kill_fallback(Preflist) ->
    kill_and_wait(Preflist, fallback).

kill_and_wait(Preflist, Type) ->
        case lists:keytake(Type, 2, Preflist) of
        false ->
            erlang:error(no_nodes_of_type, [Type, Preflist]);
        {value, {{Idx, Node}, Type}, PL2} ->
            kill_node(Node),
            lager:info("killed ~p~n", [Node]),
            [{{_, N2}, _}|_] = PL2,
            {wait_for_new_pl(Preflist, N2), Node, Idx}
    end.

kill_node(Node) ->
    rt:stop_and_wait(Node).

wait_for_new_pl(PL, Node) ->
    rt:wait_until(fun() ->
                          NewPL = get_preflist(Node),
                          lager:info("new ~p~n old ~p~nNode ~p~n", [NewPL, PL, Node]),
                          NewPL /= PL%% andalso contains_fallback(NewPL)
                  end),
    get_preflist(Node).

contains_fallback(PL) ->
    lists:keymember(fallback, 2, PL).

start_fallback_and_wait_for_handoff(DeadFallback) ->
    %% Below is random voodoo shit as I have no idea how to _KNOW_ that handoff has happened
    %% whatver, it takes 2 minutes, force_handoff? 2 minutes son!
    rt:start_and_wait(DeadFallback),
    rpc:call(DeadFallback, riak_core_vnode_manager, force_handoffs, []),
    rt:wait_until_transfers_complete([DeadFallback]).




