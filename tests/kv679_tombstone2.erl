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
%%% riak_test for kv679 another doomstone flavour.
%%%
%%% issue kv679 is a possible dataloss issue, it's basically caused by
%%% the fact that per key logical clocks can go backwards in time in
%%% certain situations. The situation under test here is as follows:
%%% Create value. Delete value (write tombstone reap tombstone from
%%% all but one crashed primary). Write new value. Crashed primary
%%% comes back, read repair tombstone dominates the new value. This
%%% test depends on things happening inside a certain time limit, so
%%% technically it is not determenistic. If you think of a better way,
%%% please let me know.
%%% @end

-module(kv679_tombstone2).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"kv679">>).
-define(KEY, <<"test">>).

confirm() ->
    Config = [{riak_kv, [{delete_mode, 10000}]}, %% 10 seconds to reap.
              {riak_core, [{ring_creation_size, 8},
                           {vnode_management_timer, 1000},
                           {handoff_concurrency, 100},
                           {vnode_inactivity_timeout, 1000}]}],

    %% 4 'cos I want a perfect preflist
    Nodes = rt:build_cluster(4, Config),

    Clients =  kv679_tombstone:create_pb_clients(Nodes),

    %% Get preflist for key
    PL = kv679_tombstone:get_preflist(hd(Nodes)),

    ?assert(perfect_preflist(PL)),

    %% Patsy is the primary node that will take a fall, where the
    %% lingering doomstone will stay
    {CoordClient, Patsy} = get_coord_client_and_patsy(Clients, PL),

    lager:info("CoordClient ~p~nPatsy ~p~n", [CoordClient, Patsy]),

    %% Write key some times
    kv679_tombstone:write_key(CoordClient, [<<"bob">>, <<"phil">>, <<"pete">>]),

    dump_clock(CoordClient),

    lager:info("wrote key thrice"),

    delete_key(CoordClient),

    lager:info("deleted key"),

    %% kill the patsy, must happen before the reap
    rt:brutal_kill(Patsy),

    lager:info("killed the patsy"),

    %% A time to reap wait for the up nodes to reap, can't use
    %% kv679_tombstone:read_it_and_reap
    timer:sleep(15000),

    lager:info("tombstone (should be) reaped"),

    %% %% write the key again, this will start a new clock, a clock
    %% that is in the past of that un-reaped primary tombstone. We use the
    %% same node to get the same clock.
    kv679_tombstone:write_key(CoordClient, [<<"jon">>]),

    dump_clock(CoordClient),

    %% %% Bring the patsy back up, and wait until the preflists are as
    %% before
    rt:start_and_wait(Patsy),

    rt:wait_until(fun() ->
                          PL2 = kv679_tombstone:get_preflist(Patsy),
                          PL == PL2
                  end),

    %% Read a few times, just in case (repair, then reap, etc)
    Res = [begin
               timer:sleep(100),
               {I, kv679_tombstone:read_key(CoordClient)}
           end || I <- lists:seq(1, 5)],

    lager:info("res ~p", [Res]),

    First = hd(lists:dropwhile(fun({_I, {ok, _}}) -> false;
                                  (_) -> true end,
                               Res)),

    lager:info("res ~p", [First]),

    %% The last result
    {_, Res2} = hd(lists:reverse(Res)),

    ?assertMatch({ok, _}, Res2),
    {ok, Obj} = Res2,
    ?assertEqual(<<"jon">>, riakc_obj:get_value(Obj)),

    pass.


perfect_preflist(PL) ->
    %% N=3 primaries, each on a unique node
    length(lists:usort([Node || {{_Idx, Node}, Type} <- PL,
                                Type == primary])) == 3.

get_coord_client_and_patsy(Clients, PL) ->
    {CoordNode, _}=CoordClient=kv679_tombstone:coordinating_client(Clients, PL),
    PL2 = [Node || {{_Idx, Node}, Type} <- PL,
                   Type == primary,
                   Node /= CoordNode],
    {CoordClient, hd(lists:reverse(PL2))}.

delete_key({_, Client}) ->
    {ok, Obj} = riakc_pb_socket:get(Client, ?BUCKET, ?KEY),
    riakc_pb_socket:delete_obj(Client, Obj).

dump_clock({Node, Client}) ->
    case riakc_pb_socket:get(Client, ?BUCKET, ?KEY) of
        {ok, O} ->
            VCE = riakc_obj:vclock(O),
            VC = rt:rpc_call(Node, riak_object, decode_vclock, [VCE]),
            lager:info("VC ~p~n", [VC]),
            NodeId = erlang:crc32(term_to_binary(Node)),
            Id = <<NodeId:32/unsigned-integer>>,
            lager:info("Coord Node ID ~p~n", [Id]);
        Res ->
            lager:info("no clock in ~p~n", [Res])
    end.

