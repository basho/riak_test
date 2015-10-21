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
%% -------------------------------------------------------------------
%%% @copyright (C) 2015, Basho Technologies
%%% @doc
%%% riak_test for riak_kv_sweeper and 
%%%
%%% Verify that the sweeper doesn't reap until we set a short grace period
%%%
%%% @end

-module(verify_sweep_reaper).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(NUM_NODES, 1).
-define(NUM_KEYS, 1000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).

confirm() ->
    Config = [{riak_core, 
               [{ring_creation_size, 4}
               ]},
              {riak_kv,
               [{delete_mode, keep},
                {sweep_tick, 1000},       %% Speed up sweeping
                {anti_entropy, {off, []}} %% No AAE please
               ]}
             ],

    Nodes = rt:build_cluster(1, Config),

    [Client] =  create_pb_clients(Nodes),

    %% Write key.
    Key1 = <<"1">>,
    write_key(Client, Key1, <<"one">>),
    {fail,false} = delete_key(Client, Key1, 10, 6000),
    

    set_tombstone_grace(Nodes, 5),
    %% Write key.
    Key2 = <<"2">>,
    write_key(Client, Key2, <<"two">>),
    ok = delete_key(Client, Key2, 10, 6000),
    ok = delete_key(Client, Key1, 10, 6000),

    pass.

%%% Client/Key ops
create_pb_clients(Nodes) ->
    [begin
         C = rt:pbc(N),
         riakc_pb_socket:set_options(C, [queue_if_disconnected]),
         C
     end || N <- Nodes].

coordinating_client(Clients, Preflist) ->
    {{_, FirstPrimary}, primary} = lists:keyfind(primary, 2, Preflist),
    lists:keyfind(FirstPrimary, 1, Clients).

up_client(DeadNode, Clients) ->
    {value, _, LiveClients} = lists:keytake(DeadNode, 1, Clients),
    hd(LiveClients).

write_key(Client, Key, Val) ->
    write_key(Client, Key, Val, []).

write_key(Client, Key, Val, Opts) when is_binary(Val) ->
    Object = case riakc_pb_socket:get(Client, ?BUCKET, Key, []) of
                 {ok, O1} ->
                     lager:info("writing existing!"),
                     riakc_obj:update_value(O1, Val);
                 _ ->
                     lager:info("writing new!"),
                     riakc_obj:new(?BUCKET, Key, Val)
             end,
    riakc_pb_socket:put(Client, Object, Opts).

read_key(Client, Key) ->
    riakc_pb_socket:get(Client, ?BUCKET, Key, []).

delete_key(Client, Key, Retry, Delay) ->
    riakc_pb_socket:delete(Client, ?BUCKET, Key),
    Fun = 
        fun() ->
                case  riakc_pb_socket:get(Client, ?BUCKET, Key, [deletedvclock]) of
                    {error, notfound, _} ->
                        lager:info("Not reaped yet", []),
                        false;
                    {error, notfound} ->
                        lager:info("Reaped", []),
                        true
                end
        end,
    rt:wait_until(Fun, Retry, Delay).

set_tombstone_grace(Nodes, Time) ->
    rpc:multicall(Nodes, application, set_env, [riak_kv, tombstone_grace_period,Time]).
