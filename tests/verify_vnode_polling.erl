%% -------------------------------------------------------------------
%%
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
%%% @doc
%%% riak_test for soft-limit vnode polling and put fsm routing
%%% see riak/1661 for details.
%%% @end

-module(verify_vnode_polling).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"bucket">>).
-define(KEY, <<"key">>).
-define(VALUE, <<"value">>).

-define(RING_SIZE, 8).

confirm() ->
    Conf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [{allow_mult, true},
                                                 {dvv_enabled, true},
                                                 {ring_creation_size, ?RING_SIZE},
                                                 {vnode_management_timer, 1000},
                                                 {handoff_concurrency, 100},
                                                 {vnode_inactivity_timeout, 1000}]}]}],

    Nodes = rt:build_cluster(5, Conf),
    Node1 = hd(Nodes),

    Preflist = rt:get_preflist(Node1, ?BUCKET, ?KEY),

    lager:info("Got preflist"),
    lager:info("Preflist ~p~n", [Preflist]),

    %% make a client with a node _NOT_ on the preflist
    PLNodes = sets:from_list([Node || {{_Idx, Node}, _Type} <- Preflist]),
    NonPLNodes = sets:to_list(sets:subtract(sets:from_list(Nodes), PLNodes)),

    lager:info("non pl nodes are ~p", [NonPLNodes]),

    {ok, Client} = riak:client_connect(hd(NonPLNodes)),

    lager:info("Attempting to write key to ~p", [hd(NonPLNodes)]),

    %% Write key and confirm error pw=2 unsatisfied
    WriteRes = client_write(Client, ?BUCKET, ?KEY, ?VALUE, [{mbox_check, false}]),

    lager:info("write result was ~p", [WriteRes]),

    ?assertEqual(ok, WriteRes),

    pass.

client_write(Client, Bucket, Key, Value, Opts) ->
    Obj = riak_object:new(Bucket, Key, Value),
    Client:put(Obj, Opts).
