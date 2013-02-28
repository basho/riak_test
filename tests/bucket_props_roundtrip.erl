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
%% -------------------------------------------------------------------
-module(bucket_props_roundtrip).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"pbc_props_verify">>).
-define(COMMIT_HOOK, {struct, [{<<"mod">>, <<"foo">>}, {<<"fun">>, <<"bar">>}]}).
-define(CHASHFUN, {my_mod, my_chash}).
-define(LINKFUN, {modfun, my_mod, my_link_fun}).

%% TODO: rhc does not support all bucket properties, even though HTTP
%% interface is not restricted.
-define(PROPS,
        [
         {n_val, 2, 3},
         {allow_mult, true, false},
         %% {last_write_wins, true, false},
         {precommit, [?COMMIT_HOOK], []},
         {postcommit, [?COMMIT_HOOK], []},
         %% {chash_keyfun, ?CHASHFUN, {riak_core_util, chash_std_keyfun}},
         %% {linkfun, ?LINKFUN, {modfun, riak_kv_wm_link_walker, mapreduce_linkfun}},
         %% {old_vclock, 10000, 86400},
         %% {young_vclock, 0, 20},
         %% {big_vclock, 100, 50},
         %% {small_vclock, 10, 50},
         %% {pr, 2, 0},
         %% {pw, all, 0},
         %% {r, all, quorum},
         %% {w, one, quorum},
         %% {dw, 0, quorum},
         %% {rw, 1, quorum},
         %% {basic_quorum, true, false},
         %% {notfound_ok, false, true},
         %% {backend, <<"custom">>, undefined},
         {search, true, false}
         %% {repl, realtime, off}
        ]).

confirm() ->
    [Node] = Nodes = rt:build_cluster(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),

    [ check_prop_set_and_get(Node, Prop, FirstVal, SecondVal) ||
        {Prop, FirstVal, SecondVal} <- ?PROPS ],

    pass.

check_prop_set_and_get(Node, Prop, One, Two) ->
    HTTP = rt:httpc(Node),
    PBC = rt:pbc(Node),
    lager:info("HTTP set property ~p = ~p", [Prop, One]),
    http_set_property(HTTP, Prop, One),
    lager:info("PBC get property ~p should == ~p", [Prop, One]),
    ?assertEqual(One, pbc_get_property(PBC, Prop)),

    lager:info("PBC set property ~p = ~p", [Prop, Two]),
    pbc_set_property(PBC, Prop, Two),
    lager:info("HTTP get property ~p should = ~p", [Prop, Two]),
    ?assertEqual(Two, http_get_property(HTTP, Prop)),
    ok.


http_set_property(Client, Prop, Value) ->
    rhc:set_bucket(Client, ?BUCKET, [{Prop, Value}]).

http_get_property(Client, Prop) ->
    {ok, Props} = rhc:get_bucket(Client, ?BUCKET),
    case lists:keyfind(Prop, 1, Props) of
        {Prop, Value} ->
            Value;
        _ -> undefined
    end.

pbc_set_property(Client, Prop, Value) ->
    riakc_pb_socket:set_bucket(Client, ?BUCKET, [{Prop, Value}]).

pbc_get_property(Client, Prop) ->
    {ok, Props} = riakc_pb_socket:get_bucket(Client, ?BUCKET),
    case lists:keyfind(Prop, 1, Props) of
        {Prop, Value} ->
            Value;
        _ -> undefined
    end.
