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
-module(verify_vclock_encoding_upgrade).
-behavior(riak_test).
-export([confirm/0]).

confirm() ->
    lager:info("Deploying previous cluster"),
    [Prev, Current] = rt_cluster:build_cluster([previous,  current]),
    PrevClient = rt:pbc(Prev),
    CurrentClient = rt:pbc(Current),
    K = <<"key">>,
    B = <<"bucket">>,
    V = <<"value">>,
    lager:info("Putting object in previous version node"),
    riakc_pb_socket:put(PrevClient, riakc_obj:new(B, K, V)),
    lager:info("Fetching object from previous version node"),
    {ok, O} = riakc_pb_socket:get(PrevClient, B, K),
    O2 = riakc_obj:update_value(O, <<"value2">>),
    lager:info("Putting updated object in current version node"),
    ok = riakc_pb_socket:put(CurrentClient, O2),
    lager:info("Fetching again from current version node"),
    {ok, O3} = riakc_pb_socket:get(CurrentClient, B, K),
    O4 = riakc_obj:update_value(O3, <<"value2">>),
    lager:info("Putting updated object back in previous version node"),
    ok = riakc_pb_socket:put(PrevClient, O4),
    lager:info("Eso es todo amigos!"),
    pass.
