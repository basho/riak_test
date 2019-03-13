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
-module(verify_vclock_zlib).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% We've got a separate test for capability negotiation and other mechanisms, so the test here is fairly
%% straightforward: get a list of different versions of nodes and join them into a cluster, making sure that
%% each time our data has been replicated:
confirm() ->
    NTestItems    = 10,                                     %% How many test items to write/verify?
    TestMode      = false,                                  %% Set to false for "production tests", true if too slow.
   
    verify_vclock:run_test(TestMode, NTestItems, encode_zlib),

    lager:info("Test verify_vclock passed."),
    pass.
