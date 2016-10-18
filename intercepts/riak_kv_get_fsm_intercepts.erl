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
%%-------------------------------------------------------------------

-module(riak_kv_get_fsm_intercepts).
-compile(export_all).
-include("intercept.hrl").
-define(M, riak_kv_get_fsm_orig).

count_start_4(From, Bucket, Key, GetOptions) ->
    ?I_INFO("sending start/4 through proxy"),
    case ?M:start_orig(From, Bucket, Key, GetOptions) of
        {error, overload} ->
            ?I_INFO("riak_kv_get_fsm not started due to overload.");
        {ok, _} ->
            gen_server:cast({global, overload_proxy}, increment_count)
    end.

%% @doc simulate slow puts by adding delay to the prepare state.
slow_prepare(Atom, State) ->
    timer:sleep(1000),
    ?M:prepare_orig(Atom, State).
