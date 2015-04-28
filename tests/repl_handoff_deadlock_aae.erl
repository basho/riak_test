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
%%% riak_test for timing deadlock on riak_repl2_keylist_server:diff_bloom and riak_repl_aae_source:finish_sending_differences
%%% which can occur when the owner VNode is handed off after the original monitor (MonRef) is created. Neither function
%%% handles the {'DOWN', MonRef, process, VNodePid, normal} case (which happens when the node exits after handoff.
%%% Note that this uses code from verify_counter_repl as its tests, as that seemed to be able to evoke the issue, even without intercepts
%%%
%%% Also tests fixes for riak_repl2_fscoordinator which used to cache the owner of an index at replication start, but those could change with handoff.
%%% @end
-module(repl_handoff_deadlock_aae).
-behavior(riak_test).
-export([confirm/0]).

confirm() ->
    repl_handoff_deadlock_common:confirm(aae).