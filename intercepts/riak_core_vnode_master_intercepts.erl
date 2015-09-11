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

-module(riak_core_vnode_master_intercepts).
-compile(export_all).
-include("intercept.hrl").

-record(riak_core_fold_req_v2, {
    foldfun :: fun(),
    acc0 :: term(),
    forwardable :: boolean(),
    opts = [] :: list()}).

-define(M, riak_core_vnode_master_orig).


stop_vnode_after_bloom_fold_request_succeeds(IndexNode, Req, Sender, VMaster) ->
    ?I_INFO("Intercepting riak_core_vnode_master:command_returning_vnode"),
    ReqFun = Req#riak_core_fold_req_v2.foldfun,

    case (ReqFun == fun riak_repl_aae_source:bloom_fold/3 orelse ReqFun == fun riak_repl_keylist_server:bloom_fold/3) of
        true ->
            random:seed(erlang:now()),
            case random:uniform(10) of
                5 ->
                    %% Simulate what happens when a VNode completes handoff between command_returning_vnode
                    %% and the fold attempting to start - other attempts to intercept and slow
                    %% certain parts of Riak to invoke the particular race condition were unsuccessful
                    ?I_INFO("Replaced VNode with spawned function in command_returning_vnode"),
                    VNodePid = spawn(fun() -> timer:sleep(100),
                                   exit(normal)
                          end),
                    {ok, VNodePid};
                _ ->
                    ?M:command_return_vnode_orig(IndexNode, Req, Sender, VMaster)
            end;
        false -> ?M:command_return_vnode_orig(IndexNode, Req, Sender, VMaster)
    end.