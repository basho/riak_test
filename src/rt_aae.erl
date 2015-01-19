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

-module(rt_aae).
-include_lib("eunit/include/eunit.hrl").

-export([wait_until_aae_trees_built/1]).

-include("rt.hrl").

wait_until_aae_trees_built(Nodes) ->
    lager:info("Wait until AAE builds all partition trees across ~p", [Nodes]),
    BuiltFun = fun() -> lists:foldl(aae_tree_built_fun(), true, Nodes) end,
    ?assertEqual(ok, rt:wait_until(BuiltFun)),
    ok.

aae_tree_built_fun() ->
    fun(Node, _AllBuilt = true) ->
            case get_aae_tree_info(Node) of
                {ok, TreeInfos} ->
                    case all_trees_have_build_times(TreeInfos) of
                        true ->
                            Partitions = [I || {I, _} <- TreeInfos],
                            all_aae_trees_built(Node, Partitions);
                        false ->
                            some_trees_not_built
                    end;
                Err ->
                    Err
            end;
       (_Node, Err) ->
            Err
    end.

% It is unlikely but possible to get a tree built time from compute_tree_info
% but an attempt to use the tree returns not_built. This is because the build
% process has finished, but the lock on the tree won't be released until it
% dies and the manager detects it. Yes, this is super freaking paranoid.
all_aae_trees_built(Node, Partitions) ->
    %% Notice that the process locking is spawned by the
    %% pmap. That's important! as it should die eventually
    %% so the lock is released and the test can lock the tree.
    IndexBuilts = rt:pmap(index_built_fun(Node), Partitions),
    BadOnes = [R || R <- IndexBuilts, R /= true],
    case BadOnes of
        [] ->
            true;
        _ ->
            BadOnes
    end.

get_aae_tree_info(Node) ->
    case rpc:call(Node, riak_kv_entropy_info, compute_tree_info, []) of
        {badrpc, _} ->
            {error, {badrpc, Node}};
        Info  ->
            lager:debug("Entropy table on node ~p : ~p", [Node, Info]),
            {ok, Info}
    end.

all_trees_have_build_times(Info) ->
    not lists:keymember(undefined, 2, Info).

index_built_fun(Node) ->
    fun(Idx) ->
            case rpc:call(Node, riak_kv_vnode,
                                     hashtree_pid, [Idx]) of
                {ok, TreePid} ->
                    case rpc:call(Node, riak_kv_index_hashtree,
                                  get_lock, [TreePid, for_riak_test]) of
                        {badrpc, _} ->
                            {error, {badrpc, Node}};
                        TreeLocked when TreeLocked == ok;
                                        TreeLocked == already_locked ->
                            true;
                        Err ->
                            % Either not_built or some unhandled result,
                            % in which case update this case please!
                            {error, {index_not_built, Node, Idx, Err}}
                    end;
                {error, _}=Err ->
                    Err;
                {badrpc, _} ->
                    {error, {badrpc, Node}}
            end
    end.
