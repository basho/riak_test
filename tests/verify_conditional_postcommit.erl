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
-module(verify_conditional_postcommit).
-export([confirm/0, conditional_hook/3, postcommit/1]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Config = [{riak_core, [{vnode_management_timer, 1000},
                           {ring_creation_size, 4}]}],
    Nodes = rt:deploy_nodes(1, Config),
    Node = hd(Nodes),
    ok = rt:load_modules_on_nodes([?MODULE], Nodes),

    lager:info("Creating bucket types 'type1' and 'type2'"),
    rt:create_and_activate_bucket_type(Node, <<"type1">>, [{magic, false}]),
    rt:create_and_activate_bucket_type(Node, <<"type2">>, [{magic, true}]),

    lager:info("Installing conditional hook"),
    CondHook = {?MODULE, conditional_hook},
    ok = rpc:call(Node, riak_kv_hooks, add_conditional_postcommit, [CondHook]),

    Bucket1 = {<<"type1">>, <<"test">>},
    Bucket2 = {<<"type2">>, <<"test">>},
    Keys = [<<N:64/integer>> || N <- lists:seq(1,1000)],
    PBC = rt:pbc(Node),

    lager:info("Writing keys as 'type1' and verifying hook is not triggered"),
    write_keys(Node, PBC, Bucket1, Keys, false), 

    lager:info("Writing keys as 'type2' and verifying hook is triggered"),
    write_keys(Node, PBC, Bucket2, Keys, true),

    lager:info("Removing conditional hook"),
    ok = rpc:call(Node, riak_kv_hooks, del_conditional_postcommit, [CondHook]),
    lager:info("Re-writing keys as 'type2' and verifying hook is not triggered"),
    write_keys(Node, PBC, Bucket2, Keys, false),
    pass.

write_keys(Node, PBC, Bucket, Keys, ShouldHook) ->
    rpc:call(Node, application, set_env, [riak_kv, hook_count, 0]),
    [ok = rt:pbc_write(PBC, Bucket, Key, Key) || Key <- Keys],
    {ok, Count} = rpc:call(Node, application, get_env, [riak_kv, hook_count]),
    case ShouldHook of
        true ->
            ?assertEqual(length(Keys), Count);
        false ->
            ?assertEqual(0, Count)
    end,
    ok.

conditional_hook(_BucketType, _Bucket, BucketProps) ->
    case lists:member({magic, true}, BucketProps) of
        true ->
            {struct, [{<<"mod">>, atom_to_binary(?MODULE, utf8)},
                      {<<"fun">>, <<"postcommit">>}]};
        false ->
            false
    end.

postcommit(_Obj) ->
    {ok, Count} = application:get_env(riak_kv, hook_count),
    application:set_env(riak_kv, hook_count, Count + 1),
    ok.
