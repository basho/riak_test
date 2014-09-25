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
%%
%% Test to verify that delete modes supplied by riak_client or
%% set in bucket properties make it all the way down to the vnode.
%%
%% Creates a bucket 'fromclient' an an object for each delete mode
%% and checks that they are treated correctly by setting, then
%% does the same for a bucket with each delete mode set as a property.
%%
%% -------------------------------------------------------------------
-module(verify_riak_client_delete_mode).
-behavior(riak_test).
-export([confirm/0]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->

    %% Deploy a node with delete mode keep so that we can test the effect of
    %% each mode option
    BaseConf = [
                {riak_kv,
                 [
                  {delete_mode, keep}
                 ]}
               ],

    Nodes = rt:deploy_nodes(1, BaseConf),
    [Node1] = Nodes,

    %% Load this module on all the nodes so we can call it
    ok = rt:load_modules_on_nodes([?MODULE], Nodes),

    %% Create objects to be removed and verify the test sees them
    lager:info("Creating and checking test fixtures"),
    create_obj(Node1, <<"fromclient">>, <<"keep">>, <<"keep">>),
    create_obj(Node1, <<"fromclient">>, <<"immediate">>, <<"immediate">>),
    create_obj(Node1, <<"fromclient">>, <<"after500">>, <<"after500">>),

    create_obj(Node1, <<"keepbucket">>, <<"keep">>, <<"keep">>),
    create_obj(Node1, <<"immediatebucket">>, <<"immediate">>, <<"immediate">>),
    create_obj(Node1, <<"afterbucket">>, <<"after500">>, <<"after500">>),

    ?assertEqual(<<"keep">>, vnode_get_value(Node1, <<"keepbucket">>, <<"keep">>)),
    ?assertEqual(<<"immediate">>, vnode_get_value(Node1, <<"immediatebucket">>, <<"immediate">>)),
    ?assertEqual(<<"after500">>, vnode_get_value(Node1, <<"afterbucket">>, <<"after500">>)),

    ?assertEqual({error, notfound}, vnode_get_value(Node1, <<"fromclient">>, <<"notakey">>)),
    ?assertEqual(<<"keep">>, vnode_get_value(Node1, <<"keepbucket">>, <<"keep">>)),
    ?assertEqual(<<"immediate">>, vnode_get_value(Node1, <<"immediatebucket">>, <<"immediate">>)),
    ?assertEqual(<<"after500">>, vnode_get_value(Node1, <<"afterbucket">>, <<"after500">>)),

    %% Check that an immediate delete is applied with no wait
    lager:info("Checking delete modes passed to client"),
    ?assertEqual({error, notfound}, verify_delete(Node1, <<"fromclient">>, <<"immediate">>, immediate)),
    lager:info("...checked immediate"),

    %% Check that keep is still kept (does overlap with vnode delete mode, but
    %% assume mechanism works).
    ?assertEqual(<<>>, verify_delete(Node1, <<"fromclient">>, <<"keep">>, keep)),
    lager:info("...checked keep"),

    %% Check that keep is still kept (does overlap with vnode delete mode, but
    %% assume mechanism works).
    ?assertEqual(<<>>, verify_delete(Node1, <<"fromclient">>, <<"after500">>, 500)),
    timer:sleep(600), % wait for the delete time and a little extra
    ?assertEqual({error, notfound}, vnode_get_value(Node1, <<"fromclient">>, <<"after500">>)),
    lager:info("...checked delayed"),

    %%
    %% Now test for bucket properties
    %%
    lager:info("Checking delete modes set in bucket"),

    ok = rpc:call(Node1, riak_core_bucket, set_bucket, [<<"keepbucket">>,
                                                        [{delete_mode, keep}]]),
    ok = rpc:call(Node1, riak_core_bucket, set_bucket, [<<"immediatebucket">>,
                                                        [{delete_mode, immediate}]]),
    ok = rpc:call(Node1, riak_core_bucket, set_bucket, [<<"afterbucket">>, [{delete_mode, 500}]]),

    %% Check that an immediate delete is applied with no wait
    lager:info("Checking delete modes passed to client"),
    ?assertEqual({error, notfound}, verify_delete(Node1, <<"immediatebucket">>,
                                                  <<"immediate">>, immediate)),
    lager:info("...checked immediate"),

    %% Check that keep is still kept (does overlap with vnode delete mode, but
    %% assume mechanism works).
    ?assertEqual(<<>>, verify_delete(Node1, <<"keepbucket">>, <<"keep">>, keep)),
    lager:info("...checked keep"),

    %% Check that keep is still kept (does overlap with vnode delete mode, but
    %% assume mechanism works).
    ?assertEqual(<<>>, verify_delete(Node1, <<"afterbucket">>, <<"after500">>, 500)),
    timer:sleep(600), % wait for the delete time and a little extra
    ?assertEqual({error, notfound}, vnode_get_value(Node1, <<"afterbucket">>, <<"after500">>)),
    lager:info("...checked delayed"),


    lager:info("Test verify_riak_client_delete_mode: Passed"),
    pass.

verify_delete(Node, Bucket, Key, Mode) ->
    %% Delete - triggers async get, so do not know if object has been reaped
    delete_with_mode(Node, Bucket, Key, Mode),

    %% wait until there are no delete processes running and 
    %% only then check the contents of the first vnode.
    ok = wait_until_no_delete_procs(Node),
    vnode_get_value(Node, Bucket, Key).

create_obj(Node, Bucket, Key, Val) ->
    rpc:call(Node, ?MODULE, do_create_obj, [Bucket, Key, Val]).

do_create_obj(Bucket, Key, Val) ->
    {ok, C} = riak:local_client(),
    riak_client:put(riak_object:new(Bucket, Key, Val), [{dw,all}], C).

delete_with_mode(Node, Bucket, Key, Mode) ->
    delete(Node, Bucket, Key, [{delete_mode, Mode}]).

%% calls get/2 - namespace clash with process dictionary
get_obj(Node, Bucket, Key) ->
    rpc:call(Node, ?MODULE, do_get, [Bucket, Key]).

do_get_obj(Bucket, Key) ->
    {ok, C} = riak:local_client(),
    riak_client:get(Bucket, Key, [{r,all}], C).

%% delete/3 with options
delete(Node, Bucket, Key, Options) ->
    rpc:call(Node, ?MODULE, do_delete, [Bucket, Key, Options]).

%% delete/2 without options
delete(Node, Bucket, Key) ->
    rpc:call(Node, ?MODULE, do_delete, [Bucket, Key]).

%% Server side delete functions
do_delete(Bucket, Key) ->
    {ok, C} = riak:local_client(),
    riak_client:delete(Bucket, Key, C).

do_delete(Bucket, Key, Options) ->
    {ok, C} = riak:local_client(),
    riak_client:delete(Bucket, Key, Options, C).

riak_client_req(Node, Func, Args) ->
    rpc:call(Node, riak_client, Func, Args ++ [id(Node)]).

vnode_get_value(Node, Bucket, Key) ->
     rpc:call(Node, ?MODULE, do_vnode_get_value, [Bucket, Key]).

do_vnode_get_value(Bucket, Key) ->
    {Index, _N} = riak_kv_util:get_index_n({Bucket, Key}),
    case riak_kv_vnode:local_get(Index, {Bucket, Key}) of
        {error, _Response} = ER ->
            ER;
        {ok, RObj} ->
            riak_object:get_value(RObj)
    end.

wait_until_no_delete_procs(Node) ->
    rpc:call(Node, ?MODULE, do_wait_until_no_delete_procs, []).

%% Runs riak-side - checks until active get FSM count is zero
do_wait_until_no_delete_procs() ->
    Running = lists:foldl(
                fun(Pid, Acc) ->
                        try
                            {dictionary, Dict} = process_info(Pid, dictionary),
                            case [x || {'$initial_call',{riak_kv_delete,delete,_Arity}} <- Dict] of
                                [] ->
                                    Acc;
                                _ ->
                                Acc + 1
                            end
                        catch _:_ -> % if not able to get dict, prolly dead
                                Acc
                        end
                end, 0, processes()),
    case Running of
        0 ->
            ok;
        _ ->
            lager:info("Waiting for ~p get FSMs to complete\n", [Running]),
            do_wait_until_no_delete_procs()
    end.


%% ClientID suitable to pass to riak_client
id(Node) ->
    {Node, undefined}.

some_fun() ->
    Bucket = <<"b">>,
    Key = <<"k">>,
    term_to_binary(fun() ->
                       {Index, _N} = riak_kv_util:get_index_n({Bucket, Key}),
                       case riak_kv_vnode:local_get(Index, {Bucket, Key}) of
                           {error, _Response} = ER ->
                               ER;
                           RObj ->
                               riak_object:get_value(RObj)
                       end
               end).
