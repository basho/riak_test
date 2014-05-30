%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% -------------------------------------------------------------------
-module(riak_rex).
-behaviour(riak_test).
-export([confirm/0]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%% @doc riak_test entry point
confirm() ->
    SetupData = setup(current),
    rex_test(SetupData),
    pass.

setup(Type) ->
    deploy_nodes(Type).

rex_test(Node) ->
    % validated we can get the rex pid on the node
    RexPid = riak_core_util:safe_rpc(Node, erlang, whereis, [rex]),
    ?assertEqual(node(RexPid), Node),
    % kill rex on the node
    ok = supervisor:terminate_child({kernel_sup, Node}, rex),
    ErrorTuple = riak_core_util:safe_rpc(Node, erlang, whereis, [rex]),
    ?assertEqual(ErrorTuple, {badrpc,rpc_process_down}),
    % restart rex so we can shut it down later
    supervisor:restart_child({kernel_sup, Node}, rex).

deploy_nodes(NumNodes, current) ->
    rt:deploy_nodes(NumNodes, conf());
deploy_nodes(_, mixed) ->
    Conf = conf(),
    rt:deploy_nodes([{current, Conf}, {previous, Conf}]).

deploy_nodes(Type) ->
    NumNodes = rt_config:get(num_nodes, 1),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Node = deploy_nodes(NumNodes, Type),
    lager:info("Node: ~p", [Node]),
    hd(Node).

kill_rex(Node) ->
    rpc:call(Node, erlang, exit, [whereis(rex), kill]).

conf() ->
    [
     {riak_kv,
      [
       {anti_entropy, {off, []}}
      ]
     }
    ].
