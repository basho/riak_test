%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% -------------------------------------------------------------------
-module(riak_rex).
-behaviour(riak_test).
-export([confirm/0]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

%% @doc riak_test entry point
confirm() ->
    SetupData = setup(current),
    rex_test(SetupData),
    pass.

setup(Type) ->
    deploy_node(Type).

rex_test(Node) ->
    % validated we can get the rex pid on the node
    RexPid1 = riak_core_util:safe_rpc(Node, erlang, whereis, [rex]),
    ?assertEqual(Node, node(RexPid1)),
    % kill rex on the node and check that safe_rpc works
    kill_rex(Node),
    ErrorTuple = riak_core_util:safe_rpc(Node, erlang, whereis, [rex]),
    ?assertEqual(badrpc, element(1, ErrorTuple)),
    % restart rex
    supervisor:restart_child({kernel_sup, Node}, rex),
    RexPid2 = riak_core_util:safe_rpc(Node, erlang, whereis, [rex]),
    ?assertEqual(node(RexPid2), Node).


deploy_node(NumNodes, current) ->
    rt:deploy_nodes(NumNodes, conf());
deploy_node(_, mixed) ->
    Conf = conf(),
    rt:deploy_nodes([{current, Conf}, {previous, Conf}]).

deploy_node(Type) ->
    NumNodes = rt_config:get(num_nodes, 1),

    lager:info("Deploy ~p node", [NumNodes]),
    Node = deploy_node(NumNodes, Type),
    lager:info("Node: ~p", [Node]),
    hd(Node).

kill_rex(Node) ->
    ok = supervisor:terminate_child({kernel_sup, Node}, rex).

conf() ->
    [
     {riak_kv,
      [
       {anti_entropy, {off, []}}
      ]
     }
    ].
