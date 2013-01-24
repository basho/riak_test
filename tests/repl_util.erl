-module(repl_util).
-export([make_cluster/1,
         log_to_nodes/2,
         log_to_nodes/3]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

make_cluster(Nodes) ->
    [First|Rest] = Nodes,
    [join(Node, First) || Node <- Rest],
    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)).

log_to_nodes(Nodes, Fmt) ->
    log_to_nodes(Nodes, Fmt, []).

log_to_nodes(Nodes, LFmt, LArgs) ->
    Module = lager,
    Function = log,
    Meta = [],
    Args = case LArgs of
               [] -> [info, Meta, "---riak_test--- " ++ LFmt];
               _  -> [info, Meta, "---riak_test--- " ++ LFmt, LArgs]
           end,
    [rpc:call(Node, Module, Function, Args) || Node <- Nodes].
