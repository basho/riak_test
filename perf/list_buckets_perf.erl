%%%-------------------------------------------------------------------
%%% @author doug
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Dec 2016 9:16 AM
%%%-------------------------------------------------------------------
-module(list_buckets_perf).

%% API
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Config = [{riak_core, [{vnode_management_timer, 1000},
                           {handoff_concurrency, 100},
                           {vnode_inactivity_timeout, 1000}]}],
    [Node | _Rest] = Nodes = rt:deploy_nodes(4, Config),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    lager:info("Putting buckets/keys... this may take some time", []),
    {Buckets, _KeyVals} = put_buckets(Nodes, 100000, 1),
    lager:info("Listing buckets...", []),
    {Time, ActualBuckets} = timer:tc(fun() ->
        verify_list_buckets(Node, pbc) end),
    lager:info("Time taken: ~p", [Time]),
    ?assertEqual(lists:sort(Buckets), lists:sort(ActualBuckets)),
    pass.

put_buckets(Nodes, NumBuckets, NumItems) ->
    Buckets = [list_to_binary(["", integer_to_list(Ki)])
               || Ki <- lists:seq(0, NumBuckets - 1)],
    KeyVals = [{list_to_binary(["test_key_", integer_to_list(I)]),
                list_to_binary(["test_value_", integer_to_list(I)])}
               || I <- lists:seq(0, NumItems - 1)],
    %%Conns = lists:map(fun(Node) -> rt:pbc(Node) end, Nodes),
%%    rt:pmap(fun(Bucket) ->
%%        put_keys(Pid, Bucket, KeyVals)
%%            end,
%%        Buckets),
%%        lists:foreach(fun(Pid) ->
%%        riakc_pb_socket:stop(Pid) end,
%%        Conns),
    Pid = rt:pbc(hd(Nodes)),
    lists:foreach(fun(Bucket) ->
        put_keys(Pid, Bucket, KeyVals) end,
        Buckets),
    riakc_pb_socket:stop(Pid),
    {Buckets, KeyVals}.
put_keys(Pid, Bucket, KeyVals) ->
    lists:foreach(fun({Key, Val}) ->
        riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val), [{w, 1}])
        end,
        KeyVals).

verify_list_buckets(Node, Interface) ->
    {Pid, Mod} = case Interface of
        pbc ->
            P = rt:pbc(Node),
            M = riakc_pb_socket,
            {P, M};
        http ->
            P = rt:httpc(Node),
            M = rhc,
            {P, M}
    end,
    lager:info("Listing buckets on ~p using ~p.",
               [Node, Interface]),

    {Status, Buckets} = Mod:list_buckets(Pid),
    case Status of
        error -> lager:info("list buckets error ~p", [Buckets]);
        _ -> ok
    end,
    ?assertEqual(ok, Status),
    case Interface of
        pbc -> riakc_pb_socket:stop(Pid);
        _ -> ok
    end,
    Buckets.