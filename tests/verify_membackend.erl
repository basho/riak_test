-module(verify_membackend).
%% -export([confirm/0]).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"ttl_test">>).

%% from 2.0, but should be valid for 1.1+
-record(state, {data_ref :: ets:tid(),
                index_ref :: ets:tid(),
                time_ref :: ets:tid(),
                max_memory :: undefined | integer(),
                used_memory=0 :: integer(),
                ttl :: integer()}).

confirm() ->
    Tests = [ttl, max_memory, combo],
    [Res1, Res2] = 
        [begin
             lager:info("testing mode ~p", [Mode]),
             put(mode, Mode),
             [begin
                  lager:info("testing setting ~p", [Test]),
                  ?MODULE:Test(Mode)
              end
              || Test <- Tests]
         end
         || Mode <- [regular, multi]],
    Res = Res1 ++ Res2,
    [ok] = lists:usort(Res),
    pass.


ttl(Mode) ->
    Conf = mkconf(ttl, Mode),
    [NodeA, NodeB] = rt:deploy_nodes(2, Conf),

    ?assertEqual(ok, check_leave_and_expiry(NodeA, NodeB)),

    rt:clean_cluster([NodeA]),
    ok.

max_memory(Mode) ->
    Conf = mkconf(max_memory, Mode),
    [NodeA, NodeB] = rt:deploy_nodes(2, Conf),

    rt:join(NodeB, NodeA),

    ?assertEqual(ok, check_put_delete(NodeA)),

    ?assertEqual(ok, check_put_consistent(NodeA)),

    ?assertEqual(ok, check_eviction(NodeA)),

    rt:clean_cluster([NodeA, NodeB]),

    ok.

combo(Mode) ->
    Conf = mkconf(combo, Mode),
    
    [NodeA, NodeB] = rt:deploy_nodes(2, Conf),

    ?assertEqual(ok, check_leave_and_expiry(NodeA, NodeB)),
 
    %% Make sure that expiry is updating used_memory correctly
    Pid = get_remote_vnode_pid(NodeA),
    0 = get_used_space(Pid, NodeA),

    ?assertEqual(ok, check_put_delete(NodeA)),

    ?assertEqual(ok, check_put_consistent(NodeA)),

    ?assertEqual(ok, check_eviction(NodeA)),

    rt:clean_cluster([NodeA]),

    ok.


check_leave_and_expiry(NodeA, NodeB) ->
    ?assertEqual([], rt:systest_write(NodeB, 1, 100, ?BUCKET, 2)),
    ?assertEqual([], rt:systest_read(NodeB, 1, 100, ?BUCKET, 2)),

    rt:join(NodeB, NodeA),

    ?assertEqual(ok, rt:wait_until_nodes_ready([NodeA, NodeB])),
    rt:wait_until_no_pending_changes([NodeA, NodeB]),

    rt:leave(NodeB),
    rt:wait_until_unpingable(NodeB),

    ?assertEqual([], rt:systest_read(NodeA, 1, 100, ?BUCKET, 2)),
    
    lager:info("waiting for keys to expire"),
    timer:sleep(timer:seconds(210)),
    
    _ = rt:systest_read(NodeA, 1, 100, ?BUCKET, 2),
    timer:sleep(timer:seconds(5)),
    Res = rt:systest_read(NodeA, 1, 100, ?BUCKET, 2),
    
    ?assertEqual(100, length(Res)),
    ok.

check_eviction(Node) ->
    lager:info("checking that values are evicted when memory limit "
               "is exceeded"),
    Size = 20000 * 8,
    Val = <<0:Size>>,

    ?assertEqual([], rt:systest_write(Node, 1, 500, ?BUCKET, 2, Val)),

    Res = length(rt:systest_read(Node, 1, 100, ?BUCKET, 2, Val)),

    %% this is a wider range than I'd like but the final outcome is
    %% somewhat hard to predict.  Just trying to verify that some
    %% memory limit somewhere is being honored and that values are
    %% being evicted.
    case Res == 100 of
        true ->
            ok;
        false ->
            ?assertEqual(Res, memory_reclamation_issue)
    end,
     
    {ok, C} = riak:client_connect(Node),

    [begin
         C:delete(?BUCKET, <<N:32/integer>>),
         timer:sleep(100)
     end
     || N <- lists:seq(1, 500)],

    %% make sure all deletes propagate?
    timer:sleep(timer:seconds(10)),
    ok.
    
check_put_delete(Node) ->
    lager:info("checking that used mem is reclaimed on delete"),
    Pid = get_remote_vnode_pid(Node),
    
    {MemBaseline, Key} = put_until_changed(Pid, Node, 1000),

    {ok, C} = riak:client_connect(Node),

    ok = C:delete(?BUCKET, <<Key:32/integer>>),
    
    timer:sleep(timer:seconds(5)),

    Mem = get_used_space(Pid, Node),

    %% this is meh, but the value isn't always the same length.
    case (Mem == MemBaseline - 1142) orelse 
        (Mem == MemBaseline - 1141) of
        true ->
            ok;
        false ->
            ?assertEqual(MemBaseline, fail)
    end,
    ok.

check_put_consistent(Node) ->
    lager:info("checking that used mem doesn't change on re-put"),
    Pid = get_remote_vnode_pid(Node),
    
    {MemBaseline, Key} = put_until_changed(Pid, Node, 1000),

    {ok, C} = riak:client_connect(Node),

    ok = C:put(riak_object:new(?BUCKET, <<Key:32/integer>>, <<0:8192>>)),
    
    {ok, _} = C:get(?BUCKET, <<Key:32/integer>>),

    timer:sleep(timer:seconds(2)),

    Mem = get_used_space(Pid, Node),

    case abs(Mem - MemBaseline) < 3 of
        true -> ok;
        false -> ?assertEqual(consistency_failure,
                              {Mem, MemBaseline})
    end,
    ok.

put_until_changed(Pid, Node, Key) ->
    {ok, C} = riak:client_connect(Node),
    UsedSpace = get_used_space(Pid, Node),

    C:put(riak_object:new(?BUCKET, <<Key:32/integer>>, <<0:8192>>)),
    
    timer:sleep(100),

    UsedSpace1 = get_used_space(Pid, Node),
    case UsedSpace < UsedSpace1 of
        true ->
            {UsedSpace1, Key};
        false ->
            put_until_changed(Pid, Node, Key+1)
    end.

mkconf(Test, Mode) ->
    MembConfig = 
        case Test of
            ttl ->
                [{ttl, 200}];
            max_memory ->
                [{max_memory, 1}];
            combo -> 
                [{max_memory, 1}, 
                 {ttl, 200}]
        end,
    case Mode of
        regular ->
            %% only memory supports TTL
            rt:set_backend(memory),

            [
             {riak_core, [
                          {ring_creation_size, 4}
                         ]},
             {riak_kv, [
                        {anti_entropy, {off, []}},
                        {delete_mode, immediate},
                        {memory_backend, MembConfig}
                       ]}
            ];
        multi ->
            rt:set_backend(multi),
            [
             {riak_core, [
                          {ring_creation_size, 4}
                         ]},
             {riak_kv, [
                        {anti_entropy, {off, []}},
                        {delete_mode, immediate},
                        {multi_backend_default, <<"memb">>},
                        {multi_backend, 
                         [
                          {<<"memb">>, riak_kv_memory_backend,
                           MembConfig}
                         ]
                        }
                       ]
             }
            ]
    end.

get_remote_vnode_pid(Node) ->
    [{_,_,VNode}|_] = rpc:call(Node, riak_core_vnode_manager, 
                               all_vnodes, [riak_kv_vnode]),
    VNode.

%% this is silly fragile
get_used_space(VNode, Node) ->
    S = rpc:call(Node, sys, get_state, [VNode]),
    Mode = get(mode),
    Version = rt:get_version(),
    %% lager:info("version mode ~p", [{Version, Mode}]),
    TwoOhReg =
        fun(X) -> 
                element(4, element(4, element(2, X)))
        end,
    TwoOhMulti =
        fun(X) -> 
                element(
                  3, lists:nth(
                       1, element(
                            2, element(
                                 4, element(
                                      4, element(2, X))))))
        end,
    Extract = 
        case {Version, Mode} of
            {<<"riak-2.0",_/binary>>, regular} ->
                TwoOhReg;
            {<<"riak_ee-2.0",_/binary>>, regular} ->
                TwoOhReg;
            {<<"riak-2.0",_/binary>>, multi} ->
                TwoOhMulti;
            {<<"riak_ee-2.0",_/binary>>, multi} ->
                TwoOhMulti;
            _Else ->
                lager:error("didn't understand version/mode tuple ~p",
                            [{Version, Mode}]),
                throw(boom)
        end,
    State = Extract(S),
    Mem = State#state.used_memory,
    lager:info("got ~p used memory", [Mem]),
    Mem.
