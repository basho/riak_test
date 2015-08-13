-module(verify_membackend).
%% -export([confirm/0]).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"ttl_test">>).

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
    {0, _} = get_used_space(Pid),

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
    
    {MemBaseline, PutSize, Key} = put_until_changed(Pid, Node, 1000),

    {ok, C} = riak:client_connect(Node),

    ok = C:delete(?BUCKET, <<Key:32/integer>>),
    
    timer:sleep(timer:seconds(5)),

    {Mem, _PutSize} = get_used_space(Pid),

    %% The size of the encoded Riak Object varies a bit based on
    %% the included metadata, so we consult the riak_kv_memory_backend
    %% to determine the actual size of the object written
    case (MemBaseline - Mem) =:= PutSize of
        true ->
            ok;
        false ->
            ?assertEqual(MemBaseline, fail)
    end,
    ok.

check_put_consistent(Node) ->
    lager:info("checking that used mem doesn't change on re-put"),
    Pid = get_remote_vnode_pid(Node),
    
    {MemBaseline, _PutSize, Key} = put_until_changed(Pid, Node, 1000),

    {ok, C} = riak:client_connect(Node),

    %% Write a slightly larger object than before
    ok = C:put(riak_object:new(?BUCKET, <<Key:32/integer>>, <<0:8192>>)),
    
    {ok, _} = C:get(?BUCKET, <<Key:32/integer>>),

    timer:sleep(timer:seconds(2)),

    {Mem, _} = get_used_space(Pid),

    case abs(Mem - MemBaseline) < 3 of
        true -> ok;
        false -> ?assertEqual(consistency_failure,
                              {Mem, MemBaseline})
    end,
    ok.

%% @doc Keep putting objects until the memory used changes
%% Also return the size of the object after it's been encoded
%% and send to the memory backend.
-spec(put_until_changed(pid(), node(), term()) -> {integer(), integer(), term()}).
put_until_changed(Pid, Node, Key) ->
    {ok, C} = riak:client_connect(Node),
    {UsedSpace, _} = get_used_space(Pid),

    C:put(riak_object:new(?BUCKET, <<Key:32/integer>>, <<0:8192>>)),

    timer:sleep(100),

    {UsedSpace1, PutObjSize} = get_used_space(Pid),
    case UsedSpace < UsedSpace1 of
        true ->
            {UsedSpace1, PutObjSize, Key};
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

%% @doc Interrogate the VNode to determine the memory used
-spec(get_used_space(pid()) -> {integer(), integer()}).
get_used_space(VNodePid) ->
    {_Mod, State} = riak_core_vnode:get_modstate(VNodePid),
    {Mod, ModState} = riak_kv_vnode:get_modstate(State),
    Status = case Mod of
        riak_kv_memory_backend ->
            riak_kv_memory_backend:status(ModState);
        riak_kv_multi_backend ->
            [{_Name, Stat}] = riak_kv_multi_backend:status(ModState),
            Stat;
        _Else ->
            lager:error("didn't understand backend ~p", [Mod]),
            throw(boom)
    end,
    Mem = proplists:get_value(used_memory, Status),
    PutObjSize = proplists:get_value(put_obj_size, Status),
    lager:info("got ~p used memory, ~p put object size", [Mem, PutObjSize]),
    {Mem, PutObjSize}.
