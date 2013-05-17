-module(repair_coverage).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

fmt(Fmt, Args) ->
    lists:flatten(io_lib:format(Fmt, Args)).

test_hash({Bucket, _}) ->
    %% <<-1:160/integer>>.
    [IndexBin|_] = binary:split(Bucket, <<"_">>),
    Index = list_to_integer(binary_to_list(IndexBin)),
    DocHash = <<(Index - 1):160/integer>>,
    %% Ring = get_my_ring(['dev1@127.0.0.1']),
    %% ?assertEqual(Index, riak_core_ring:responsible_index(DocHash, Ring)),
    %% io:format("~p: ~p : ~p~n", [Bucket, Index, DocHash]),
    %% ok.
    DocHash.

zz() ->
    Owner = 'dev1@127.0.0.1',
    PL = 
        [{0,1},
         {1438665674247607560106752257205091097473808596992,2},
         {0,2},
         {1415829711164312202009819681693899175291684651008,3},
         {1438665674247607560106752257205091097473808596992,3},
         {0,3},
         {1392993748081016843912887106182707253109560705024,4},
         {1415829711164312202009819681693899175291684651008,4},
         {1438665674247607560106752257205091097473808596992,4},
         {0,4},
         {1370157784997721485815954530671515330927436759040,5},
         {1392993748081016843912887106182707253109560705024,5},
         {1415829711164312202009819681693899175291684651008,5},
         {1438665674247607560106752257205091097473808596992,5},
         {0,5}],
    Buckets = generate_buckets(Owner, PL),
    io:format("PL: ~p~n", [PL]),
    io:format("Bk: ~p~n", [Buckets]),
    [test_hash({Bucket, <<>>}) || Bucket <- Buckets],
    ok.

repair_coverage() ->
    %% [Node, Node2] = rt:deploy_nodes(2),
    %% rt:load_code(?MODULE, [Node]),
    %% rt:load_code(?MODULE, [Node2]),
    %% rt:load_code(rt, [Node, Node2]),
    %% Ring = get_my_ring([Node]),
    %% PL = responsible_preflists(0, [3], Ring),
    %% BucketsN = generate_buckets(Node, PL),
    %% Elements = 10000,
    %% [begin
    %%      rt:systest_write(Node, 1, Elements, Bucket, N)
    %%  end || {Bucket, N} <- BucketsN],
    %% rt:stop(Node),
    %% R = rt:kv_vnode_get(Node, 0, {<<"test">>, <<"ing">>}),
    %% io:format("R: ~p~n", [R]),
    %% Perform all-partition repair to stress test repair
    %% stress_repair(),
    %% rt:maybe_analyze_coverage(),
    %% Perform single partition repair, with multiple preflists
    test_repair_index(0, [2,3,4,5]),
    %% test_repair_index(0, [3]),
    %% zz(),
    ok.

test_repair_index(Index, NVals) ->
    Elements = 10000,
    Nodes = rt:build_cluster(4),
    Ring = get_my_ring(Nodes),
    Owner = riak_core_ring:index_owner(Ring, Index),
    PL = responsible_preflists(Index, NVals, Ring),
    BucketsN = generate_buckets(Owner, PL),
    %% io:format("PL: ~p~n", [PL]),
    %% io:format("Bk: ~p~n", [Buckets]),
    rt:load_code(?MODULE, Nodes),
    rt:load_code(rt, Nodes),
    %% io:format("Up: ~p~n", [rpc:call(Owner, riak_core_node_watcher, nodes, [riak_kv])]),
    %% [begin
    %%      DocIdx = test_hash({Bucket, <<>>}),
    %%      Preflist = riak_core_apl:get_apl(DocIdx, N, Ring, Nodes),
    %%      io:format("~p~n", [Preflist]),
    %%      ok
    %%  end || {Bucket, N} <- BucketsN],
    %% Owned =
    %% [begin
    %%      DocIdx = test_hash({Bucket, <<>>}),
    %%      Preflist = riak_core_apl:get_apl(DocIdx, N, Ring, Nodes),
    %%      {_, Owners} = lists:unzip(Preflist),
    %%      lists:member(Owner, Owners)
    %%  end || {Bucket, N} <- BucketsN],
    %% io:format("O: ~p~n", [Owned]),
    [begin
         %% io:format("Writing ~p / ~p~n", [Bucket, N]),
         rt:systest_write(Owner, 1, Elements, Bucket, N)
     end || {Bucket, N} <- BucketsN],
    %% Verify data exists
    %% Others = Nodes -- [Owner],
    %% [rt:stop(Node) || Node <- Others],
    %% [?assertEqual([], rt:systest_read(Owner, 1, Elements, Bucket, 1)) || {Bucket, _N} <- BucketsN],
    [?assertEqual([], rt:systest_fold(Owner, Index, 1, Elements, Bucket)) || {Bucket, _N} <- BucketsN],
    
    DataDir = rt:kv_data_dir(Owner),
    StashCmd = fmt("mv ~s ~s.stash", [DataDir, DataDir]),
    rt:stop(Owner),
    ?assertMatch({0, _}, rtdev:cmd(StashCmd)),
    rt:start(Owner),
    rt:load_code(?MODULE, [Owner]),
    rt:load_code(rt, [Owner]),
    %% [?assert([] /= rt:systest_read(Owner, 1, Elements, Bucket, 1)) || {Bucket, _N} <- BucketsN],
    [?assert([] /= rt:systest_fold(Owner, Index, 1, Elements, Bucket)) || {Bucket, _N} <- BucketsN],

    %% [rt:start(Node) || Node <- Others],
    %% rt:load_code(?MODULE, Others),
    %% [begin
    %%      lager:info("Waiting for riak_kv on ~p", [Node]),
    %%      rt:wait_for_service(Node, riak_kv)
    %%  end || Node <- Nodes],
    lager:info("Waiting for riak_kv"),
    rt:wait_for_cluster_service(Nodes, riak_kv),
    %% rpc:call(Owner, redbug, start, ["riak_kv_vnode"]),
    ?assertMatch({ok,_}, rpc:call(Owner, riak_kv_vnode, repair, [Index])),
    timer:sleep(1000),
    rpc:multicall(Nodes, ?MODULE, action, [kill_receivers]),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_ring_converged(Nodes)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),
    %% %% lager:info("Waiting for repair to finish"),
    %% %% timer:sleep(5*60000),
    wait_for_repair(Owner, Index, riak_kv),
    %% [rt:stop(Node) || Node <- Others],
    %% [?assertEqual([], rt:systest_read(Owner, 1, Elements, Bucket, 1)) || {Bucket, _N} <- BucketsN],
    [?assertEqual([], rt:systest_fold(Owner, Index, 1, Elements, Bucket)) || {Bucket, _N} <- BucketsN],

    ok.

action(kill_receivers) ->
    exit(whereis(riak_core_handoff_receiver_sup), kill_for_test);
action(kill_senders) ->
    exit(whereis(riak_core_handoff_sender_sup), kill_for_test);
action(kill_handoff_manager) ->
    exit(whereis(riak_core_handoff_manager), kill_for_test);
action(kill_vnode_manager) ->
    exit(whereis(riak_core_vnode_manager), kill_for_test).

generate_buckets(Node, IndexNs) ->
    [begin
         Bucket = list_to_binary(fmt("~b_~b", [Index, N])),
         Props = [{n_val, N},
                  {notfound_ok, false},
                  {chash_keyfun, {?MODULE, test_hash}}],
         rpc:call(Node, riak_core_bucket, set_bucket, [Bucket, Props]),
         {Bucket, N}
     end || {Index, N} <- IndexNs].

set_bucket_defaults(Node) ->
    Props = [{notfound_ok, false}],
    rpc:call(Node, riak_core_bucket, append_bucket_defaults, [Props]),
    ok.

%% hash0(_BKey) ->
%%     <<-1:160/integer>>.

responsible_preflists(Index, AllN, Ring) ->
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    lists:flatmap(fun(N) ->
                          responsible_preflists_n(RevIndices, N)
                  end, AllN).

responsible_preflists_n(RevIndices, N) ->
    {Pred, _} = lists:split(N, RevIndices),
    [{Idx, N} || Idx <- lists:reverse(Pred)].

get_my_ring([Node|_]) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    Ring.
    
stress_repair() ->
    Nodes = rt:build_cluster(4), 
    [Node1, Node2, Node3, _Node4] = Nodes,
    NewConfig = [{riak_core, [{handoff_concurrency, 1024},
                              {default_bucket_props, [{notfound_ok, false}]}]}],
    [rt:update_app_config(Node, NewConfig) || Node <- Nodes],
    %% set_bucket_defaults(Node1),
    %% rpc:call(Node1, riak_core_bucket, set_bucket,
    %%          [<<"systest">>, [{notfound_ok, false}]]),
    %% ?assertEqual(ok, rt:wait_until_ring_converged(Nodes)),

    rt:systest_write(Node1, 10000),

    DataDir = rt:kv_data_dir(Node1),
    StashCmd = fmt("mv ~s ~s.stash", [DataDir, DataDir]),
    %% io:format("~s", [StashCmd]),
    ?assertEqual([], rt:systest_read(Node1, 10000)),
    [rt:stop(Node) || Node <- [Node1, Node2, Node3]],
    ?assertMatch({0, _}, rtdev:cmd(StashCmd)),
    rt:start(Node1),
    ?assert([] /= rt:systest_read(Node1, 10000)),

    rt:start(Node2),
    rt:start(Node3),

    rt:wait_for_cluster_service(Nodes, riak_kv),
    {ok, Ring} = rpc:call(Node1, riak_core_ring_manager, get_my_ring, []),
    lists:foreach(fun async_repair_index/1,
                  riak_core_ring:all_owners(Ring)),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_ring_converged(Nodes)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),

    [wait_for_repair(Owner, Index, riak_kv)
     || {Index, Owner} <- riak_core_ring:all_owners(Ring),
        Owner == Node1],

    [rt:stop(Node) || Node <- [Node2, Node3]],
    ?assertEqual([], rt:systest_read(Node1, 10000, 1)),

    ok.

async_repair_index({Idx, Owner}) ->
    spawn(fun() ->
                  rpc:call(Owner, riak_kv_vnode, repair, [Idx])
          end).

repair_index(Node, Idx) ->
    Owner = owner_of_according_to(Idx, Node),
    rpc:call(Owner, riak_kv_vnode, repair, [Idx]),
    ok.

owner_of_according_to(Idx, Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    riak_core_ring:index_owner(Ring, Idx).

cover_modules() ->
    [riak_core_repair,
     riak_core_ring,
     riak_core_vnode,
     riak_core_handoff_manager,
     riak_core_handoff_sender,
     riak_core_handoff_receiver,
     riak_core_vnode_manager,
     %% kv
     riak_kv_vnode,
     %% search
     merge_index_backend,
     riak_search_config,
     riak_search_vnode,
     riak_search_worker,
     search,
     %% merge_index
     merge_index,
     mi_locks,
     mi_server].

wait_for_repair(Node, Index, Service) ->
    F = fun(_) ->
                repair_status(Node, Index, Service) == not_found
        end,
    ?assertEqual(ok, rt:wait_until(Node, F)),
    ok.

repair_status(Node, Index, riak_kv) ->
    rpc:call(Node, riak_kv_vnode, repair_status, [Index]);
repair_status(Node, Index, riak_search) ->
    rpc:call(Node, riak_search_vnode, repair_status, [Index]).
