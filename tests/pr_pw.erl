-module(pr_pw).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(TYPE_ALL, <<"ptype_all">>).
-define(TYPE_QUORUM, <<"ptype_quorum">>).

confirm() ->
    application:start(inets),
    lager:info("Deploy some nodes"),
    Nodes = rt:build_cluster(4),

    %% calculate the preflist for foo/bar
    {ok, Ring} =
        rpc:call(hd(Nodes), riak_core_ring_manager, get_my_ring, []),
    UpNodes =
        rpc:call(hd(Nodes), riak_core_node_watcher, nodes, [riak_kv]),
    N = 3,

    lager:info("Setting Custom bucket types for pr/pw test"),
    TypePropsAll = [{pr, 3}, {pw, 3}, {allow_mult, false}],
    lager:info("Create bucket type ~p, wait for propagation", [?TYPE_ALL]),
    rt:create_and_activate_bucket_type(hd(Nodes), ?TYPE_ALL, TypePropsAll),
    rt:wait_until_bucket_type_status(?TYPE_ALL, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {?TYPE_ALL, <<"bucket">>}, TypePropsAll),
    TypePropsQ = [{pr, quorum}, {pw, quorum}, {allow_mult, false}],
    lager:info("Create bucket type ~p, wait for propagation", [?TYPE_QUORUM]),
    rt:create_and_activate_bucket_type(hd(Nodes), ?TYPE_QUORUM, TypePropsQ),
    rt:wait_until_bucket_type_status(?TYPE_QUORUM, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {?TYPE_QUORUM, <<"bucket">>}, TypePropsQ),

    PreflistFun = 
        fun(B, K) ->
            H = rpc:call(hd(Nodes), riak_core_util, chash_key, [{B, K}]),
            riak_core_apl:get_apl_ann(H, N, Ring, UpNodes)
        end,
    Preflist2 = PreflistFun(<<"foo">>, <<"bar">>),

    lager:info("Beginning key search"),
    AllKey = find_key(PreflistFun, {?TYPE_ALL, <<"foo">>}, Preflist2),
    QKey = find_key(PreflistFun, {?TYPE_QUORUM, <<"foo">>}, Preflist2),

    lager:info("Preflist is ~p", [Preflist2]),
    PLNodes = [Node || {{_Index, Node}, _Status} <- Preflist2],
    lager:info("Nodes in preflist ~p", [PLNodes]),
    [SafeNode] = Nodes -- PLNodes,
    lager:info("Node not involved in this preflist ~p", [SafeNode]),
    %% connect to the only node in the preflist we won't break, to avoid
    %% random put forwarding
    {ok, C} = riak:client_connect(hd(PLNodes)),
    NodeUrl = rt:http_url(hd(PLNodes)),
    UrlFun=fun(Key, Value, Params) ->
            lists:flatten(io_lib:format("~s/riak/~s/~s~s",
                    [NodeUrl, Key, Value, Params]))
    end,

    Obj = riak_object:new(<<"foo">>, <<"bar">>, <<42:32/integer>>),
    ?assertEqual(ok,
                    riak_client:put(Obj, [{pw, all}], C)),
    ?assertMatch({ok, _},
                    riak_client:get(<<"foo">>, <<"bar">>, [{pr, all}], C)),
    ObjTBAll =
        riak_object:new(
            {?TYPE_ALL, <<"foo">>}, AllKey, <<42:32/integer>>),
    ObjTBQ =
        riak_object:new(
            {?TYPE_QUORUM, <<"foo">>}, QKey, <<42:32/integer>>),

    check_typed_bucket_ok(C, ObjTBAll, AllKey, ?TYPE_ALL),
    check_typed_bucket_ok(C, ObjTBQ, QKey, ?TYPE_QUORUM),
    

    %% check pr/pw can't be violated
    ?assertEqual({error, {pw_val_violation, evil}},
                    riak_client:put(Obj, [{pw, evil}], C)),
    ?assertEqual({error, {pr_val_violation, evil}},
                    riak_client:get(<<"foo">>, <<"bar">>, [{pr, evil}], C)),

    ?assertMatch({ok, {{_, 400, _}, _, "pr query parameter must be"++_}},
        httpc:request(get, {UrlFun(<<"foo">>, <<"bar">>, <<"?pr=evil">>), []}, [], [])),

    ?assertMatch({ok, {{_, 400, _}, _, "pw query parameter must be"++_}},
        httpc:request(put, {UrlFun(<<"foo">>, <<"bar">>,
                    <<"?pw=evil">>), [], "text/plain", <<42:32/integer>>}, [], [])),

    %% install an intercept to emulate a node that kernel paniced or
    %% something where it can take some time for the node_watcher to spot the
    %% downed node
    {{Index, Node}, _} = lists:last(Preflist2),
    make_intercepts_tab(Node, Index),
    rt_intercept:add(Node, {riak_kv_vnode,  [{{do_get,4}, drop_do_get},
                                                {{do_head, 4}, drop_do_head},
                                                {{do_put, 7}, drop_do_put}]}),
    lager:info("disabling do_get and do_head for index ~p on ~p", [Index, Node]),
    rt:log_to_nodes(Nodes, "disabling do_get and do_head for index ~p on ~p", [Index, Node]),
    timer:sleep(100),

    %% one vnode will never return, so we get timeouts
    ?assertEqual({error, timeout},
        riak_client:get(<<"foo">>, <<"bar">>, [{pr, all}], C)),
    ?assertEqual({error, timeout},
        riak_client:put(Obj, [{pw, all}], C)),
    
    check_typed_bucket_error(C, ObjTBAll, ?TYPE_ALL, AllKey, {error, timeout}),

    %% we can still meet quorum, though
    ?assertEqual(ok,
                    riak_client:put(Obj, [{pw, quorum}], C)),
    ?assertMatch({ok, _},
                    riak_client:get(<<"foo">>, <<"bar">>, [{pr, quorum}], C)),
    
    check_typed_bucket_ok(C, ObjTBQ, QKey, ?TYPE_QUORUM),

    rt:stop_and_wait(Node),

    %% there's now a fallback in the preflist, so PR/PW won't be satisfied
    %% anymore
    ?assertEqual({error, {pr_val_unsatisfied, 3, 2}},
                    riak_client:get(<<"foo">>, <<"bar">>, [{pr, all}], C)),
    ?assertEqual({error, {pw_val_unsatisfied, 3, 2}},
                    riak_client:put(Obj, [{pw, all}], C)),

    check_typed_bucket_error(
        C, ObjTBAll, ?TYPE_ALL, AllKey, unsatisfied),
    
    ?assertMatch({ok, {{_, 503, _}, _, "PR-value unsatisfied: 2/3\n"}},
        httpc:request(get, {UrlFun(<<"foo">>, <<"bar">>, <<"?pr=all">>), []}, [], [])),

    ?assertMatch({ok, {{_, 503, _}, _, "PW-value unsatisfied: 2/3\n"}},
        httpc:request(put, {UrlFun(<<"foo">>, <<"bar">>,
                    <<"?pw=all">>), [], "text/plain", <<42:32/integer>>}, [], [])),

    check_typed_bucket_ok(C, ObjTBQ, QKey, ?TYPE_QUORUM),

    %% emulate another node failure
    {{Index2, Node2}, _} = lists:nth(2, Preflist2),
    make_intercepts_tab(Node2, Index2),
    rt_intercept:add(Node2, {riak_kv_vnode,  [{{do_get,4}, drop_do_get},
                                                {{do_head, 4}, drop_do_head},
                                                {{do_put, 7}, drop_do_put}]}),
    lager:info("disabling do_get, do_put and do_head for index ~p on ~p", [Index2, Node2]),
    rt:log_to_nodes(Nodes, "disabling do_get, do_put and do_head for index ~p on ~p", [Index2, Node2]),
    timer:sleep(100),

    %% can't even meet quorum now
    ?assertEqual({error, timeout},
                    riak_client:get(<<"foo">>, <<"bar">>, [{pr, quorum}], C)),
    ?assertEqual({error, timeout}, 
                    riak_client:put(Obj, [{pw, quorum}], C)),

    check_typed_bucket_error(C, ObjTBQ, ?TYPE_QUORUM, QKey, {error, timeout}),

    %% restart the node
    rt:start_and_wait(Node),
    rt:wait_for_service(Node, riak_kv),

    %% we can make quorum again
    ?assertEqual(ok,
                    riak_client:put(Obj, [{pw, quorum}], C)),
    ?assertMatch({ok, _},
                    riak_client:get(<<"foo">>, <<"bar">>, [{pr, quorum}], C)),
    
    check_typed_bucket_ok(C, ObjTBQ, QKey, ?TYPE_QUORUM),

    %% intercepts still in force on second node, so we'll get timeouts
    ?assertEqual({error, timeout},
                    riak_client:get(<<"foo">>, <<"bar">>, [{pr, all}], C)),
    ?assertEqual({error, timeout},
                    riak_client:put(Obj, [{pw, all}], C)),
    
    check_typed_bucket_error(C, ObjTBAll, ?TYPE_ALL, AllKey, {error, timeout}),

    %% reboot the node
    rt:stop_and_wait(Node2),
    rt:start_and_wait(Node2),
    rt:wait_for_service(Node2, riak_kv),

    %% everything is happy again
    ?assertEqual(ok,
                    riak_client:put(Obj, [{pw, all}], C)),
    ?assertMatch({ok, _}, 
                    riak_client:get(<<"foo">>, <<"bar">>, [{pr, all}], C)),
    
    check_typed_bucket_ok(C, ObjTBAll, AllKey, ?TYPE_ALL),
    check_typed_bucket_ok(C, ObjTBQ, QKey, ?TYPE_QUORUM),

    %% make a vnode start to fail puts
    make_intercepts_tab(Node2, Index2),
    rt_intercept:add(Node2, {riak_kv_vnode,  [{{do_put, 7}, error_do_put}]}),
    lager:info("failing do_put for index ~p on ~p", [Index2, Node2]),
    rt:log_to_nodes(Nodes, "failing do_put for index ~p on ~p", [Index2, Node2]),
    timer:sleep(100),

    %% there's now a failing vnode in the preflist, so PW/DW won't be satisfied
    %% anymore
    ?assertEqual({error, {pw_val_unsatisfied, 3, 2}},
                    riak_client:put(Obj, [{pw, all}], C)),
    ?assertEqual({error, {dw_val_unsatisfied, 3, 2}},
                    riak_client:put(Obj, [{dw, all}], C)),

    ?assertMatch({ok, {{_, 503, _}, _, "PW-value unsatisfied: 2/3\n"}},
        httpc:request(put, {UrlFun(<<"foo">>, <<"bar">>,
                    <<"?pw=all">>), [], "text/plain", <<42:32/integer>>}, [], [])),
    ?assertMatch({ok, {{_, 503, _}, _, "DW-value unsatisfied: 2/3\n"}},
        httpc:request(put, {UrlFun(<<"foo">>, <<"bar">>,
                    <<"?dw=all">>), [], "text/plain", <<42:32/integer>>}, [], [])),
    pass.

make_intercepts_tab(Node, Partition) ->
    SupPid = rpc:call(Node, erlang, whereis, [sasl_safe_sup]),
    intercepts_tab = rpc:call(Node, ets, new, [intercepts_tab, [named_table,
                public, set, {heir, SupPid, {}}]]),
    true = rpc:call(Node, ets, insert, [intercepts_tab, {drop_do_get_partitions,
                [Partition]}]),
    true = rpc:call(Node, ets, insert, [intercepts_tab, {drop_do_head_partitions,
                [Partition]}]),
    true = rpc:call(Node, ets, insert, [intercepts_tab, {drop_do_put_partitions,
                [Partition]}]).

check_typed_bucket_ok(C, Obj, K, Type) ->
    ?assertEqual(
        ok, riak_client:put(Obj, [], C)),
    ?assertMatch(
        {ok, _}, riak_client:get({Type, <<"foo">>}, K, C)).

check_typed_bucket_error(C, Obj, Type, K, ExpectedError) ->
    {GetError, PutError} =
        case ExpectedError of
            unsatisfied ->
                {{error,{pr_val_unsatisfied,3,2}},
                    {error,{pw_val_unsatisfied,3,2}}};
            _ ->
                {ExpectedError, ExpectedError}
        end,

    ?assertEqual(
        PutError, riak_client:put(Obj, [], C)),
    ?assertMatch(
        GetError, riak_client:get({Type, <<"foo">>}, K, C)).

find_key(PreflistFun, Bucket, TargetPreflist) ->
    TestK =
        list_to_binary(
            lists:flatten(
                io_lib:format("K~8..0B", [leveled_rand:uniform(1000000)]))),
    case PreflistFun(Bucket, TestK) of
        TargetPreflist ->
            lager:info(
                "Found key ~p with preflist ~p for Bucket ~p",
                [TestK, TargetPreflist, Bucket]),
            TestK;
        _ ->
            find_key(PreflistFun, Bucket, TargetPreflist)
    end.
