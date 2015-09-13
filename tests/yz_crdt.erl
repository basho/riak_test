-module(yz_crdt).

-compile(export_all).
-compile({parse_transform, rt_intercept_pt}).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).
-define(INDEX, <<"maps">>).
-define(TYPE, <<"maps">>).
-define(KEY, "Chris Meiklejohn").
-define(BUCKET, {?TYPE, <<"testbucket">>}).
-define(GET(K,L), proplists:get_value(K, L)).
-define(N, 3).

-define(CONF,
        [
         {riak_core,
          [{ring_creation_size, 8}]
         },
         {riak_kv,
          [{delete_mode, keep},
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 8},
           {anti_entropy_tick, 1000}]},
         {yokozuna,
          [{enabled, true}]
         }]).

confirm() ->
    rt:set_advanced_conf(all, ?CONF),

    %% Configure cluster.
    Nodes = rt:build_cluster(5, ?CONF),

    Node = rt:select_random(Nodes),

    %% Create PB connection.
    Pid = rt:pbc(Node),
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    %% Create index.
    riakc_pb_socket:create_search_index(Pid, ?INDEX, <<"_yz_default">>, []),

    %% Create bucket type for maps.
    rt:create_and_activate_bucket_type(Node,
                                       ?TYPE,
                                       [{datatype, map},
                                        {n_val, ?N},
                                        {search_index, ?INDEX}]),

    %% Write some sample data.

    Map1 = riakc_map:update(
            {<<"name">>, register},
            fun(R) ->
                    riakc_register:set(list_to_binary(?KEY), R)
            end, riakc_map:new()),
    Map2 = riakc_map:update(
             {<<"interests">>, set},
             fun(S) ->
                     riakc_set:add_element(<<"thing">>, S) end,
             Map1),
    ok = riakc_pb_socket:update_type(
           Pid,
           ?BUCKET,
           ?KEY,
           riakc_map:to_op(Map2)),


    yokozuna_rt:drain_solrqs(Nodes),
    yokozuna_rt:commit(Nodes, ?INDEX),

    %% Wait for yokozuna index to trigger.
    yokozuna_rt:commit(Nodes, ?INDEX),

    %% Perform simple queries, check for register, set fields.
    ok = rt:wait_until(
           fun() ->
               validate_search_results(Pid)
           end),
    %% Stop PB connection.
    riakc_pb_socket:stop(Pid),

    %% Clean cluster.
    rt:clean_cluster(Nodes),

    pass.

validate_search_results(Pid) ->
    try
        {ok, {search_results, Results1a, _, _}} = riakc_pb_socket:search(
            Pid, ?INDEX, <<"name_register:Chris*">>),
        lager:info("Search name_register:Chris*: ~p~n", [Results1a]),
        ?assertEqual(length(Results1a), 1),
        ?assertEqual(?GET(<<"name_register">>, ?GET(?INDEX, Results1a)),
                     list_to_binary(?KEY)),
        ?assertEqual(?GET(<<"interests_set">>, ?GET(?INDEX, Results1a)),
                     <<"thing">>),

        {ok, {search_results, Results2a, _, _}} = riakc_pb_socket:search(
            Pid, ?INDEX, <<"interests_set:thing*">>),
        lager:info("Search interests_set:thing*: ~p~n", [Results2a]),
        ?assertEqual(length(Results2a), 1),
        ?assertEqual(?GET(<<"name_register">>, ?GET(?INDEX, Results2a)),
                     list_to_binary(?KEY)),
        ?assertEqual(?GET(<<"interests_set">>, ?GET(?INDEX, Results2a)),
                     <<"thing">>),

        {ok, {search_results, Results3a, _, _}} = riakc_pb_socket:search(
            Pid, ?INDEX, <<"_yz_rb:testbucket">>),
        lager:info("Search testbucket: ~p~n", [Results3a]),
        ?assertEqual(length(Results3a), 1),
        ?assertEqual(?GET(<<"name_register">>, ?GET(?INDEX, Results3a)),
                     list_to_binary(?KEY)),
        ?assertEqual(?GET(<<"interests_set">>, ?GET(?INDEX, Results3a)),
                     <<"thing">>),

        %% Redo queries and check if results are equal
        {ok, {search_results, Results1b, _, _}} = riakc_pb_socket:search(
            Pid, ?INDEX, <<"name_register:Chris*">>),
        ?assertEqual(number_of_fields(Results1a, ?INDEX),
                     number_of_fields(Results1b, ?INDEX)),

        {ok, {search_results, Results2b, _, _}} = riakc_pb_socket:search(
            Pid, ?INDEX, <<"interests_set:thing*">>),
        ?assertEqual(number_of_fields(Results2a, ?INDEX),
                     number_of_fields(Results2b, ?INDEX)),

        {ok, {search_results, Results3b, _, _}} = riakc_pb_socket:search(
            Pid, ?INDEX, <<"_yz_rb:testbucket">>),
        ?assertEqual(number_of_fields(Results3a, ?INDEX),
                     number_of_fields(Results3b, ?INDEX)),
        true
    catch Err:Reason ->
        lager:info("Waiting for CRDT search results to converge. Error was ~p.", [{Err, Reason}]),
        false
    end,

    test_repeat_sets(Pid, Nodes, ?BUCKET, ?INDEX, ?KEY),
    test_delete(Pid, Nodes, ?BUCKET, ?INDEX, ?KEY),
    test_delete_aae(Pid, Nodes, ?BUCKET, ?INDEX),

    pass.

test_repeat_sets(Pid, Cluster, Bucket, Index, Key) ->
    {ok, M1} = riakc_pb_socket:fetch_type(Pid, Bucket, Key),
    M2 = riakc_map:update(
           {<<"update">>, register},
           fun(R) ->
                   riakc_register:set(<<"foo">>, R)
           end, M1),
    ok = riakc_pb_socket:update_type(
           Pid,
           Bucket,
           Key,
           riakc_map:to_op(M2)),
    M3 = riakc_map:update(
           {<<"update">>, register},
           fun(R) ->
                   riakc_register:set(<<"bar">>, R)
           end, M1),
    ok = riakc_pb_socket:update_type(
           Pid,
           Bucket,
           Key,
           riakc_map:to_op(M3)),

    yokozuna_rt:commit(Cluster, Index),

    {ok, {search_results, Results, _, _}} = riakc_pb_socket:search(
                                              Pid, Index,
                                              <<"update_register:*">>),

    lager:info("Search update_register:*: ~p~n", [Results]),
    ?assertEqual(1, length(Results)).

test_delete(Pid, Cluster, Bucket, Index, Key) ->
    {ok, M1} = riakc_pb_socket:fetch_type(Pid, Bucket, Key),
    M2 = riakc_map:erase({<<"name">>, register}, M1),
    M3 = riakc_map:update(
           {<<"interests">>, set},
           fun(S) ->
                   riakc_set:del_element(<<"thing">>,
                   riakc_set:add_element(<<"roses">>, S))
           end, M2),

    ok = riakc_pb_socket:update_type(
           Pid,
           Bucket,
           Key,
           riakc_map:to_op(M3)),

    yokozuna_rt:commit(Cluster, Index),

    {ok, {search_results, Results1, _, _}} = riakc_pb_socket:search(
                                              Pid, Index,
                                              <<"name_register:*">>),
    lager:info("Search deleted/erased name_register:*: ~p~n", [Results1]),
    ?assertEqual(0, length(Results1)),

    M4 = riakc_map:update(
           {<<"interests">>, set},
           fun(S) ->
               riakc_set:add_element(<<"pans">>, S)
           end, M3),

    ok = riakc_pb_socket:update_type(
           Pid,
           Bucket,
           Key,
           riakc_map:to_op(M4)),

    yokozuna_rt:commit(Cluster, Index),

    {ok, {search_results, Results2, _, _}} = riakc_pb_socket:search(
                                               Pid, Index,
                                               <<"interests_set:thing*">>),
    lager:info("Search deleted interests_set:thing*: ~p~n", [Results2]),
    ?assertEqual(0, length(Results2)),

    lager:info("Delete key for map"),
    ?assertEqual(ok, riakc_pb_socket:delete(Pid, Bucket, Key)),
    yokozuna_rt:commit(Cluster, Index),
    ?assertEqual({error, {notfound, map}}, riakc_pb_socket:fetch_type(Pid, Bucket, Key)),

    {ok, {search_results, Results3, _, _}} = riakc_pb_socket:search(
                                               Pid, Index,
                                               <<"*:*">>),
    lager:info("Search deleted map *:*: ~p~n", [Results3]),
    ?assertEqual(0, length(Results3)),

    lager:info("Recreate object and check counts"),

    M5 = riakc_map:update(
            {<<"name">>, register},
            fun(R) ->
                    riakc_register:set(<<"hello">>, R)
            end, riakc_map:new()),

    ok = riakc_pb_socket:update_type(
           Pid,
           Bucket,
           Key,
           riakc_map:to_op(M5)),

    yokozuna_rt:commit(Cluster, Index),

    {ok, M6} = riakc_pb_socket:fetch_type(Pid, Bucket, Key),
    Keys = riakc_map:fetch_keys(M6),
    ?assertEqual(1, length(Keys)),
    ?assert(riakc_map:is_key({<<"name">>, register}, M6)),
    {ok, {search_results, Results4, _, _}} = riakc_pb_socket:search(
                                               Pid, Index,
                                               <<"*:*">>),
    lager:info("Search recreated map *:*: ~p~n", [Results4]),
    ?assertEqual(1, length(Results4)),

    lager:info("Delete key for map again"),
    ?assertEqual(ok, riakc_pb_socket:delete(Pid, Bucket, Key)),
    yokozuna_rt:commit(Cluster, Index),

    ?assertEqual({error, {notfound, map}}, riakc_pb_socket:fetch_type(Pid, Bucket, Key)),

    {ok, {search_results, Results5, _, _}} = riakc_pb_socket:search(
                                               Pid, Index,
                                               <<"*:*">>),
    lager:info("Search ~p deleted map *:*: ~p~n", [Key, Results5]),
    ?assertEqual(0, length(Results5)).

test_delete_aae(Pid, Cluster, Bucket, Index) ->
    Key1 = <<"ohyokozuna">>,
    M1 = riakc_map:update(
           {<<"name">>, register},
           fun(R) ->
                   riakc_register:set(<<"jokes are">>, R)
           end, riakc_map:new()),
    ok = riakc_pb_socket:update_type(
           Pid,
           Bucket,
           Key1,
           riakc_map:to_op(M1)),

    Key2 = <<"ohriaksearch">>,
    M2 = riakc_map:update(
             {<<"name">>, register},
             fun(R) ->
                     riakc_register:set(<<"better explained">>, R)
             end, riakc_map:new()),
    ok = riakc_pb_socket:update_type(
           Pid,
           Bucket,
           Key2,
           riakc_map:to_op(M2)),

    [make_intercepts_tab(ANode) || ANode <- Cluster],

    [rt_intercept:add(ANode, {yz_kv, [{{delete_operation, 6},
                                       handle_delete_operation}]})
     || ANode <- Cluster],
    [true = rpc:call(ANode, ets, insert, [intercepts_tab, {del_put, 0}]) ||
        ANode <- Cluster],
    [rt_intercept:wait_until_loaded(ANode) || ANode <- Cluster],

    lager:info("Delete key ~p for map", [Key2]),
    ?assertEqual(ok, riakc_pb_socket:delete(Pid, Bucket, Key2)),
    ?assertEqual({error, {notfound, map}}, riakc_pb_socket:fetch_type(Pid, Bucket, Key2)),
    yokozuna_rt:commit(Cluster, Index),

    {ok, {search_results, Results1, _, _}} = riakc_pb_socket:search(
                                               Pid, Index,
                                               <<"*:*">>),
    lager:info("Search all results, expect extra b/c tombstone ... *:*: ~p~n",
               [length(Results1)]),
    ?assertEqual(2, length(Results1)),

    lager:info("Expire and re-check"),
    yokozuna_rt:expire_trees(Cluster),
    yokozuna_rt:wait_for_full_exchange_round(Cluster, erlang:now()),

    yokozuna_rt:commit(Cluster, Index),

    {ok, {search_results, Results2, _, _}} = riakc_pb_socket:search(
                                               Pid, Index,
                                               <<"*:*">>),
    lager:info("Search all results, expect removed tombstone b/c aae ... *:*: ~p~n",
               [length(Results2)]),
    ?assertEqual(1, length(Results2)),

    lager:info("Recreate object and check counts"),

    M3 = riakc_map:update(
           {<<"name">>, register},
           fun(R) ->
                   riakc_register:set(<<"hello again, is it me you're looking for">>,
                                      R)
           end, riakc_map:new()),

    ok = riakc_pb_socket:update_type(
           Pid,
           Bucket,
           Key2,
           riakc_map:to_op(M3)),

    yokozuna_rt:commit(Cluster, Index),

    {ok, M4} = riakc_pb_socket:fetch_type(Pid, Bucket, Key2),
    Keys = riakc_map:fetch_keys(M4),
    ?assertEqual(1, length(Keys)),
    ?assert(riakc_map:is_key({<<"name">>, register}, M4)),
    {ok, {search_results, Results3, _, _}} = riakc_pb_socket:search(
                                               Pid, Index,
                                               <<"*:*">>),
    lager:info("Search recreated map *:*: ~p~n", [Results3]),
    ?assertEqual(2, length(Results3)).

%% @private
number_of_fields(Resp, Index) ->
    length(?GET(Index, Resp)).

%% @private
make_intercepts_tab(Node) ->
    SupPid = rpc:call(Node, erlang, whereis, [sasl_safe_sup]),
    intercepts_tab = rpc:call(Node, ets, new, [intercepts_tab, [named_table,
        public, set, {heir, SupPid, {}}]]).
