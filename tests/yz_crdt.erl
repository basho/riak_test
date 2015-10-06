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
    %% Perform simple queries, check for register, set fields.
    ok = rt:wait_until(
           fun() ->
               validate_search_results(Pid)
           end),
    %% Stop PB connection.
    riakc_pb_socket:stop(Pid),

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

 		test_repeat_sets(Pid, Nodes, ?BUCKET, ?INDEX, ?KEY),
    	test_delete(Pid, Nodes, ?BUCKET, ?INDEX, ?KEY),
    	test_delete_aae(Pid, Nodes, ?BUCKET, ?INDEX),
    	test_siblings(Pid, Nodes, ?BUCKET, ?INDEX),
        true
    %% Stop PB connection.
    riakc_pb_socket:stop(Pid),

    %% Clean cluster.
    rt:clean_cluster(Nodes),

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

    [rt_intercept:add(ANode, {yz_kv, [{{delete_operation, 5},
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

test_siblings(Pid, Cluster, Bucket, Index) ->
    Key1 = <<"Movies">>,
    Key2 = <<"Games">>,
    Set1 = <<"directors">>,
    Set2 = <<"characters">>,

    %% make siblings
    {P1, P2} = lists:split(1, Cluster),

    %% PB connections for accessing each side
    Pid1 = rt:pbc(hd(P1)),
    Pid2 = rt:pbc(hd(P2)),

    %% Create an object in Partition 1 and siblings in Partition 2
    lager:info("Create partition: ~p | ~p", [P1, P2]),
    Partition = rt:partition(P1, P2),
    try
        %% P1 writes
        lager:info("Writing to Partition 1 Set 1: Key ~p | Director ~p",
                   [Key1, <<"Kubrick">>]),
        M1 = riakc_map:update(
               {Set1, set},
               fun(S) ->
                       riakc_set:add_element(<<"Kubrick">>, S)
               end, riakc_map:new()),
        ok = riakc_pb_socket:update_type(
               Pid1,
               Bucket,
               Key1,
               riakc_map:to_op(M1)),

        lager:info("Writing to Partition 1 Set 1: Key ~p | Director ~p",
                   [Key1, <<"Demme">>]),
        M2 = riakc_map:update(
               {Set1, set},
               fun(S) ->
                       riakc_set:add_element(<<"Demme">>, S)
               end, riakc_map:new()),
        ok = riakc_pb_socket:update_type(
               Pid1,
               Bucket,
               Key1,
               riakc_map:to_op(M2)),

        %% P2 Siblings
        lager:info("Writing to Partition 2 Set 2: Key ~p | Char ~p",
                   [Key2, <<"Sonic">>]),
        M3 = riakc_map:update(
               {Set2, set},
               fun(S) ->
                       riakc_set:add_element(<<"Sonic">>, S)
               end, riakc_map:new()),
        ok = riakc_pb_socket:update_type(
               Pid2,
               Bucket,
               Key2,
               riakc_map:to_op(M3)),

        lager:info("Delete key from Partition 2: Key ~p", [Key2]),
        ok = riakc_pb_socket:delete(Pid2, Bucket, Key2),

        lager:info("Writing to Partition 2 Set 2: after delete: Key ~p | Char ~p",
                   [Key2, <<"Crash">>]),
        M4 = riakc_map:update(
               {Set2, set},
               fun(S) ->
                       riakc_set:add_element(<<"Crash">>, S)
               end, riakc_map:new()),
        ok = riakc_pb_socket:update_type(
               Pid2,
               Bucket,
               Key2,
               riakc_map:to_op(M4)),

        lager:info("Writing to Partition 2 Set 2: Key ~p | Char ~p",
                   [Key2, <<"Mario">>]),
        M5 = riakc_map:update(
               {Set2, set},
               fun(S) ->
                       riakc_set:add_element(<<"Mario">>, S)
               end, riakc_map:new()),
        ok = riakc_pb_socket:update_type(
               Pid2,
               Bucket,
               Key2,
               riakc_map:to_op(M5)),

        lager:info("Writing to Partition 2 Set 1: Key ~p | Director ~p",
                   [Key1, <<"Klimov">>]),
        M6 = riakc_map:update(
               {Set1, set},
               fun(S) ->
                       riakc_set:add_element(<<"Klimov">>, S)
               end, riakc_map:new()),
        ok = riakc_pb_socket:update_type(
               Pid2,
               Bucket,
               Key1,
               riakc_map:to_op(M6)),
        ok
    after
        rt:heal(Partition)
    end,

    rt:wait_until_transfers_complete(Cluster),
    yokozuna_rt:commit(Cluster, ?INDEX),

    %% Verify Counts
    lager:info("Verify counts and operations after heal + transfers + commits"),

    {ok, MF1} = riakc_pb_socket:fetch_type(Pid, Bucket, Key1),
    Keys = riakc_map:fetch_keys(MF1),
    ?assertEqual(1, length(Keys)),
    ?assert(riakc_map:is_key({<<"directors">>, set}, MF1)),
    {ok, {search_results, Results1, _, _}} = riakc_pb_socket:search(
                                               Pid, Index,
                                               <<"directors_set:*">>),
    lager:info("Search movies map directors_set:*: ~p~n", [Results1]),
    ?assertEqual(3, length(proplists:lookup_all(<<"directors_set">>,
                                                ?GET(?INDEX, Results1)))),

    {ok, MF2} = riakc_pb_socket:fetch_type(Pid, Bucket, Key2),
    Keys2 = riakc_map:fetch_keys(MF2),
    ?assertEqual(1, length(Keys2)),
    ?assert(riakc_map:is_key({<<"characters">>, set}, MF2)),
    {ok, {search_results, Results2, _, _}} = riakc_pb_socket:search(
                                               Pid, Index,
                                               <<"characters_set:*">>),
    lager:info("Search games map characters_set:*: ~p~n", [Results2]),
    ?assertEqual(2, length(proplists:lookup_all(<<"characters_set">>,
                                                ?GET(?INDEX, Results2)))),

    {ok, {search_results, Results3, _, _}} = riakc_pb_socket:search(
                                               Pid, Index,
                                               <<"_yz_vtag:*">>),
    lager:info("Search vtags in search *:*: ~p~n", [Results3]),
    ?assertEqual(0, length(Results3)).


%% @private
number_of_fields(Resp, Index) ->
    length(?GET(Index, Resp)).

%% @private
make_intercepts_tab(Node) ->
    SupPid = rpc:call(Node, erlang, whereis, [sasl_safe_sup]),
    intercepts_tab = rpc:call(Node, ets, new, [intercepts_tab, [named_table,
        public, set, {heir, SupPid, {}}]]).
