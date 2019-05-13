-module(bucket_types).

-behavior(riak_test).
-export([confirm/0, mapred_modfun/3, mapred_modfun_type/3]).

-include_lib("eunit/include/eunit.hrl").

-define(SUMVALUE_MAPRED,
            [{map,
                {modfun, riak_kv_mapreduce, map_object_value},
                undefined, false},
            {reduce,
                {modfun, riak_kv_mapreduce, reduce_string_to_integer},
                undefined, false},
            {reduce,
                {modfun, riak_kv_mapreduce, reduce_sum},
                undefined, true}]).

-define(COUNT_MAPRED,
            [{map,
                {modfun, riak_kv_mapreduce, map_object_value},
                undefined, false},
            {reduce,
                {modfun, riak_kv_mapreduce, reduce_count_inputs},
                undefined, true}]).

confirm() ->
    application:start(inets),
    lager:info("Deploy some nodes"),
    Nodes = rt:build_cluster(4, [], [
                                     {riak_core, [{default_bucket_props,
                                                   [
                                                       {n_val, 2},
                                                       {allow_mult, true},
                                                       {dvv_enabled, true}
                                                   ]}]}]),
    Node = hd(Nodes),

    RMD = riak_test_runner:metadata(),
    HaveIndexes = case proplists:get_value(backend, RMD) of
                      undefined -> false; %% default is da 'cask
                      bitcask -> false;
                      _ -> true
                  end,

    PB = rt:pbc(Node),

    lager:info("default type get/put test"),
    %% write explicitly to the default type
    riakc_pb_socket:put(PB, riakc_obj:new({<<"default">>, <<"bucket">>},
                                             <<"key">>, <<"value">>)),

    %% read from the default bucket implicitly
    {ok, O1} = riakc_pb_socket:get(PB, <<"bucket">>, <<"key">>),
    %% read from the default bucket explicitly
    {ok, O2} = riakc_pb_socket:get(PB, {<<"default">>, <<"bucket">>}, <<"key">>),

    %% same object, but slightly different presentation
    ?assertEqual(riakc_obj:key(O1), riakc_obj:key(O2)),
    ?assertEqual(riakc_obj:get_value(O1), riakc_obj:get_value(O2)),
    ?assertEqual(riakc_obj:only_bucket(O1), riakc_obj:only_bucket(O2)),
    ?assertEqual(riakc_obj:vclock(O1), riakc_obj:vclock(O2)),
    ?assertEqual(undefined, riakc_obj:bucket_type(O1)),
    ?assertEqual(<<"default">>, riakc_obj:bucket_type(O2)),

    %% write implicitly to the default bucket
    riakc_pb_socket:put(PB, riakc_obj:update_value(O1, <<"newvalue">>)),

    %% read from the default bucket explicitly
    {ok, O3} = riakc_pb_socket:get(PB, {<<"default">>, <<"bucket">>}, <<"key">>),

    ?assertEqual(<<"newvalue">>, riakc_obj:get_value(O3)),

    lager:info("list_keys test"),
    %% list keys
    ?assertEqual({ok, [<<"key">>]}, riakc_pb_socket:list_keys(PB, <<"bucket">>)),
    ?assertEqual({ok, [<<"key">>]}, riakc_pb_socket:list_keys(PB, {<<"default">>,
                                                      <<"bucket">>})),
    lager:info("list_buckets test"),
    %% list buckets
    ?assertEqual({ok, [<<"bucket">>]}, riakc_pb_socket:list_buckets(PB)),
    ?assertEqual({ok, [<<"bucket">>]}, riakc_pb_socket:list_buckets(PB, <<"default">>)),

    lager:info("default type delete test"),
    %% delete explicitly via the default bucket
    ok = riakc_pb_socket:delete(PB, {<<"default">>, <<"bucket">>}, <<"key">>),

    %% read from the default bucket implicitly
    {error, notfound} = riakc_pb_socket:get(PB, <<"bucket">>, <<"key">>),
    %% read from the default bucket explicitly
    {error, notfound} = riakc_pb_socket:get(PB, {<<"default">>, <<"bucket">>}, <<"key">>),

    %% write it again
    riakc_pb_socket:put(PB, riakc_obj:new({<<"default">>, <<"bucket">>},
                                             <<"key">>, <<"newestvalue">>)),

    {ok, O4} = riakc_pb_socket:get(PB, {<<"default">>, <<"bucket">>}, <<"key">>),

    %% delete explicitly via the default bucket
    ok = riakc_pb_socket:delete_obj(PB, O4),

    %% read from the default bucket implicitly
    {error, notfound} = riakc_pb_socket:get(PB, <<"bucket">>, <<"key">>),
    %% read from the default bucket explicitly
    {error, notfound} = riakc_pb_socket:get(PB, {<<"default">>, <<"bucket">>}, <<"key">>),

    ?assertEqual(ok, rt:wait_until(fun() ->
                                           rt:pbc_really_deleted(PB,
                                                                 <<"bucket">>,
                                                                 [<<"key">>])
                                   end)),

    %% now there should be no buckets or keys to be listed...
    %%
    %% list keys
    ?assertEqual({ok, []}, riakc_pb_socket:list_keys(PB, <<"bucket">>)),
    ?assertEqual({ok, []}, riakc_pb_socket:list_keys(PB, {<<"default">>,
                                                      <<"bucket">>})),
    %% list buckets
    ?assertEqual({ok, []}, riakc_pb_socket:list_buckets(PB)),
    ?assertEqual({ok, []}, riakc_pb_socket:list_buckets(PB, <<"default">>)),


    lager:info("custom type get/put test"),
    Type = <<"mytype">>,
    TypeProps = [{n_val, 3}],
    lager:info("Create bucket type ~p, wait for propagation", [Type]),
    rt:create_and_activate_bucket_type(Node, Type, TypeProps),
    rt:wait_until_bucket_type_status(Type, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {Type, <<"bucket">>}, TypeProps),

    lager:info("doing put"),
    riakc_pb_socket:put(PB, riakc_obj:new({Type, <<"bucket">>},
                                             <<"key">>, <<"newestvalue">>)),

    lager:info("custom type list_keys test"),
    ?assertEqual({ok, []}, riakc_pb_socket:list_keys(PB, <<"bucket">>)),
    ?assertEqual({ok, [<<"key">>]}, riakc_pb_socket:list_keys(PB, {Type,
                                                      <<"bucket">>})),
    lager:info("doing get"),
    {ok, O5} = riakc_pb_socket:get(PB, {Type, <<"bucket">>}, <<"key">>),

    ?assertEqual(<<"newestvalue">>, riakc_obj:get_value(O5)),

    lager:info("doing get"),
    %% this type is NOT aliased to the default buckey
    {error, notfound} = riakc_pb_socket:get(PB, <<"bucket">>, <<"key">>),

    lager:info("custom type list_buckets test"),
    %% list buckets
    ?assertEqual({ok, []}, riakc_pb_socket:list_buckets(PB)),
    ?assertEqual({ok, [<<"bucket">>]}, riakc_pb_socket:list_buckets(PB, Type)),

    %%% Beginning of UTF-8 test

    lager:info("UTF-8 type get/put test"),
    %% こんにちは - konnichiwa (Japanese)
    UnicodeType = unicode:characters_to_binary([12371,12435,12395,12385,12399], utf8),
    %% سلام - Salam (Arabic)
    UnicodeBucket = unicode:characters_to_binary([1587,1604,1575,1605], utf8),
    lager:info("Create bucket type, wait for propagation"),
    rt:create_and_activate_bucket_type(Node, UnicodeType, TypeProps),
    rt:wait_until_bucket_type_status(UnicodeType, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {UnicodeType, UnicodeBucket}, TypeProps),

    lager:info("doing put"),
    riakc_pb_socket:put(PB, riakc_obj:new({UnicodeType, UnicodeBucket},
                                             <<"key">>, <<"yetanothervalue">>)),

    lager:info("custom type list_keys test"),
    ?assertEqual({ok, [<<"key">>]}, riakc_pb_socket:list_keys(PB,
                                                              {UnicodeType,
                                                               UnicodeBucket})),
    lager:info("doing get"),
    {ok, O6} = riakc_pb_socket:get(PB, {UnicodeType, UnicodeBucket}, <<"key">>),

    ?assertEqual(<<"yetanothervalue">>, riakc_obj:get_value(O6)),

    lager:info("custom type list_buckets test"),
    %% list buckets
    ?assertEqual({ok, [UnicodeBucket]}, riakc_pb_socket:list_buckets(PB, UnicodeType)),

    %%% End of UTF-8 test

    lager:info("bucket properties tests"),
    riakc_pb_socket:set_bucket(PB, {<<"default">>, <<"mybucket">>},
                               [{n_val, 5}]),
    {ok, BProps} = riakc_pb_socket:get_bucket(PB, <<"mybucket">>),
    ?assertEqual(5, proplists:get_value(n_val, BProps)),

    riakc_pb_socket:reset_bucket(PB, {<<"default">>, <<"mybucket">>}),

    {ok, BProps1} = riakc_pb_socket:get_bucket(PB, <<"mybucket">>),
    ?assertEqual(2, proplists:get_value(n_val, BProps1)),

    riakc_pb_socket:set_bucket(PB, {Type, <<"mybucket">>},
                               [{n_val, 5}]),
    {ok, BProps2} = riakc_pb_socket:get_bucket(PB, <<"mybucket">>),
    %% the default in the app.config is set to 2...
    ?assertEqual(2, proplists:get_value(n_val, BProps2)),

    {ok, BProps3} = riakc_pb_socket:get_bucket(PB, {Type,
                                                    <<"mybucket">>}),
    ?assertEqual(5, proplists:get_value(n_val, BProps3)),

    %% Check our unicode brethren
    riakc_pb_socket:set_bucket(PB, {UnicodeType, UnicodeBucket},
                               [{n_val, 4}]),
    {ok, UBProps1} = riakc_pb_socket:get_bucket(PB, {UnicodeType,
                                                     UnicodeBucket}),
    ?assertEqual(4, proplists:get_value(n_val, UBProps1)),

    riakc_pb_socket:reset_bucket(PB, {Type, <<"mybucket">>}),

    {ok, BProps4} = riakc_pb_socket:get_bucket(PB, {Type,
                                                    <<"mybucket">>}),
    ?assertEqual(3, proplists:get_value(n_val, BProps4)),

    riakc_pb_socket:reset_bucket(PB, {UnicodeType, UnicodeBucket}),

    {ok, UBProps2} = riakc_pb_socket:get_bucket(PB, {UnicodeType,
                                                     UnicodeBucket}),

    ?assertEqual(3, proplists:get_value(n_val, UBProps2)),

    {error, NTGR} = riakc_pb_socket:get_bucket(PB, {<<"nonexistent">>, <<"mybucket">>}),

    lager:info("GOT ERROR ~s", [NTGR]),

    ?assertMatch(<<"No bucket-type named 'nonexistent'", _/binary>>, NTGR),

    {error, NTSR} = riakc_pb_socket:set_bucket(PB, {<<"nonexistent">>, <<"mybucket">>}, [{n_val, 3}]),

    lager:info("GOT ERROR ~s", [NTSR]),

    ?assertMatch(<<"No bucket-type named 'nonexistent'", _/binary>>, NTSR),

    lager:info("bucket type properties test"),

    riakc_pb_socket:set_bucket_type(PB, Type,
                               [{n_val, 5}]),

    {ok, BProps5} = riakc_pb_socket:get_bucket_type(PB, Type),

    ?assertEqual(5, proplists:get_value(n_val, BProps5)),

    %% check that the bucket inherits from its type
    {ok, BProps6} = riakc_pb_socket:get_bucket(PB, {Type,
                                                    <<"mybucket">>}),
    ?assertEqual(5, proplists:get_value(n_val, BProps6)),

    riakc_pb_socket:set_bucket_type(PB, Type, [{n_val, 3}]),

    {ok, BProps7} = riakc_pb_socket:get_bucket_type(PB, Type),

    ?assertEqual(3, proplists:get_value(n_val, BProps7)),

    %% Repeat type checks for unicode type/bucket

    riakc_pb_socket:set_bucket_type(PB, UnicodeType,
                                    [{n_val, 5}]),

    {ok, UBProps3} = riakc_pb_socket:get_bucket_type(PB, UnicodeType),

    ?assertEqual(5, proplists:get_value(n_val, UBProps3)),

    %% check that the bucket inherits from its type
    {ok, UBProps4} = riakc_pb_socket:get_bucket(PB, {UnicodeType,
                                                     UnicodeBucket}),

    ?assertEqual(5, proplists:get_value(n_val, UBProps4)),

    riakc_pb_socket:set_bucket_type(PB, UnicodeType, [{n_val, 3}]),

    {ok, UBProps5} = riakc_pb_socket:get_bucket_type(PB, UnicodeType),

    ?assertEqual(3, proplists:get_value(n_val, UBProps5)),

    %% make sure a regular bucket under the default type reflects app.config
    {ok, BProps8} = riakc_pb_socket:get_bucket(PB, {<<"default">>,
                                                    <<"mybucket">>}),
    ?assertEqual(2, proplists:get_value(n_val, BProps8)),

    %% make sure the type we previously created is NOT affected
    {ok, BProps9} = riakc_pb_socket:get_bucket_type(PB, Type),

    ?assertEqual(3, proplists:get_value(n_val, BProps9)),

    %% make sure a bucket under that type is also not affected
    {ok, BProps10} = riakc_pb_socket:get_bucket(PB, {Type,
                                                    <<"mybucket">>}),
    ?assertEqual(3, proplists:get_value(n_val, BProps10)),

    %% make sure a newly created type is not affected either
    %% create a new type
    Type2 = <<"mynewtype">>,
    rt:create_and_activate_bucket_type(Node, Type2, []),
    rt:wait_until_bucket_type_status(Type2, active, Nodes),

    {ok, BProps11} = riakc_pb_socket:get_bucket_type(PB, Type2),

    ?assertEqual(3, proplists:get_value(n_val, BProps11)),

    %% 2i tests

    case HaveIndexes of
        false -> ok;
        true ->
            Obj01 = riakc_obj:new(<<"test">>, <<"JRD">>, <<"John Robert Doe, 25">>),
            Obj02 = riakc_obj:new({Type, <<"test">>}, <<"JRD">>, <<"Jane Rachel Doe, 21">>),

            Obj1 = riakc_obj:update_metadata(Obj01,
                                             riakc_obj:set_secondary_index(
                                               riakc_obj:get_update_metadata(Obj01),
                                               [{{integer_index, "age"},
                                                 [25]},{{binary_index, "name"},
                                                        [<<"John">>, <<"Robert">>
                                                         ,<<"Doe">>]}])),

            Obj2 = riakc_obj:update_metadata(Obj02,
                                             riakc_obj:set_secondary_index(
                                               riakc_obj:get_update_metadata(Obj02),
                                               [{{integer_index, "age"},
                                                 [21]},{{binary_index, "name"},
                                                        [<<"Jane">>, <<"Rachel">>
                                                         ,<<"Doe">>]}])),

            riakc_pb_socket:put(PB, Obj1),
            riakc_pb_socket:put(PB, Obj2),

            ?assertMatch({ok, {index_results_v1, [<<"JRD">>], _, _}}, riakc_pb_socket:get_index(PB, <<"test">>,
                                                                                                {binary_index,
                                                                                                 "name"},
                                                                                                <<"John">>)),

            ?assertMatch({ok, {index_results_v1, [], _, _}}, riakc_pb_socket:get_index(PB, <<"test">>,
                                                                                       {binary_index,
                                                                                        "name"},
                                                                                       <<"Jane">>)),

            ?assertMatch({ok, {index_results_v1, [<<"JRD">>], _, _}}, riakc_pb_socket:get_index(PB,
                                                                                                {Type,
                                                                                                 <<"test">>},
                                                                                                {binary_index,
                                                                                                 "name"},
                                                                                                <<"Jane">>)),

            %% wild stab at the undocumented cs_bucket_fold
            {ok, ReqID} = riakc_pb_socket:cs_bucket_fold(PB, <<"test">>, []),
            accumulate(ReqID),

            {ok, ReqID2} = riakc_pb_socket:cs_bucket_fold(PB, {Type,
                                                               <<"test">>}, []),
            accumulate(ReqID2),
            ok
    end,


    Store = fun(Bucket, {K,V, BI, II}) ->
                    O=riakc_obj:new(Bucket, K),
                    MD=riakc_obj:add_secondary_index(dict:new(),
                                                     {{binary_index, "b_idx"},
                                                      [BI]}),
                    MD2=riakc_obj:add_secondary_index(MD, {{integer_index,
                                                            "i_idx"}, [II]}),
                    OTwo=riakc_obj:update_metadata(O,MD2),
                    lager:info("storing ~p", [OTwo]),
                    riakc_pb_socket:put(PB,riakc_obj:update_value(OTwo, V, "application/json"))
            end,

    [Store(<<"MRbucket">>, KV) || KV <- [
                         {<<"foo">>, <<"2">>, <<"a">>, 4},
                         {<<"bar">>, <<"3">>, <<"b">>, 7},
                         {<<"baz">>, <<"4">>, <<"a">>, 4}]],

    ?assertEqual({ok, [{2, [9]}]},
                 riakc_pb_socket:mapred_bucket(PB, <<"MRbucket">>, ?SUMVALUE_MAPRED)),

    [Store({Type, <<"MRbucket">>}, KV) || KV <- [
                         {<<"foo">>, <<"2">>, <<"a">>, 4},
                         {<<"bar">>, <<"3">>, <<"b">>, 7},
                         {<<"baz">>, <<"4">>, <<"a">>, 4},
                         {<<"bam">>, <<"5">>, <<"a">>, 3}]],

    ?assertEqual({ok, [{0, [<<"2">>]}]},
                 riakc_pb_socket:mapred(PB, {{Type, <<"MRbucket">>},
                                             [[<<"starts_with">>, <<"f">>]]},
                                        [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, true}])),

    ?assertEqual({ok, [{2, [14]}]},
                 riakc_pb_socket:mapred_bucket(PB, {Type, <<"MRbucket">>},
                                                ?SUMVALUE_MAPRED)),

    ?assertEqual({ok, [{1, [3]}]},
                 riakc_pb_socket:mapred(PB,
                                [{<<"MRbucket">>, <<"foo">>},
                                 {<<"MRbucket">>, <<"bar">>},
                                 {<<"MRbucket">>, <<"baz">>}],
                                ?COUNT_MAPRED)),

    ?assertEqual({ok, [{1, [4]}]},
                 riakc_pb_socket:mapred(PB,
                                [{{{Type, <<"MRbucket">>}, <<"foo">>},
                                  undefined},
                                 {{{Type, <<"MRbucket">>}, <<"bar">>},
                                  undefined},
                                 {{{Type, <<"MRbucket">>}, <<"baz">>},
                                  undefined},
                                 {{{Type, <<"MRbucket">>}, <<"bam">>},
                                 undefined}],
                                ?COUNT_MAPRED)),

    case HaveIndexes of
        false -> ok;
        true ->
            {ok, [{1, Results}]} = riakc_pb_socket:mapred(PB,
                                                {index,<<"MRbucket">>,{integer_index,
                                                                        "i_idx"},3,5},
                                                [{map, {modfun, riak_kv_mapreduce,
                                                        map_object_value},
                                                    undefined, false},
                                                {reduce, {modfun, riak_kv_mapreduce,
                                                            reduce_set_union},
                                                    undefined, true}]),
            ?assertEqual([<<"2">>, <<"4">>], lists:sort(Results)),

            {ok, [{1, Results1}]} = riakc_pb_socket:mapred(PB,
                                                {index,{Type,
                                                        <<"MRbucket">>},{integer_index,
                                                                        "i_idx"},3,5},
                                                [{map, {modfun, riak_kv_mapreduce,
                                                        map_object_value},
                                                    undefined, false},
                                                {reduce, {modfun, riak_kv_mapreduce,
                                                            reduce_set_union},
                                                    undefined, true}]),
            ?assertEqual([<<"2">>, <<"4">>, <<"5">>], lists:sort(Results1)),

            {ok, [{1, Results2}]} = riakc_pb_socket:mapred(PB,
                                                {index,<<"MRbucket">>,{binary_index,
                                                                        "b_idx"}, <<"a">>},
                                                [{map, {modfun, riak_kv_mapreduce,
                                                        map_object_value},
                                                    undefined, false},
                                                {reduce, {modfun, riak_kv_mapreduce,
                                                            reduce_set_union},
                                                    undefined, true}]),
            ?assertEqual([<<"2">>, <<"4">>], lists:sort(Results2)),

            {ok, [{1, Results3}]} = riakc_pb_socket:mapred(PB,
                                                {index,{Type,
                                                        <<"MRbucket">>},{binary_index,
                                                                        "b_idx"}, <<"a">>},
                                                [{map, {modfun, riak_kv_mapreduce,
                                                        map_object_value},
                                                    undefined, false},
                                                {reduce, {modfun, riak_kv_mapreduce,
                                                            reduce_set_union},
                                                    undefined, true}]),
            ?assertEqual([<<"2">>, <<"4">>, <<"5">>], lists:sort(Results3)),
            ok
    end,

    %% load this module on all the nodes
    ok = rt:load_modules_on_nodes([?MODULE], Nodes),

    %% do a modfun mapred using the function from this module
    ?assertEqual({ok, [{2, [2]}]},
                 riakc_pb_socket:mapred_bucket(PB, {modfun, ?MODULE,
                                                    mapred_modfun, []},
                                       ?SUMVALUE_MAPRED)),

    %% do a modfun mapred using the function from this module
    ?assertEqual({ok, [{2, [5]}]},
                 riakc_pb_socket:mapred_bucket(PB, {modfun, ?MODULE,
                                                    mapred_modfun_type, []},
                                       ?SUMVALUE_MAPRED)),

    riakc_pb_socket:stop(PB),
    pass.

accumulate(ReqID) ->
    receive
        {ReqID, {done, _}} ->
            ok;
        {ReqID, Msg} ->
            lager:info("got ~p", [Msg]),
            accumulate(ReqID)
    end.

mapred_modfun(Pipe, Args, _Timeout) ->
    lager:info("Args for mapred modfun are ~p", [Args]),
    riak_pipe:queue_work(Pipe, {{<<"MRbucket">>, <<"foo">>}, {struct, []}}),
    riak_pipe:eoi(Pipe).

mapred_modfun_type(Pipe, Args, _Timeout) ->
    lager:info("Args for mapred modfun are ~p", [Args]),
    riak_pipe:queue_work(Pipe, {{{<<"mytype">>, <<"MRbucket">>}, <<"bam">>}, {struct, []}}),
    riak_pipe:eoi(Pipe).
