-module(bucket_types).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    application:start(inets),
    lager:info("Deploy some nodes"),
    Nodes = rt:build_cluster(4, [], [{riak_core, [{default_bucket_props,
                                                   [{n_val, 2}]}]}]),
    Node = hd(Nodes),

    {ok, [{"127.0.0.1", Port}]} = rpc:call(Node, application, get_env,
                                           [riak_api, pb]),

    {ok, PB} = riakc_pb_socket:start_link("127.0.0.1", Port, []),

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
    ?assertEqual(undefined, riakc_obj:type(O1)),
    ?assertEqual(<<"default">>, riakc_obj:type(O2)),

    %% write implicitly to the default bucket
    riakc_pb_socket:put(PB, riakc_obj:new(<<"bucket">>,
                                             <<"key">>, <<"newvalue">>)),
 
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

    timer:sleep(5000), %% wait for delete_mode 3s to expire

    %% now there shoyld be no buckets or keys to be listed...
    %%
    %% list keys
    ?assertEqual({ok, []}, riakc_pb_socket:list_keys(PB, <<"bucket">>)),
    ?assertEqual({ok, []}, riakc_pb_socket:list_keys(PB, {<<"default">>,
                                                      <<"bucket">>})),
    %% list buckets
    ?assertEqual({ok, []}, riakc_pb_socket:list_buckets(PB)),
    ?assertEqual({ok, []}, riakc_pb_socket:list_buckets(PB, <<"default">>)),


    lager:info("custom type get/put test"),
    %% create a new type
    ok = rpc:call(Node, riak_core_bucket_type, create, [<<"mytype">>, [{n_val,
                                                                        3}]]),
    %% allow cluster metadata some time to propogate
    timer:sleep(1000),

    lager:info("doing put"),
    riakc_pb_socket:put(PB, riakc_obj:new({<<"mytype">>, <<"bucket">>},
                                             <<"key">>, <<"newestvalue">>)),

    lager:info("doing get"),
    {ok, O5} = riakc_pb_socket:get(PB, {<<"mytype">>, <<"bucket">>}, <<"key">>),

    ?assertEqual(<<"newestvalue">>, riakc_obj:get_value(O5)),

    lager:info("doing get"),
    %% this type is NOT aliased to the default buckey
    {error, notfound} = riakc_pb_socket:get(PB, <<"bucket">>, <<"key">>),

    lager:info("custom type list_keys test"),
    ?assertEqual({ok, []}, riakc_pb_socket:list_keys(PB, <<"bucket">>)),
    ?assertEqual({ok, [<<"key">>]}, riakc_pb_socket:list_keys(PB, {<<"mytype">>,
                                                      <<"bucket">>})),
    lager:info("custom type list_buckets test"),
    %% list buckets
    ?assertEqual({ok, []}, riakc_pb_socket:list_buckets(PB)),
    ?assertEqual({ok, [<<"bucket">>]}, riakc_pb_socket:list_buckets(PB, <<"mytype">>)),

    lager:info("bucket properties tests"),
    riakc_pb_socket:set_bucket(PB, {<<"default">>, <<"mybucket">>},
                               [{n_val, 5}]),
    {ok, BProps} = riakc_pb_socket:get_bucket(PB, <<"mybucket">>),
    ?assertEqual(5, proplists:get_value(n_val, BProps)),

    riakc_pb_socket:reset_bucket(PB, {<<"default">>, <<"mybucket">>}),

    {ok, BProps1} = riakc_pb_socket:get_bucket(PB, <<"mybucket">>),
    ?assertEqual(2, proplists:get_value(n_val, BProps1)),

    riakc_pb_socket:set_bucket(PB, {<<"mytype">>, <<"mybucket">>},
                               [{n_val, 5}]),
    {ok, BProps2} = riakc_pb_socket:get_bucket(PB, <<"mybucket">>),
    %% the default in the app.config is set to 2...
    ?assertEqual(2, proplists:get_value(n_val, BProps2)),

    {ok, BProps3} = riakc_pb_socket:get_bucket(PB, {<<"mytype">>,
                                                    <<"mybucket">>}),
    ?assertEqual(5, proplists:get_value(n_val, BProps3)),

    riakc_pb_socket:reset_bucket(PB, {<<"mytype">>, <<"mybucket">>}),

    {ok, BProps4} = riakc_pb_socket:get_bucket(PB, {<<"mytype">>,
                                                    <<"mybucket">>}),
    ?assertEqual(3, proplists:get_value(n_val, BProps4)),

    lager:info("bucket type properties test"),

    riakc_pb_socket:set_bucket_type(PB, <<"mytype">>,
                               [{n_val, 5}]),

    {ok, BProps5} = riakc_pb_socket:get_bucket_type(PB, <<"mytype">>),

    ?assertEqual(5, proplists:get_value(n_val, BProps5)),

    %% check that the bucket inherits from its type
    {ok, BProps6} = riakc_pb_socket:get_bucket(PB, {<<"mytype">>,
                                                    <<"mybucket">>}),
    ?assertEqual(5, proplists:get_value(n_val, BProps6)),

    riakc_pb_socket:reset_bucket_type(PB, <<"mytype">>),

    {ok, BProps7} = riakc_pb_socket:get_bucket_type(PB, <<"mytype">>),

    ?assertEqual(3, proplists:get_value(n_val, BProps7)),

    %% make sure a regular bucket under the default type reflects app.config
    {ok, BProps8} = riakc_pb_socket:get_bucket(PB, {<<"default">>,
                                                    <<"mybucket">>}),
    ?assertEqual(2, proplists:get_value(n_val, BProps8)),

    %% make sure the type we previously created is NOT affected
    {ok, BProps9} = riakc_pb_socket:get_bucket_type(PB, <<"mytype">>),

    ?assertEqual(3, proplists:get_value(n_val, BProps9)),

    %% make sure a bucket under that type is also not affected
    {ok, BProps10} = riakc_pb_socket:get_bucket(PB, {<<"mytype">>,
                                                    <<"mybucket">>}),
    ?assertEqual(3, proplists:get_value(n_val, BProps10)),

    %% make sure a newly created type is not affected either
    %% create a new type
    ok = rpc:call(Node, riak_core_bucket_type, create, [<<"mynewtype">>, []]),
    %% allow cluster metadata some time to propogate
    timer:sleep(1000),

    {ok, BProps11} = riakc_pb_socket:get_bucket_type(PB, <<"mynewtype">>),

    ?assertEqual(3, proplists:get_value(n_val, BProps11)),


    ok.

