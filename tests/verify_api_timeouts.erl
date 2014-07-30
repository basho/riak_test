
-module(verify_api_timeouts).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"listkeys_bucket">>).
-define(NUM_BUCKETS, 1200).
-define(NUM_KEYS, 1000).

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    [Node] = rt_cluster:build_cluster(1),
    rt:wait_until_pingable(Node),

    HC = rt_http:httpc(Node),
    lager:info("setting up initial data and loading remote code"),
    rt_http:httpc_write(HC, <<"foo">>, <<"bar">>, <<"foobarbaz\n">>),
    rt_http:httpc_write(HC, <<"foo">>, <<"bar2">>, <<"foobarbaz2\n">>),

    put_keys(Node, ?BUCKET, ?NUM_KEYS),
    put_buckets(Node, ?NUM_BUCKETS),
    timer:sleep(2000),

    rt_intercept:add(Node, {riak_kv_get_fsm,
                            [{{prepare,2}, slow_prepare}]}),
    rt_intercept:add(Node, {riak_kv_put_fsm,
                            [{{prepare,2}, slow_prepare}]}),
    rt_intercept:add(Node, {riak_kv_vnode,
                            [{{handle_coverage,4}, slow_handle_coverage}]}),


    lager:info("testing HTTP API"),

    lager:info("testing GET timeout"),
    {error, Tup1} = rhc:get(HC, <<"foo">>, <<"bar">>, [{timeout, 100}]),
    ?assertMatch({ok, "503", _, <<"request timed out\n">>}, Tup1),

    lager:info("testing PUT timeout"),
    {error, Tup2} = rhc:put(HC, riakc_obj:new(<<"foo">>, <<"bar">>,
                                              <<"getgetgetgetget\n">>),
                            [{timeout, 100}]),
    ?assertMatch({ok, "503", _, <<"request timed out\n">>}, Tup2),

    lager:info("testing DELETE timeout"),
    {error, Tup3} = rhc:delete(HC, <<"foo">>, <<"bar">>, [{timeout, 100}]),
    ?assertMatch({ok, "503", _, <<"request timed out\n">>}, Tup3),

    lager:info("testing invalid timeout value"),
    {error, Tup4} = rhc:get(HC, <<"foo">>, <<"bar">>, [{timeout, asdasdasd}]),
    ?assertMatch({ok, "400", _,
                  <<"Bad timeout value \"asdasdasd\"\n">>},
                 Tup4),

    lager:info("testing GET still works before long timeout"),
    {ok, O} = rhc:get(HC, <<"foo">>, <<"bar">>, [{timeout, 4000}]),

    %% either of these are potentially valid.
    case riakc_obj:get_values(O) of
        [<<"foobarbaz\n">>] ->
            lager:info("Original Value"),
            ok;
        [<<"getgetgetgetget\n">>] ->
            lager:info("New Value"),
            ok;
        [_A, _B] = L ->
            ?assertEqual([<<"foobarbaz\n">>,<<"getgetgetgetget\n">>],
                         lists:sort(L)),
            lager:info("Both Values"),
            ok;
        V -> ?assertEqual({object_value, <<"getgetgetgetget\n">>},
                          {object_value, V})
    end,


    PC = rt_pb:pbc(Node),

    lager:info("testing PBC API"),

    BOOM = {error, <<"timeout">>},

    lager:info("testing GET timeout"),
    PGET = riakc_pb_socket:get(PC, <<"foo">>, <<"bar2">>, [{timeout, 100}]),
    ?assertEqual(BOOM, PGET),

    lager:info("testing PUT timeout"),
    PPUT = riakc_pb_socket:put(PC,
                               riakc_obj:new(<<"foo">>, <<"bar2">>,
                                             <<"get2get2get2get2get\n">>),
                               [{timeout, 100}]),
    ?assertEqual(BOOM, PPUT),

    lager:info("testing DELETE timeout"),
    PDEL = riakc_pb_socket:delete(PC, <<"foo">>, <<"bar2">>,
                                  [{timeout, 100}]),
    ?assertEqual(BOOM, PDEL),

    lager:info("testing invalid timeout value"),
    ?assertError(badarg, riakc_pb_socket:get(PC, <<"foo">>, <<"bar2">>,
                                             [{timeout, asdasdasd}])),

    lager:info("testing GET still works before long timeout"),
    {ok, O2} = riakc_pb_socket:get(PC, <<"foo">>, <<"bar2">>,
                                  [{timeout, 4000}]),

    %% either of these are potentially valid.
    case riakc_obj:get_values(O2) of
        [<<"get2get2get2get2get\n">>] ->
            lager:info("New Value"),
            ok;
        [<<"foobarbaz2\n">>] ->
            lager:info("Original Value"),
            ok;
        [_A2, _B2] = L2 ->
            ?assertEqual([<<"foobarbaz2\n">>, <<"get2get2get2get2get\n">>],
                         lists:sort(L2)),
            lager:info("Both Values"),
            ok;
        V2 -> ?assertEqual({object_value, <<"get2get2get2get2get\n">>}, 
                           {object_value, V2})
    end,


    Long = 1000000,
    Short = 1000,

    lager:info("Checking List timeouts"),

    lager:info("Checking PBC"),
    Pid = rt_pb:pbc(Node),
    lager:info("Checking keys timeout"),
    ?assertMatch({error, <<"timeout">>},
                 riakc_pb_socket:list_keys(Pid, ?BUCKET, Short)),
    lager:info("Checking keys w/ long timeout"),
    ?assertMatch({ok, _},
                 riakc_pb_socket:list_keys(Pid, ?BUCKET, Long)),
    lager:info("Checking stream keys timeout"),
    {ok, ReqId0} = riakc_pb_socket:stream_list_keys(Pid, ?BUCKET, Short),
    wait_for_error(ReqId0),
    lager:info("Checking stream keys works w/ long timeout"),
    {ok, ReqId8} = riakc_pb_socket:stream_list_keys(Pid, ?BUCKET, Long),
    wait_for_end(ReqId8),

    lager:info("Checking buckets timeout"),
    ?assertMatch({error, <<"timeout">>},
                 riakc_pb_socket:list_buckets(Pid, Short)),
    lager:info("Checking buckets w/ long timeout"),
    ?assertMatch({ok, _},
                 riakc_pb_socket:list_buckets(Pid, Long)),
    lager:info("Checking stream buckets timeout"),
    {ok, ReqId1} = riakc_pb_socket:stream_list_buckets(Pid, Short),
    wait_for_error(ReqId1),
    lager:info("Checking stream buckets works w/ long timeout"),
    {ok, ReqId7} = riakc_pb_socket:stream_list_buckets(Pid, Long),
    wait_for_end(ReqId7),


    lager:info("Checking HTTP"),
    LHC = rt_http:httpc(Node),
    lager:info("Checking keys timeout"),
    ?assertMatch({error, <<"timeout">>},
                 rhc:list_keys(LHC, ?BUCKET, Short)),
    lager:info("Checking keys w/ long timeout"),
    ?assertMatch({ok, _},
                 rhc:list_keys(LHC, ?BUCKET, Long)),
    lager:info("Checking stream keys timeout"),
    {ok, ReqId2} = rhc:stream_list_keys(LHC, ?BUCKET, Short),
    wait_for_error(ReqId2),
    lager:info("Checking stream keys works w/ long timeout"),
    {ok, ReqId4} = rhc:stream_list_keys(LHC, ?BUCKET, Long),
    wait_for_end(ReqId4),

    lager:info("Checking buckets timeout"),
    ?assertMatch({error, <<"timeout">>},
                 rhc:list_buckets(LHC, Short)),
    lager:info("Checking buckets w/ long timeout"),
    ?assertMatch({ok, _},
                 rhc:list_buckets(LHC, Long)),
    lager:info("Checking stream buckets timeout"),
    {ok, ReqId3} = rhc:stream_list_buckets(LHC, Short),
    wait_for_error(ReqId3),
    lager:info("Checking stream buckets works w/ long timeout"),
    {ok, ReqId5} = rhc:stream_list_buckets(LHC, Long),
    wait_for_end(ReqId5),




    pass.


wait_for_error(ReqId) ->
    receive
        {ReqId, done} ->
            lager:error("stream incorrectly finished"),
            error(stream_finished);
        {ReqId, {error, <<"timeout">>}} ->
            lager:info("stream correctly timed out"),
            ok;
        {ReqId, {_Key, _Vals}} ->
            %% the line below is spammy but nice for debugging
            %%{ReqId, {Key, Vals}} ->
            %%lager:info("Got some values ~p, ~p", [Key, Vals]),
            wait_for_error(ReqId);
        {ReqId, Other} ->
            error({unexpected_message, Other})
    after 10000 ->
            error(error_stream_recv_timed_out)
    end.

wait_for_end(ReqId) ->
    receive
        {ReqId, done} ->
            lager:info("stream correctly finished"),
            ok;
        {ReqId, {error, <<"timeout">>}} ->
            lager:error("stream incorrectly timed out"),
            error(stream_timed_out);
       {ReqId, {_Key, _Vals}} ->
            %% the line below is spammy but nice for debugging
            %%{ReqId, {Key, Vals}} ->
            %%lager:info("Got some values ~p, ~p", [Key, Vals]),
            wait_for_end(ReqId);
        {ReqId, Other} ->
            error({unexpected_message, Other})
    after 10000 ->
            error(error_stream_recv_timed_out)
    end.


put_buckets(Node, Num) ->
    Pid = rt_pb:pbc(Node),
    Buckets = [list_to_binary(["", integer_to_list(Ki)])
               || Ki <- lists:seq(0, Num - 1)],
    {Key, Val} = {<<"test_key">>, <<"test_value">>},
    [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val))
     || Bucket <- Buckets],
    riakc_pb_socket:stop(Pid).


put_keys(Node, Bucket, Num) ->
    Pid = rt_pb:pbc(Node),
    Keys = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
    Vals = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
    [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val)) || {Key, Val} <- lists:zip(Keys, Vals)],
    riakc_pb_socket:stop(Pid).
