
-module(verify_api_timeouts).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Node] = rt:build_cluster(1),
    rt:wait_until_pingable(Node),
    
    HC = rt:httpc(Node),
    lager:info("setting up initial data and loading remote code"),
    rt:httpc_write(HC, <<"foo">>, <<"bar">>, <<"foobarbaz\n">>),
    rt:httpc_write(HC, <<"foo">>, <<"bar2">>, <<"foobarbaz2\n">>),

    rt_intercept:add(Node, {riak_kv_get_fsm,
                            [{{prepare,2}, slow_prepare}]}),
    rt_intercept:add(Node, {riak_kv_put_fsm,
                            [{{prepare,2}, slow_prepare}]}),
    
    
    lager:info("testing HTTP API"),

    lager:info("testing GET timeout"),
    {error, Tup1} = rhc:get(HC, <<"foo">>, <<"bar">>, [{timeout, 100}]),
    {ok, "503", _, GET} = Tup1,
    ?assertEqual(GET, <<"request timed out\n">>),
    
    lager:info("testing PUT timeout"),
    {error, Tup2} = rhc:put(HC, riakc_obj:new(<<"foo">>, <<"bar">>,
                                              <<"getgetgetgetget\n">>),
                            [{timeout, 100}]),
    {ok, "503", _, PUT} = Tup2,
    ?assertEqual(PUT, <<"request timed out\n">>),
 
    lager:info("testing DELETE timeout"),
    {error, Tup3} = rhc:delete(HC, <<"foo">>, <<"bar">>, [{timeout, 100}]),
    {ok, "503", _, DEL} = Tup3,
    ?assertEqual(DEL, <<"request timed out\n">>),

    lager:info("testing invalid timeout value"),
    {error, Tup4} = rhc:get(HC, <<"foo">>, <<"bar">>, [{timeout, asdasdasd}]),
    {ok, "400", _, INV} = Tup4,
    ?assertEqual(INV, <<"Bad timeout value \"asdasdasd\", using 60000\n">>),

    lager:info("testing GET still works before long timeout"),
    {ok, O} = rhc:get(HC, <<"foo">>, <<"bar">>, [{timeout, 4000}]),

    %% either of these are potentially valid.
    case riakc_obj:get_value(O) of 
        <<"foobarbaz\n">> -> 
            lager:info("Original Value"),
            ok;
        <<"getgetgetgetget\n">> -> 
            lager:info("New Value"),
            ok;
        _ -> ?assertEqual(true, false)
    end,


    PC = rt:pbc(Node),

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
    try
        _ = riakc_pb_socket:get(PC, <<"foo">>, <<"bar2">>, 
                                [{timeout, asdasdasd}]),
        erlang:error("should never get here")
    catch
        error:badarg -> 
            ok;
        C:E -> 
            lager:error("~p:~p", [C,E])
    end,

    lager:info("testing GET still works before long timeout"),
    {ok, O2} = riakc_pb_socket:get(PC, <<"foo">>, <<"bar2">>, 
                                  [{timeout, 4000}]),

    %% either of these are potentially valid.
    case riakc_obj:get_value(O2) of  
        <<"get2get2get2get2get\n">> -> 
            lager:info("New Value"),
            ok;
        <<"foobarbaz2\n">> -> 
            lager:info("Original Value"),
            ok;
        _ -> ?assertEqual(true, false)
    end,


    pass.    




