
-module(verify_wm_timeouts).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Node] = rt:build_cluster(1),
    rt:wait_until_pingable(Node),
    
    lager:info("setting up initial data and loading remote code"),
    _Prep = os:cmd(io_lib:format("curl -s -S ~s/buckets/foo/keys/bar "
                                "-X PUT -H 'content-type: text/plain' "
                                "-d 'foobarbaz\n'", 
                                [rt:http_url(Node)])),

    rt_intercept:add(Node, {riak_kv_get_fsm,
                            [{{prepare,2}, slow_prepare}]}),
    rt_intercept:add(Node, {riak_kv_put_fsm,
                            [{{prepare,2}, slow_prepare}]}),
    
    lager:info("testing GET timeout"),
    %%curl must be installed locally for this to work.
    GET = os:cmd(io_lib:format("curl -s -S ~s/buckets/foo/keys/bar "
                               "-H 'x-riak-timeout: 100'",
                               [rt:http_url(Node)])),
    ?assertEqual(GET, "request timed out\n"),
    
    lager:info("testing PUT timeout"),
    PUT = os:cmd(io_lib:format("curl -s -S ~s/buckets/foo/keys/fnord "
                               "-X PUT -H 'content-type: text/plain' "
                               "-H 'x-riak-timeout: 100' "
                               "-d 'foobarbazdasdasd\n'", 
                               [rt:http_url(Node)])),
    ?assertEqual(PUT, "request timed out\n"),

    lager:info("testing DELETE timeout"),
    DEL = os:cmd(io_lib:format("curl -s -S ~s/buckets/foo/keys/bar "
                               "-X DELETE -H 'x-riak-timeout: 100'",
                               [rt:http_url(Node)])),
    ?assertEqual(DEL, "request timed out\n"),

    lager:info("testing invalid timeout value"),
    INV = os:cmd(io_lib:format("curl -s -S ~s/buckets/foo/keys/bar "
                               "-H 'x-riak-timeout: asdasasdasd'",
                               [rt:http_url(Node)])),
    ?assertEqual(INV, "foobarbaz\n"),

    lager:info("testing GET still works before long timeout"),
    LGET = os:cmd(io_lib:format("curl -s -S ~s/buckets/foo/keys/bar "
                                "-H 'x-riak-timeout: 4000'",
                                [rt:http_url(Node)])),
    ?assertEqual(LGET, "foobarbaz\n"),
    
    pass.    




