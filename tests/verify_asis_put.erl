-module(verify_asis_put).
-include_lib("eunit/include/eunit.hrl").
-export([confirm/0]).

confirm() ->
    %% 1. Deploy two nodes
    [Node1, Node2] = rt_cluster:deploy_nodes(2),
    %% 2. With PBC
    lager:info("Put new object in ~p via PBC.", [Node1]),
    PB1 = rt_pb:pbc(Node1),
    PB2 = rt_pb:pbc(Node2),
    Obj1 = riakc_obj:new(<<"verify_asis_put">>, <<"1">>, <<"test">>, "text/plain"),
    %%    a. put in node 1
    %%    b. fetch from node 1 for vclock
    {ok, Obj1a} = riakc_pb_socket:put(PB1, Obj1, [return_body]),
    %%    c. put asis in node 2 
    %%    d. fetch from node 2, check vclock is same
    lager:info("Put object asis in ~p via PBC.", [Node2]),
    {ok, Obj1b} = riakc_pb_socket:put(PB2, Obj1a, [asis, return_body]),
    lager:info("Check vclock equality after asis put (PBC)."),
    ?assertEqual({vclock_equal, riakc_obj:vclock(Obj1a)},
                 {vclock_equal, riakc_obj:vclock(Obj1b)}),

    %% 3. Repeat with HTTP, nodes reversed
    lager:info("Put new object in ~p via HTTP.", [Node2]),
    HTTP1 = rt:httpc(Node1),
    HTTP2 = rt:httpc(Node2),
    Obj2 = riakc_obj:new(<<"verify_asis_put">>, <<"2">>, <<"test">>, "text/plain"),
    %%    a. put in node 2
    %%    b. fetch from node 2 for vclock
    {ok, Obj2a} = rhc:put(HTTP2, Obj2, [return_body]),
    %%    c. put asis in node 1    
    %%    d. fetch from node 1, check vclock is same
    lager:info("Put object asis in ~p via PBC.", [Node1]),
    {ok, Obj2b} = rhc:put(HTTP1, Obj2a, [asis, return_body]),
    lager:info("Check vclock equality after asis put (HTTP)."),
    ?assertEqual({vclock_equal, riakc_obj:vclock(Obj2a)},
                 {vclock_equal, riakc_obj:vclock(Obj2b)}),

    pass.
