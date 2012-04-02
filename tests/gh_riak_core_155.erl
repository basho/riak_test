-module(gh_riak_core_155).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

gh_riak_core_155() ->
    [Node] = rt:build_cluster(1),
  
    %% Generate a valid preflist for our get requests
    rpc:call(Node, riak_core, wait_for_service, [riak_kv]),
    BKey = {<<"bucket">>, <<"value">>},
    DocIdx = riak_core_util:chash_std_keyfun(BKey),
    PL = rpc:call(Node, riak_core_apl, get_apl, [DocIdx, 3, riak_kv]),

    lager:info("Adding delayed start to app.config"),
    NewConfig = [{riak_core, [{delayed_start, 1000}]}],
    rt:update_app_config(Node, NewConfig),

    %% Restart node, add mocks that delay proxy startup, and issue gets.
    %% Gets will come in before proxies started, and should trigger crash.
    rt:stop(Node),
    rt:async_start(Node),
    rt:wait_until_pingable(Node),

    load_code(?MODULE, [Node]),
    load_code(meck, [Node]),
    load_code(meck_mod, [Node]),
    rpc:call(Node, ?MODULE, setup_mocks, []),

    lager:info("Installed mocks to delay riak_kv proxy startup"),
    lager:info("Issuing 10000 gets against ~p", [Node]),
    perform_gets(10000, Node, PL, BKey),

    lager:info("Verifying ~p has not crashed", [Node]),
    [begin
         ?assertEqual(pong, net_adm:ping(Node)),
         timer:sleep(1000)
     end || _ <- lists:seq(1,10)],

    lager:info("Test passed"),
    ok.

load_code(Module, Nodes) ->
    {Module, Bin, File} = code:get_object_code(Module),
    {_, []} = rpc:multicall(Nodes, code, load_binary, [Module, File, Bin]).

setup_mocks() ->
    application:start(lager),
    meck:new(riak_core_vnode_proxy_sup, [unstick, passthrough, no_link]),
    meck:expect(riak_core_vnode_proxy_sup, start_proxies,
                fun(Mod=riak_kv_vnode) ->
                        lager:info("Delaying start of riak_kv_vnode proxies"),
                        timer:sleep(3000),
                        meck:passthrough([Mod]);
                   (Mod) ->
                        meck:passthrough([Mod])
                end),
    lager:info("Installed mocks").

perform_gets(Count, Node, PL, BKey) ->
    rpc:call(Node, riak_kv_vnode, get, [PL, BKey, make_ref()]),
    perform_gets2(Count, Node, PL, BKey).

perform_gets2(0, _, _, _) ->
    ok;
perform_gets2(Count, Node, PL, BKey) ->
    rpc:call(Node, riak_kv_vnode, get, [PL, BKey, make_ref()], 1000),
    perform_gets(Count - 1, Node, PL, BKey).
