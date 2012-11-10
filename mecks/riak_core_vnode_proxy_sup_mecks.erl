-module(riak_core_vnode_proxy_sup_mecks).
-compile(export_all).
-include("meck.hrl").

delay_start_proxies(Ms) ->
    ?M_EXPECT(start_proxies,
              fun(Mod=riak_kv_vnode) ->
                      ?M_INFO("Delaying start of riak_kv_vnode proxies for ~pms",
                              [Ms]),
                      timer:sleep(Ms),
                      meck:passthrough([Mod]);
                 (Mod) ->
                      meck:passthrough([Mod])
              end).
