-module(riak_core_ring_manager_mecks).
-compile(export_all).
-include("meck.hrl").

noop_ring_trans() ->
    ?M_EXPECT(ring_trans,
              fun(_, _) ->
                      error_logger:error_msg("ring_trans is now a noop"),
                      not_changed
              end),
    error_logger:error_msg("ring_trans will now be a noop"),
    ok.
