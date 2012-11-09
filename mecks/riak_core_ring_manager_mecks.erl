-module(riak_core_ring_manager_mecks).
-compile(export_all).

noop_ring_trans() ->
    meck:new(riak_core_ring_manager, [passthrough, unstick, no_link]),
    meck:expect(riak_core_ring_manager, ring_trans,
                fun(_, _) ->
                        error_logger:error_msg("someone tried to call ring_trans - doing nothing"),
                        not_changed
                end),
    %% verify
    not_changed = riak_core_ring_manager:ring_trans(o,o),
    error_logger:error_msg("riak_core_ring_manager:ring_trans will now be a noop"),
    ok.
