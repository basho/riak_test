%% -------------------------------------------------------------------
%%% @copyright (C) 2018, NHS Digital
%%% @doc
%%% riak_test for soft-limit vnode polling and put fsm routing
%%% see riak/1661 for details.
%%% @end

-module(verify_vnode_polling).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"bucket">>).
-define(KEY, <<"key">>).
-define(VALUE, <<"value">>).

-define(RING_SIZE, 8).

confirm() ->
    Conf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [{allow_mult, true},
                                                 {dvv_enabled, true},
                                                 {ring_creation_size, ?RING_SIZE},
                                                 {vnode_management_timer, 1000},
                                                 {handoff_concurrency, 100},
                                                 {vnode_inactivity_timeout, 1000}]}]}],

    [Node1|_] = rt:build_cluster(5, Conf),

    Preflist = rt:get_preflist(Node1, ?BUCKET, ?KEY),

    lager:info("Got preflist"),
    lager:info("Preflist ~p~n", [Preflist]),

    PBClient = rt:pbc(Node1),

    lager:info("Attempting to write key"),

    %% Write key and confirm error pw=2 unsatisfied
    rt:pbc_write(PBClient, ?BUCKET, ?KEY, ?VALUE),

    pass.
