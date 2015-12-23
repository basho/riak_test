%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(verify_write_once).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(NVAL, 2).
-define(BUCKET_TYPE, <<"write_once">>).
-define(BUCKET, {?BUCKET_TYPE, <<"bucket">>}).
-define(ASYNC_PUT_BUCKET_TYPE, <<"async_put">>).
-define(ASYNC_PUT_BUCKET, {?ASYNC_PUT_BUCKET_TYPE, <<"bucket">>}).
-define(ANY_VALUE, <<"any">>).


%% @doc This test exercises the write_once bucket property, which results in puts that avoid coordination
%%      and reads before writes, and which therefore have lower latency and higher throughput.
%%
confirm() ->
    %%
    %% Set two clusters.  We need one for most of the testing of this code path.
    %% The first cluster will use the memory back end.
    %% The second cluster will be a singleton cluster with the leveldb back end,
    %% in order to test asynchronous puts
    %%
    [Cluster1, Cluster2] = rt:deploy_clusters([
        {4, config(?DEFAULT_RING_SIZE, ?NVAL)},
        {1, config(?DEFAULT_RING_SIZE, ?NVAL, riak_kv_eleveldb_backend)}
    ]),
    rt:join_cluster(Cluster1),
    % rt:join_cluster(Cluster2),
    lager:info("Set up clusters: ~p, ~p", [Cluster1, Cluster2]),
    %%
    %% Select a random node, and use it to create an immutable bucket
    %%
    Node = lists:nth(random:uniform(length((Cluster1))), Cluster1),
    rt:create_and_activate_bucket_type(Node, ?BUCKET_TYPE, [{write_once, true}]),
    rt:wait_until_bucket_type_status(?BUCKET_TYPE, active, Cluster1),
    lager:info("Created ~p bucket type on ~p", [?BUCKET_TYPE, Node]),
    %%
    %%
    %%
    pass = confirm_put(Node),
    pass = confirm_w(Cluster1),
    pass = confirm_pw(Cluster1),
    pass = confirm_rww(Cluster1),
    pass = confirm_async_put(hd(Cluster2)),
    pass.

%%
%% private
%%


confirm_put(Node) ->
    ok = verify_put(Node, ?BUCKET, <<"confirm_put_key">>, <<"confirm_put_value">>),
    verify_failed_put(
        Node, ?BUCKET, <<"confirm_put-bad_w">>, ?ANY_VALUE, [{w, 9999}],
        fun(Error) ->
            ?assertMatch({n_val_violation, 3}, Error)
        end
    ),
    verify_failed_put(
        Node, ?BUCKET, <<"confirm_put-bad_pw">>, ?ANY_VALUE, [{pw, 9999}],
        fun(Error) ->
            ?assertMatch({n_val_violation, 3}, Error)
        end
    ),
    lager:info("confirm_put...ok"),
    pass.


confirm_w(Nodes) ->
    %%
    %% split the cluster into 2 paritions [dev1, dev2, dev3], [dev4]
    %%
    P1 = lists:sublist(Nodes, 3),
    P2 = lists:sublist(Nodes, 4, 1),
    PartitonInfo = rt:partition(P1, P2),
    [Node1 | _Rest1] = P1,
    verify_put(Node1, ?BUCKET, <<"confirm_w_key">>, <<"confirm_w_value">>),
    [Node2 | _Rest2] = P2,
    %%
    %% By setting sloppy_quorum to false, we require a strict quorum of primaries.  But because
    %% we only have one node in the partition, the put should fail.  It should bail immediately
    %% without even attempting a write on the back end, because a quorum will not be possible.
    %%
    verify_failed_put(
        Node2, ?BUCKET, <<"confirm_w_key">>, <<"confirm_w_value">>, [{sloppy_quorum, false}],
        fun(Error) ->
            ?assertMatch({insufficient_vnodes, _, need, 2}, Error)
        end
    ),
    rt:heal(PartitonInfo),
    lager:info("confirm_pw...ok"),
    pass.


confirm_pw(Nodes) ->
    %%
    %% split the cluster into 2 paritions [dev1, dev2, dev3], [dev4]
    %%
    P1 = lists:sublist(Nodes, 3),
    P2 = lists:sublist(Nodes, 4, 1),
    PartitonInfo = rt:partition(P1, P2),
    [Node1 | _Rest1] = P1,
    verify_put(Node1, ?BUCKET, <<"confirm_pw_key">>, <<"confirm_pw_value">>),
    [Node2 | _Rest2] = P2,
    %%
    %% Similar to the above test -- if pw is all, then we require n_val puts on primaries, but
    %% the node is a singleton in the partition, so this, too, should fail.  This will time
    %% out, so set the timeout to something small.
    %%
    verify_put_timeout(
        Node2, ?BUCKET, <<"confirm_pw_key">>, ?ANY_VALUE, [{pw, all}], 1000,
        fun(Error) ->
            ?assertMatch({pw_val_unsatisfied, 3, _}, Error)
        end
    ),
    rt:heal(PartitonInfo),
    lager:info("confirm_pw...ok"),
    pass.

confirm_rww(Nodes) ->
    %%
    %% split the cluster into 2 paritions
    %%
    P1 = lists:sublist(Nodes, 2),
    P2 = lists:sublist(Nodes, 3, 2),
    PartitonInfo = rt:partition(P1, P2),
    NumFastMerges = num_fast_merges(Nodes),
    %%
    %% put different values into each partiton
    %%
    [Node1 | _Rest1] = P1,
    verify_put(Node1, ?BUCKET, <<"confirm_rww_key">>, <<"confirm_rww_value1">>),
    [Node2 | _Rest2] = P2,
    verify_put(Node2, ?BUCKET, <<"confirm_rww_key">>, <<"confirm_rww_value2">>),
    %%
    %% After healing, both should agree on an arbitrary value
    %%
    rt:heal(PartitonInfo),
    rt:wait_until(fun() ->
        V1 = get(Node1, ?BUCKET, <<"confirm_rww_key">>),
        V2 = get(Node2, ?BUCKET, <<"confirm_rww_key">>),
        V1 =:= V2
    end),
    ?assert(NumFastMerges < num_fast_merges(Nodes)),
    lager:info("confirm_rww...ok"),
    pass.

%%
%% In order to test asynchronous puts, at this point we need a node with leveldb, as
%% that is currently the only back end that supports it.  In the future, we may add
%% async puts as a capability which can be arbitrated through the multi backend.
%%
confirm_async_put(Node) ->
    %%
    %% Set up the intercepts on the singleton node in cluster2
    %%
    make_intercepts_tab(Node),
    rt_intercept:add(
        Node, {riak_kv_vnode, [
            %% Count everytime riak_kv_vnode:handle_handoff_command/3 is called with a write_once message
            {{handle_command, 3}, count_w1c_handle_command}
        ]}
    ),
    %%
    %% Create the bucket type
    %%
    rt:create_and_activate_bucket_type(Node, ?ASYNC_PUT_BUCKET_TYPE, [{write_once, true}, {backend, myeleveldb}]),
    rt:wait_until_bucket_type_status(?ASYNC_PUT_BUCKET_TYPE, active, [Node]),
    lager:info("Created ~p bucket type on ~p", [?ASYNC_PUT_BUCKET_TYPE, Node]),
    %%
    %% Clear the intercept counters
    %%
    true = rpc:call(Node, ets, insert, [intercepts_tab, {w1c_async_replies, 0}]),
    true = rpc:call(Node, ets, insert, [intercepts_tab, {w1c_sync_replies, 0}]),

    ok = verify_put(Node, ?ASYNC_PUT_BUCKET, <<"confirm_async_put_key">>, <<"confirm_async_put_value">>),
    %%
    %% verify that we have handled 3 asynchronous writes and 0 synchronous writes
    %%
    [{_, W1CAsyncReplies}] = rpc:call(Node, ets, lookup, [intercepts_tab, w1c_async_replies]),
    [{_, W1CSyncReplies}]  = rpc:call(Node, ets, lookup, [intercepts_tab, w1c_sync_replies]),
    ?assertEqual(0, W1CSyncReplies),
    ?assertEqual(3, W1CAsyncReplies),
    %%
    %% reconfigure the node to force use of synchronous writes with leveldb
    %%
    rt:update_app_config(Node, [{riak_kv, [{allow_async_put, false}]}]),
    rt:start(Node),
    %%
    %% Set up the intercepts on the singleton node in cluster2
    %%
    make_intercepts_tab(Node),
    rt_intercept:add(
        Node, {riak_kv_vnode, [
            %% Count everytime riak_kv_vnode:handle_handoff_command/3 is called with a write_once message
            {{handle_command, 3}, count_w1c_handle_command}
        ]}
    ),
    %%
    %% Clear the intercept counters
    %%
    true = rpc:call(Node, ets, insert, [intercepts_tab, {w1c_async_replies, 0}]),
    true = rpc:call(Node, ets, insert, [intercepts_tab, {w1c_sync_replies, 0}]),

    ok = verify_put(Node, ?ASYNC_PUT_BUCKET, <<"confirm_async_put_key">>, <<"confirm_async_put_value">>),
    %%
    %% verify that we have handled 0 asynchronous writes and 3 synchronous writes, instead
    %%
    [{_, W1CAsyncReplies2}] = rpc:call(Node, ets, lookup, [intercepts_tab, w1c_async_replies]),
    [{_, W1CSyncReplies2}]  = rpc:call(Node, ets, lookup, [intercepts_tab, w1c_sync_replies]),
    ?assertEqual(3, W1CSyncReplies2),
    ?assertEqual(0, W1CAsyncReplies2),
    %%
    %% done!
    %%
    lager:info("confirm_async_put...ok"),
    pass.

verify_put(Node, Bucket, Key, Value) ->
    verify_put(Node, Bucket, Key, Value, [], Value).

verify_put(Node, Bucket, Key, Value, Options, ExpectedValue) ->
    Client = rt:pbc(Node),
    _Ret = riakc_pb_socket:put(
        Client, riakc_obj:new(
            Bucket, Key, Value
        ),
        Options
    ),
    {ok, Val} = riakc_pb_socket:get(Client, Bucket, Key),
    ?assertEqual(ExpectedValue, riakc_obj:get_value(Val)),
    ok.

verify_failed_put(Node, Bucket, Key, Value, Options, ExpectedPutReturnFunc) ->
    Client = rt:pbc(Node),
    {error, PutReturnValue} = riakc_pb_socket:put(
        Client, riakc_obj:new(
            Bucket, Key, Value
        ),
        Options
    ),
    ExpectedPutReturnFunc(parse(PutReturnValue)),
    ok.


verify_put_timeout(Node, Bucket, Key, Value, Options, Timeout, ExpectedPutReturnFunc) ->
    Client = rt:pbc(Node),
    {Time, {error, Val}} = timer:tc(
        fun() ->
            riakc_pb_socket:put(
                Client, riakc_obj:new(
                    Bucket, Key, Value
                ), [{timeout, Timeout} | Options]
            )
        end
    ),
    ExpectedPutReturnFunc(parse(Val)),
    ?assert(Time div 1000000 =< 2*Timeout),
    ok.

num_fast_merges(Nodes) ->
    lists:foldl(
        fun(Node, Acc) ->
            {write_once_merge, N} = proplists:lookup(
                write_once_merge,
                rt:rpc_call(Node, riak_kv_stat, get_stats, [])
            ),
            Acc + N
        end,
        0, Nodes
    ).

get(Node, Bucket, Key) ->
    Client = rt:pbc(Node),
    {ok, Val} = riakc_pb_socket:get(Client, Bucket, Key),
    riakc_obj:get_value(Val).

config(RingSize, NVal) ->
    config(RingSize, NVal, riak_kv_multi_backend).

config(RingSize, NVal, Backend) ->
    [
        {riak_core, [
            {default_bucket_props,
                [
                    {n_val, NVal},
                    {allow_mult, true},
                    {dvv_enabled, true}
                ]},
            {vnode_management_timer, 1000},
            {ring_creation_size, RingSize}]
        },
        {riak_kv, [
            {anti_entropy_build_limit, {100, 1000}},
            {anti_entropy_concurrency, 100},
            {anti_entropy_tick, 100},
            {anti_entropy, {on, []}},
            {anti_entropy_timeout, 5000},
            {storage_backend, Backend},
            {multi_backend, [
                {mymemory, riak_kv_memory_backend, []},
                {myeleveldb, riak_kv_eleveldb_backend, []}
            ]}
        ]}
    ].

parse(Binary) ->
    {ok, Tokens, _} = erl_scan:string(binary_to_list(Binary) ++ "."),
    {ok, Term} = erl_parse:parse_term(Tokens),
    Term.

make_intercepts_tab(Node) ->
    SupPid = rpc:call(Node, erlang, whereis, [sasl_safe_sup]),
    intercepts_tab = rpc:call(Node, ets, new, [intercepts_tab, [named_table,
        public, set, {heir, SupPid, {}}]]).
