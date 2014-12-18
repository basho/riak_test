%% @doc Tests to ensure a stalling or blocking fssource process does not
%% cause status call to timeout. Useful for only 2.0 and up (and up is
%% a regression test).
-module(repl_fs_stat_caching).
-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").
-define(TEST_BUCKET, <<"repl_fs_stat_caching">>).

-export([confirm/0]).

confirm() ->
    {{SrcLead, SrcCluster}, {SinkLead, _SinkCluster}} = setup(),
    SinkPort = repl_util:get_cluster_mgr_port(SinkLead),
    repl_util:connect_cluster(SrcLead, "127.0.0.1", SinkPort),

    lager:info("Loading source cluster"),
    [] = repl_util:do_write(SrcLead, 1, 1000, ?TEST_BUCKET, 1),

    repl_util:enable_fullsync(SrcLead, "sink"),
    rpc:call(SrcLead, riak_repl_console, fullsync, [["start", "sink"]]),

    % and now, the actual test.
    % find a random fssource, suspend it, and then ensure we can get a
    % status.
    {ok, Suspended} = suspend_an_fs_source(SrcCluster),
    lager:info("Suspended: ~p", [Suspended]),
    {ok, Status} = rt:riak_repl(SrcLead, "status"),
    FailLine = "RPC to '" ++ atom_to_list(SrcLead) ++ "' failed: timeout\n",
    ?assertNotEqual(FailLine, Status),

    true = rpc:block_call(node(Suspended), erlang, resume_process, [Suspended]),

    pass.

setup() ->
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),
    NodeCount = rt_config:get(num_nodes, 6),

    lager:info("Deploy ~p nodes", [NodeCount]),
    Nodes = rt:deploy_nodes(NodeCount, cluster_conf(), [riak_kv, riak_repl]),
    SplitSize = NodeCount div 2,
    {SourceNodes, SinkNodes} = lists:split(SplitSize, Nodes),

    lager:info("making cluster Source from ~p", [SourceNodes]),
    repl_util:make_cluster(SourceNodes),

    lager:info("making cluster Sink from ~p", [SinkNodes]),
    repl_util:make_cluster(SinkNodes),

    SrcHead = hd(SourceNodes),
    SinkHead = hd(SinkNodes),
    repl_util:name_cluster(SrcHead, "source"),
    repl_util:name_cluster(SinkHead, "sink"),

    rt:wait_until_ring_converged(SourceNodes),
    rt:wait_until_ring_converged(SinkNodes),

    rt:wait_until_transfers_complete(SourceNodes),
    rt:wait_until_transfers_complete(SinkNodes),

    ok = repl_util:wait_until_leader_converge(SourceNodes),
    ok = repl_util:wait_until_leader_converge(SinkNodes),

    SourceLead = repl_util:get_leader(SrcHead),
    SinkLead = repl_util:get_leader(SinkHead),

    {{SourceLead, SourceNodes}, {SinkLead, SinkNodes}}.

cluster_conf() ->
    [
        {riak_repl, [
            {fullsync_on_connect, false},
            {fullsync_interval, disabled},
            {max_fssource_cluster, 3},
            {max_fssource_node, 1},
            {max_fssink_node, 20},
            {rtq_max_bytes, 1048576}
        ]}
    ].

suspend_an_fs_source([]) ->
    {error, no_nodes};

suspend_an_fs_source(Nodes) ->
    suspend_an_fs_source(Nodes, 10000).

suspend_an_fs_source([_Node | _Tail], 0) ->
    {error, tries_ran_out};

suspend_an_fs_source([Node | Tail], TriesLeft) ->
    Pids = rpc:call(Node, riak_repl2_fssource_sup, enabled, []),
    case maybe_suspend_an_fs_source(Node, Pids) of
        false ->
            suspend_an_fs_source(Tail ++ [Node], TriesLeft - 1);
        Pid ->
            {ok, Pid}
    end.

maybe_suspend_an_fs_source(_Node, []) ->
    false;

maybe_suspend_an_fs_source(Node, [{_Remote, Pid} | Tail]) ->
    case rpc:block_call(Node, erlang, suspend_process, [Pid]) of
        false ->
            maybe_suspend_an_fs_source(Node, Tail);
        true ->
            Pid
    end.
