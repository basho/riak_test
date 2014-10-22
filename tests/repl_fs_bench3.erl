-module(repl_fs_bench3).
-export([confirm/0]).
-export([loadgen/5]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-compile({parse_transform, rt_intercept_pt}).

%% -define(NUM_KEYS, 100000).
%% -define(NUM_KEYS, 1000000).
%% -define(NUM_KEYS, 10000).
-define(TEST_BUCKET, <<"repl_bench">>).

confirm() ->
    %% Config = [{riak_core, [{ring_creation_size, 8},
    %%                        {default_bucket_props, [{n_val, 1},
    %%                                                {allow_mult, false}]}]},
    %%           {riak_kv, [{anti_entropy, {on, []}},
    %%                      {anti_entropy_build_limit, {100, 1000}},
    %%                      {anti_entropy_concurrency, 100}]},
    %%           {riak_repl, [{fullsync_strategy, Strategy},
    %%                        {fullsync_pipeline, Pipeline},
    %%                        {fullsync_direct, Direct},
    %%                        {fullsync_on_connect, false},
    %%                        {fullsync_interval, disabled},
    %%                        {max_fssource_retries, infinity},
    %%                        {max_fssource_cluster, 1},
    %%                        {max_fssource_node, 1},
    %%                        {max_fssink_node, 1}]}],
    %% deploy_certs(2),

    %% Strategy = keylist,
    %% NumKeys = 10000,
    %% Strategy = aae,
    %% Pipeline = true,
    %% Direct = 100,

    NumKeys = list_to_integer(rt_config:get(fs_num_keys)),
    Strategy = list_to_atom(rt_config:get(fs_strategy)),
    Pipeline = true,
    Direct = list_to_integer(rt_config:get(fs_direct)),
    NoPool = list_to_atom(rt_config:get(fs_no_pool, "false")),

    Config = [{riak_core, [
                           {ssl_enabled, false},
                           {certfile, "./etc/certs/cert.pem"},
                           {keyfile, "./etc/certs/key.pem"},
                           {cacertdir, "./etc/certs/ca"},
                           {ring_creation_size, 64},
                           {default_bucket_props, [{n_val, 3},
                                                   {allow_mult, false}]}]},
              {riak_kv, [{anti_entropy, {on, []}},
                         {anti_entropy_build_limit, {100, 1000}},
                         {anti_entropy_concurrency, 100}]},
              {riak_repl, [{fullsync_strategy, Strategy},
                           {fullsync_pipeline, Pipeline},
                           {fullsync_direct, Direct},
                           {fullsync_on_connect, false},
                           {fullsync_interval, disabled},
                           {max_fssource_retries, infinity},
                           {max_fssource_cluster, 1},
                           {max_fssource_node, 1},
                           {max_fssink_node, 1}]}],

    Clusters = rt:build_clusters([{5, Config},
                                  {5, Config}]),
    [ANodes, BNodes] = Clusters,

    %% load this module on all the nodes
    AllNodes = lists:append(Clusters),
    ok = rt:load_modules_on_nodes([?MODULE], AllNodes),
    NoPool andalso setup_intercepts(AllNodes),

    lists:foldl(fun(Nodes, N) ->
                        io:format("---~nCluster ~b: ~p~n", [N, Nodes]),
                        rpc:call(hd(Nodes), riak_core_console, member_status, [[]]),
                        N+1
                end, 1, Clusters),

    lager:info("Nodes: ~p", [[ANodes, BNodes]]),
    %% Nodes = lists:append(ANodes, BNodes),
    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),

    %% setup_nat(ANodes),
    %% setup_nat(BNodes),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),
    rpc:call(AFirst, riak_core_ring_manager, force_update, []),
    rt:wait_until_ring_converged(ANodes),
    rpc:call(BFirst, riak_core_ring_manager, force_update, []),
    rt:wait_until_ring_converged(BNodes),

    LeaderA = rpc:call(AFirst,
                       riak_core_cluster_mgr, get_leader, []),

    {ok, {IP, Port}} = rpc:call(BFirst,
                                application, get_env, [riak_core, cluster_mgr]),
    %% IP = rtssh:get_public_ip(BFirst),
    IP = rtssh:get_private_ip(BFirst),

    lager:info("Connecting ~p to ~p:~p", [LeaderA, IP, Port]), 
    repl_util:connect_cluster(LeaderA, IP, Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    %% Perform fullsync of an empty cluster.
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {EmptyTime, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),
    lager:info("EmptyTime: ~p~n", [EmptyTime]),

    %% Write keys and perform fullsync.
    %% repl_util:write_to_cluster(AFirst, 0, NumKeys, ?TEST_BUCKET),
    write_to_cluster(AFirst, 0, NumKeys, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {FullTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),
    lager:info("FullTime: ~p~n", [FullTime]),

    %% Rewrite first 10% keys and perform fullsync.
    write_to_cluster(AFirst, 0, NumKeys div 10, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime1, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),
    lager:info("DiffTime1: ~p~n", [DiffTime1]),

    %% Rewrite first 1% keys and perform fullsync.
    write_to_cluster(AFirst, 0, NumKeys div 100, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime2, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),
    lager:info("DiffTime2: ~p~n", [DiffTime2]),

    %% Write no keys, and perform the fullsync.
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {NoneTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),
    lager:info("NoneTime: ~p~n", [NoneTime]),

    Times = {EmptyTime, FullTime, DiffTime1, DiffTime2, NoneTime},
    io:format("==================================================~n"
              "~p~n"
              "==================================================~n",
              [{NumKeys, {Strategy, Pipeline, Direct}, Times}]),
    Out = io_lib:format("~p~n", [{NumKeys, {Strategy, Pipeline, Direct}, Times}]),
    file:write_file("RESULTS", Out, [append, raw]),
    pass.

setup_nat(Nodes) ->
    First = hd(Nodes),
    _ = [begin
             Public = rtssh:get_public_ip(Node),
             Private = rtssh:get_private_ip(Node),
             case Public == Private of
                 true ->
                     ok;
                 false ->
                     ok = rpc:call(First, riak_repl_console, add_nat_map, [[Public, Private]])
             end
         end || Node <- Nodes],
    ok.

deploy_certs(Num) ->
    Clusters = rt_config:get(rtssh_clusters, []),
    case length(Clusters) < Num of
        true ->
            erlang:error("Requested more clusters than available");
        false ->
            Sites = ["site" ++ integer_to_list(X) ++ ".basho.com" || X <- lists:seq(1, Num)],
            CertDir = rt_config:get(rt_scratch_dir) ++ "/certs",
            make_certs:rootCA(CertDir, "rootCA"),
            %% make_certs:intermediateCA(CertDir, "intCA", "rootCA"),
            make_certs:endusers(CertDir, "rootCA", Sites),
            %% Riak before 2.0 required directory to hold cacerts.pem
            {ok, Dirs} = file:list_dir(CertDir),
            [begin
                 CACerts = filename:join([CertDir, Dir, "cacerts.pem"]),
                 case filelib:is_regular(CACerts) of
                     true ->
                         CADir = filename:join([CertDir, Dir, "ca"]),
                         Copy = filename:join(CADir, "cacerts.pem"),
                         file:make_dir(CADir),
                         {ok, _} = file:copy(CACerts, Copy);
                     _ ->
                         ok
                 end
             end || Dir <- Dirs],
            Both = lists:zip(lists:sublist(Clusters, Num), Sites),
            lager:info("Sites: ~p", [Both]),
            _ = [begin
                     rt:pmap(fun(Host) ->
                                     Path = filename:join(CertDir, Site),
                                     _ = [begin
                                              %% Etc = NodePath ++ "/etc/",
                                              Dest = filename:join(Etc, "certs"),
                                              rtssh:ssh_cmd(Host, "mkdir -p " ++ Dest),
                                              Cmd = "rsync -tr " ++ Path ++ "/* " ++ Host ++ ":" ++ Dest,
                                              Result = rtssh:cmd(Cmd),
                                              lager:info("Syncing site certs :: ~p :: ~p :: ~p~n", [Host, Cmd, Result])
                                          end || DevPath <- rtssh:devpaths(),
                                                 Etc <- rtssh:all_the_files(Host, DevPath, "etc")],
                                     ok
                             end, Hosts)
                 end || {{_, Hosts}, Site} <- Both],
            ok
            %% Sites: [{{1,["riak101.priv"]},"site1.basho.com"},{{2,["riak201.priv"]},"site2.basho.com"}]
    end,
    pass.

deploy_certs2(Num) ->
    Clusters = rt_config:get(rtssh_clusters, []),
    case length(Clusters) < Num of
        true ->
            erlang:error("Requested more clusters than available");
        false ->
            Sites = ["site" ++ integer_to_list(X) || X <- lists:seq(1, Num)],
            PrivDir = rt:priv_dir(),
            CertDir = filename:join([PrivDir, "certs", "selfsigned"]),
            CA = filename:join(CertDir, "ca"),
            Both = lists:zip(lists:sublist(Clusters, Num), Sites),
            lager:info("Sites: ~p", [Both]),
            _ = [begin
                     rt:pmap(fun(Host) ->
                                     SiteCert = filename:join([CertDir, Site ++ "-cert.pem"]),
                                     SiteKey = filename:join([CertDir, Site ++ "-key.pem"]),
                                     _ = [begin
                                              %% Etc = NodePath ++ "/etc/",
                                              lager:info("Syncing site certs :: ~p", [Host]),
                                              Dest = filename:join(Etc, "certs"),
                                              ok = rsync(Host, CA, Dest),
                                              ok = rsync(Host, SiteCert, Dest ++ "/cert.pem"),
                                              ok = rsync(Host, SiteKey, Dest ++ "/key.pem"),
                                              %% rtssh:ssh_cmd(Host, "mkdir -p " ++ Dest),
                                              %% Cmd = "rsync -tr " ++ CA ++ " " ++ Host ++ ":" ++ Dest,
                                              %% Result = rtssh:cmd(Cmd),
                                              %% lager:info("Syncing site certs :: ~p :: ~p :: ~p~n", [Host, Cmd, Result]),
                                              ok
                                          end || DevPath <- rtssh:devpaths(),
                                                 Etc <- rtssh:all_the_files(Host, DevPath, "etc")],
                                     ok
                             end, Hosts)
                 end || {{_, Hosts}, Site} <- Both],
            ok
            %% Sites: [{{1,["riak101.priv"]},"site1.basho.com"},{{2,["riak201.priv"]},"site2.basho.com"}]
    end,
    pass.

rsync(Host, Local, Remote) ->
    Cmd = "rsync -tr " ++ Local ++ " " ++ Host ++ ":" ++ Remote,
    Result = rtssh:cmd(Cmd),
    lager:info("rsync :: ~p :: ~p :: ~p", [Host, Cmd, Result]),
    ok.

write_to_cluster(Node, First, Last, Bucket) ->
    Workers = 16,
    Size = 64,
    ok = rpc:call(Node, ?MODULE, loadgen, [Bucket, First, Last, Size, Workers]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Intercepts
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(MSG_INIT, 1).
-define(MSG_LOCK_TREE, 2).
-define(MSG_UPDATE_TREE, 3).
-define(MSG_GET_AAE_BUCKET, 4).
-define(MSG_GET_AAE_SEGMENT, 5).
-define(MSG_REPLY, 6).
-define(MSG_PUT_OBJ, 7).
-define(MSG_GET_OBJ, 8).
-define(MSG_COMPLETE, 9).

setup_intercepts(Nodes) ->
    _ = [rt_intercept:load_code(N) || N <- Nodes],
    _ = [setup_node_intercepts(Node) || Node <- Nodes],
    ok.

setup_node_intercepts(Node) ->
    rt_intercept:add(Node, {riak_repl_aae_sink, [{{process_msg, 3},
        {[],
         fun(Msg, Args, State) ->
                 ?MODULE:process_msg_int(Msg, Args, State)
         end}}]}).
         %% fun(?MSG_PUT_OBJ, {fs_diff_obj, BObj}, State) ->
         %%         RObj = riak_repl_util:from_wire(BObj),
         %%         riak_repl_util:do_repl_put(RObj),
         %%         {noreply, State};
         %%    (Msg, Args, State) ->
         %%         riak_repl_aae_sink_orig:process_msg(Msg, Args, State)
         %% end}}]}).

process_msg_int(?MSG_PUT_OBJ, {fs_diff_obj, BObj}, State) ->
    RObj = riak_repl_util:from_wire(BObj),
    riak_repl_util:do_repl_put(RObj),
    {noreply, State};
process_msg_int(Msg, Args, State) ->
    riak_repl_aae_sink_orig:process_msg_orig(Msg, Args, State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Load generator
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(gen, {workers :: integer(),
              bucket  :: binary(),
              value   :: binary()}).

%% tt() ->
%%     go(120000*8*5, 1024*1024).

loadgen(_Bucket, FirstKey, LastKey, _Size, _Workers) when FirstKey >= LastKey ->
    ok;
loadgen(Bucket, FirstKey, LastKey, Size, Workers) ->
    Bin = random_binary(Size),
    Ranges = partition_range(FirstKey, LastKey, Workers),
    Gen = #gen{workers=Workers,
               bucket=Bucket,
               value=Bin},
    pmap(fun({Start, Stop}) ->
                 io:format("Starting worker ~p: ~p/~p~n", [self(), Start, Stop]),
                 {ok, C} = riak:local_client(),
                 worker(Start, Stop, C, 0, os:timestamp(), Gen)
         end, Ranges),
    ok.

worker(Same, Same, _C, _Count, _T0, _Gen) ->
    ok;
worker(X, Stop, C, Count, T0, Gen=#gen{workers=Workers,
                                       bucket=Bucket,
                                       value=Value}) ->
    if Count > 10000 ->
            T1 = os:timestamp(),
            TD = timer:now_diff(T1, T0),
            Rate = Count * 1000000 div TD,
            io:format("~p (~p)~n", [Rate, Rate * Workers]),
            Count2 = 0,
            Time = T1;
       true ->
            Count2 = Count + 1,
            Time = T0
    end,
    case C:put(riak_object:new(Bucket, <<X:64/integer>>, Value),1) of
        ok ->
            worker(X + 1, Stop, C, Count2, Time, Gen);
        {error, overload} ->
            worker(X + 1, Stop, C, Count2, Time, Gen);
        {error, timeout} ->
            worker(X, Stop, C, Count2, Time, Gen)
    end.

random_binary(N) ->
    random_binary(N, []).

random_binary(N, Acc) when N =< 0 ->
    iolist_to_binary(Acc);
random_binary(N, Acc) ->
    X = random:uniform(1 bsl 127),
    case <<X:128/integer>> of
        <<Bin:N/binary, _/binary>> ->
            random_binary(N-16, [Bin|Acc]);
        Bin ->
            random_binary(N-16, [Bin|Acc])
    end.

pmap(F, L) ->
    Parent = self(),
    {_Pids, _} =
        lists:mapfoldl(
          fun(X, N) ->
                  Pid = spawn_link(fun() ->
                                           Parent ! {pmap, N, F(X)}
                                   end),
                  {Pid, N+1}
          end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

partition_range(Start, End, 1) ->
    [{Start, End}];
partition_range(Start, End, Num) ->
    Span = div_ceiling(End - Start, Num),
    [{RS, erlang:min(RS + Span - 1, End)} || RS <- lists:seq(Start, End, Span)].

div_ceiling(A, B) ->
    (A + B - 1) div B.
