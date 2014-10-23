-module(repl_fs_bench4).
-export([confirm/0]).
-export([loadgen/5]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% Use case
    %%
    %% 4 billion keys, N=3, across 512-vnodes, 6 nodes
    %% 4 billion / 512 * 3 = ~23 million keys per vnode
    %%
    %% Objects are 8k, 1% of objects are 8k - 40 MB
    %%
    %% Want to scale down for testing.
    %%
    %% c3.8xlarge machines has 640 GB of SSD space.
    %%
    %% Let's go with 16 ring size across 6 nodes, which
    %% will be 2 or 3 vnodes per node.
    %%
    %% Should be about 565 GB or so on nodes with 3 vnodes.
    %%
    %% Tunables
    %% -- max_fssource_cluster, max_fssource_node, max_fssink_node
    %% -- aae_bloom_num_keys, aae_bloom_rate
    %% -- fullsync_direct_limit, fullsync_direct_mode
    %% -- others?
    %%
    RingSize = 16,
    NumNodes = 6,
    KeysPerVN = 23000000,
    TotalKeys = KeysPerVN * RingSize,
    BigKeys = TotalKeys * 1 div 100,
    SmallKeys = TotalKeys - BigKeys,
    BaseSize = 8 * 1024,
    MaxSize = 40 * 1024 * 1024,

    MaxSourceCluster = 5,
    MaxSourceNode = 1,
    MaxSinkNode = 1,
    AAEBloomKeys = TotalKeys,
    AAEBloomRate = 0.01,
    FullsyncDirectLimit = 1000,
    FullsyncDirectMode = inline,

    Config = [{riak_core, [
                           {ssl_enabled, false},
                           {certfile, "./etc/certs/cert.pem"},
                           {keyfile, "./etc/certs/key.pem"},
                           {cacertdir, "./etc/certs/ca"},
                           {ring_creation_size, RingSize},
                           {default_bucket_props, [{n_val, 3},
                                                   {allow_mult, false}]}]},
              {riak_kv, [{anti_entropy, {on, []}},
                         {anti_entropy_build_limit, {100, 1000}},
                         {anti_entropy_concurrency, 100}]},
              {riak_repl, [{fullsync_strategy, aae},
                           {fullsync_direct_limit, FullsyncDirectLimit},
                           {fullsync_direct_mode, FullsyncDirectMode},
                           {fullsync_on_connect, false},
                           {fullsync_interval, disabled},
                           {aae_bloom_num_keys, AAEBloomKeys},
                           {aae_bloom_rate, AAEBloomRate},
                           {max_fssource_retries, infinity},
                           {max_fssource_cluster, MaxSourceCluster},
                           {max_fssource_node, MaxSourceNode},
                           {max_fssink_node, MaxSinkNode}]}],

    Clusters = rt:build_clusters([{NumNodes, Config},
                                  {NumNodes, Config}]),
    [ANodes, BNodes] = Clusters,

    %% load this module on all the nodes
    AllNodes = lists:append(Clusters),
    ok = rt:load_modules_on_nodes([?MODULE], AllNodes),

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

    WriteKeys = fun(Percent) ->
                        write_to_cluster(AFirst,
                                         0, SmallKeys * Percent div 100,
                                         <<"small">>,
                                         BaseSize),
                        write_to_cluster(AFirst,
                                         0, BigKeys * Percent div 100,
                                         <<"big">>,
                                         {BaseSize, MaxSize})
                end,

    %% Write keys and perform fullsync.
    WriteKeys(100),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {FullTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),
    lager:info("FullTime: ~p~n", [FullTime]),

    %% Rewrite first 10% keys and perform fullsync.
    WriteKeys(10),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime1, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),
    lager:info("DiffTime1: ~p~n", [DiffTime1]),

    %% Rewrite first 1% keys and perform fullsync.
    WriteKeys(1),
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
              [{TotalKeys, Times}]),
    pass.

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

write_to_cluster(Node, First, Last, Bucket, Size) ->
    Workers = 16,
    ok = rpc:call(Node, ?MODULE, loadgen, [Bucket, First, Last, Size, Workers]).

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
    Ranges = partition_range(FirstKey, LastKey, Workers),
    Gen = case Size of
              {_, _} ->
                  #gen{workers=Workers,
                       bucket=Bucket,
                       value=Size};
              _ ->
                  Bin = random_binary(Size),
                  #gen{workers=Workers,
                       bucket=Bucket,
                       value=Bin}
          end,
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
                                       value=MaybeValue}) ->
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
    Value = case MaybeValue of
                {MinSize, MaxSize} ->
                    Size = MinSize + random:uniform(MaxSize - MinSize),
                    random_binary(Size);
                _ ->
                    MaybeValue
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
