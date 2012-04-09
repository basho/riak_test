-module(partition_repair).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/1,
             enable_search_hook/2,
             get_ring/1,
             join/2,
             update_app_config/2]).

-define(FMT(S, L), lists:flatten(io_lib:format(S, L))).

%% @doc This test verifies that partition repair successfully repairs
%% all data after it has wiped out by a simulated disk crash.
partition_repair() ->
    SpamDir = get_os_env("SPAM_DIR"),
    RingSize = list_to_integer(get_os_env("RING_SIZE", "16")),
    NVal = get_os_env("N_VAL", undefined),
    Bucket = <<"scotts_spam">>,

    lager:info("Build a cluster"),
    lager:info("riak_search enabled: true"),
    lager:info("ring_creation_size: ~p", [RingSize]),
    lager:info("riak_core vnode_inactivity_timeout 1000"),
    Conf = [
            {riak_core,
             [
              {ring_creation_size, RingSize},
              {vnode_inactivity_timeout, 1000}
             ]},
            {riak_search,
             [
              {enabled, true}
             ]}
            %% {lager,
            %%  [{handlers,
            %%    [{lager_file_backend,
            %%      [{"./log/console.log",debug,10485760,"$D0",5}]}]}]}
           ],

    Nodes = rt:build_cluster(4, Conf),

    case NVal of
        undefined ->
            ok;
        _ ->
            lager:info("Set n_val to ~p", [NVal]),
            set_search_schema_nval(Bucket, NVal)
    end,

    lager:info("Enable search hook"),
    enable_search_hook(hd(Nodes), Bucket),

    lager:info("Insert Scott's spam emails"),
    {ok, C} = riak:client_connect(hd(Nodes)),
    [put_file(C, Bucket, F) || F <- file_list(SpamDir)],

    lager:info("Stash ITFs for each partition"),
    %% need to load the module so riak can see the fold fun
    load_module_on_riak(Nodes, ?MODULE),
    Ring = get_ring(hd(Nodes)),
    Owners = riak_core_ring:all_owners(Ring),
    [stash_postings(Owner) || Owner <- Owners],

    lager:info("Emulate data loss, repair, verify correct data"),
    [kill_repair_verify(Owner) || Owner <- Owners].

kill_repair_verify({Partition, Node}) ->
    {ok, [Stash]} = file:consult(stash_path(Partition)),
    ExpectToVerify = dict:size(Stash),

    %% kill the partition data
    [Name, _] = string:tokens(atom_to_list(Node), "@"),
    Path = rt:config(rtdev_path) ++ "/dev/" ++ Name ++ "/data/merge_index/" ++ integer_to_list(Partition),
    lager:info("Killing data for ~p on ~p at ~p", [Partition, Node, Path]),
    ?assertCmd("rm -rf " ++ Path),

    %% force restart of vnode since some data is kept in memory
    lager:info("Restarting search vnode for ~p on ~p", [Partition, Node]),
    {ok, Pid} = rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
                         [Partition, riak_search_vnode]),
    ?assert(rpc:call(Node, erlang, exit, [Pid, kill])),
    ?assertNot(rpc:call(Node, erlang, is_process_alive, [Pid])),

    lager:info("Verify data is missing"),
    ?assertEqual(0, count_postings({Partition, Node})),

    %% repair the partition, ignore return for now
    lager:info("Invoking repair for ~p on ~p", [Partition, Node]),
    _Ignore = rpc:call(Node, riak_core_handoff_manager, add_repair, [Partition]),
    lager:info("return value of add_repair ~p", [_Ignore]),
    wait_for_repair({Partition, Node}),

    lager:info("Verify ~p on ~p is fully repaired", [Partition, Node]),
    Postings2 = get_postings({Partition, Node}),
    {Verified, NotFound} = dict:fold(verify(Postings2), {0, []}, Stash),
    case NotFound of
        [] -> ok;
        _ ->
            NF = rt:config(rtdev_path) ++ "/dev/postings_stash",
            NF2 = NF ++ "/" ++ integer_to_list(Partition) ++ ".notfound",
            ?assertEqual(ok, file:write_file(NF2, io_lib:format("~p.", [NotFound])))
    end,
    ?assertEqual(ExpectToVerify, Verified).

verify(PostingsAfterRepair) ->
    fun(IFT, StashedPostings, {Verified, NotFound}) ->
            StashedPosting={IFT, StashedPostings},
            case dict:find(IFT, PostingsAfterRepair) of
                error -> {Verified, [StashedPosting|NotFound]};
                {ok, RepairedPostings} ->
                    case lists:all(fun is_true/1,
                                   [lists:member(P, RepairedPostings)
                                    || P <- StashedPostings]) of
                        true -> {Verified+1, NotFound};
                        false -> {Verified, [StashedPosting|NotFound]}
                    end
            end
    end.

is_true(X) ->
    X == true.

count_postings({Partition, Node}) ->
    dict:size(get_postings({Partition, Node})).

get_postings({Partition, Node}) ->
    VMaster = riak_search_vnode_master,
    %% TODO: add compile time support for riak_test
    Req = {riak_core_fold_req_v1, fun stash/3, dict:new()},
    Postings = riak_core_vnode_master:sync_command({Partition, Node},
                                                   Req,
                                                   VMaster,
                                                   infinity),
    Postings.

stash_postings({Partition, Node}) ->
    File = stash_path(Partition),
    ?assertEqual(ok, filelib:ensure_dir(File)),
    lager:info("Stashing ~p at ~p to ~p", [Partition, Node, File]),
    Postings = get_postings({Partition, Node}),
    ?assertEqual(ok, file:write_file(File, io_lib:format("~p.", [Postings]))).

stash({_I,{_F,_T}}=K, _Postings=V, Stash) ->
    dict:append_list(K, V, Stash).

stash_path(Partition) ->
    Path = rt:config(rtdev_path) ++ "/dev/postings_stash",
    Path ++ "/" ++ integer_to_list(Partition) ++ ".stash".

file_list(Dir) ->
    filelib:wildcard(Dir ++ "/*").

wait_for_repair({Partition, Node}) ->
    Reply = rpc:call(Node, riak_core_handoff_manager, repair_status,
                     [Partition]),
    case Reply of
        no_repair -> ok;
        repair_in_progress ->
            timer:sleep(timer:seconds(1)),
            wait_for_repair({Partition, Node})
    end.

%%
%% STUFF TO MOVE TO rt?
%%
put_file(C, Bucket, File) ->
    K = list_to_binary(string:strip(os:cmd("basename " ++ File), right, $\n)),
    {ok, Val} = file:read_file(File),
    O = riak_object:new(Bucket, K, Val, "text/plain"),
    ?assertEqual(ok, C:put(O)).

get_os_env(Var) ->
    case get_os_env(Var, undefined) of
        undefined -> exit({os_env_var_undefined, Var});
        Value -> Value
    end.

get_os_env(Var, Default) ->
    case os:getenv(Var) of
        false -> Default;
        Value -> Value
    end.

load_module_on_riak(Nodes, Mod) ->
    {Mod, Bin, File} = code:get_object_code(Mod),
    [?assertEqual({module, Mod},
                  rpc:call(Node, code, load_binary, [Mod, File, Bin]))
     || Node <- Nodes].

-spec set_search_schema_nval(binary(), pos_integer()) -> ok.
set_search_schema_nval(Bucket, NVal) ->
    %% TODO: Search currently offers no easy way to pragmatically
    %% change a schema and save it.  This is because the external and
    %% internal formats of the schema are different.  The parser reads
    %% the external format and an internal representation is created
    %% which is then stored/access via `riak_search_config'.  Rather
    %% than allowing the internal format to be modified and set you
    %% must send the update in the external format.
    BucketStr = binary_to_list(Bucket),
    SearchCmd = ?FMT("~s/dev/dev1/bin/search-cmd", [rt:config(rtdev_path)]),
    GetSchema = ?FMT("~s show-schema ~s > current-schema",
                     [SearchCmd, BucketStr]),
    ModifyNVal = ?FMT("sed -E 's/n_val, [0-9]+/n_val, ~s/' "
                      "current-schema > new-schema",
                      [NVal]),
    SetSchema = ?FMT("~s set-schema ~s new-schema", [SearchCmd, BucketStr]),
    ClearCache = ?FMT("~s clear-schema-cache", [SearchCmd]),
    ?assertCmd(GetSchema),
    ?assertCmd(ModifyNVal),
    ?assertCmd(SetSchema),
    ?assertCmd(ClearCache).
