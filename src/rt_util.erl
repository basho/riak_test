-module(rt_util).
-include_lib("eunit/include/eunit.hrl").

-type error() :: {error(), term()}.
-type result() :: ok | error().
-type wait_result() ::ok | {fail, term()}.

-type erl_rpc_result() :: {ok, term()} | {badrpc, term()}.
-type rt_rpc_result() :: {ok, term()} | rt_util:error().

-type release() :: string().
-type products() :: riak | riak_ee | riak_cs | riak_cs_ee.
-type version() :: {products(), release()}.
-type version_selector() :: atom() | version().

-export_type([error/0,
              products/0,
              release/0,
              result/0,
              rt_rpc_result/0,
              version/0,
              version_selector/0,
              wait_result/0]).

-export([add_deps/1,
         base_dir_for_version/2,
         ip_addr_to_string/1,
         maybe_append_when_not_endswith/2,
         maybe_call_funs/1,
         maybe_rpc_call/4,
         major_release/1,
         merge_configs/2,
         pmap/2,
         parse_release/1,
         term_serialized_form/1,
         version_to_string/1,
         wait_until/1,
         wait_until/2,
         wait_until/3,
         wait_until_no_pending_changes/2]).

-ifdef(TEST).
-export([setup_test_env/0,
         test_success_fun/0]).
-endif.

-spec add_deps(filelib:dirname()) -> ok.
add_deps(Path) ->
    lager:debug("Adding dep path ~p", [Path]),
    case file:list_dir(Path) of
        {ok, Deps} ->
            [code:add_path(lists:append([Path, "/", Dep, "/ebin"])) || Dep <- Deps],
            ok;
        {error, Reason} ->
            lager:error("Failed to add dep path ~p due to ~p.", [Path, Reason]),
            erlang:error(Reason)
    end.

-spec base_dir_for_version(filelib:dirname(), version()) -> filelib:dirname().
base_dir_for_version(RootPath, Version) ->
    filename:join(RootPath, version_to_string(Version)).

-spec ip_addr_to_string({pos_integer(), pos_integer(), pos_integer(), pos_integer()}) -> string().
ip_addr_to_string(IP) ->
    string:join([integer_to_list(X) || X <- tuple_to_list(IP)], ".").

-spec maybe_append_when_not_endswith(string(), string()) -> string().
maybe_append_when_not_endswith(String, Suffix) ->
    maybe_append_when_not_endswith(lists:suffix(Suffix, String), String, Suffix).

-spec maybe_append_when_not_endswith(boolean(), string(), string()) -> string().
maybe_append_when_not_endswith(true, String, _Suffix) ->
    String;
maybe_append_when_not_endswith(false, String, Suffix) ->
    String ++ Suffix.

-spec maybe_rpc_call(node(), module(), function(), [term()]) -> erl_rpc_result().
maybe_rpc_call(NodeName, Module, Function, Args) ->
    maybe_rpc_call(rpc:call(NodeName, Module, Function, Args)).

%% -spec maybe_call_funs([module(), function(), [term()]]) -> term().
maybe_call_funs(CallSpecs) ->
    lists:foldl(fun([Module, Function, Args], PrevResult) ->
                        maybe_call_fun(PrevResult, Module, Function, Args)
                end, ok, CallSpecs).

-spec maybe_call_fun(ok | {ok, term()} | rt_util:error(), module(), function(), [term()]) -> term().
maybe_call_fun(ok, Module, Function, Args) ->
    erlang:apply(Module, Function, Args);
maybe_call_fun({ok, _}, Module, Function, Args) ->
    erlang:apply(Module, Function, Args);
maybe_call_fun(true, Module, Function, Args) ->
    erlang:apply(Module, Function, Args);
maybe_call_fun(Error, Module, Function, Args) ->
    lager:debug("~p:~p(~p) not called due error ~p", [Module, Function, Args, Error]),
    Error.

-spec maybe_rpc_call(erl_rpc_result()) -> rt_rpc_result().
maybe_rpc_call({badrpc, _}) ->
    {error, badrpc};
maybe_rpc_call(Result) ->
    Result.

-spec merge_configs(proplists:proplist() | tuple(), proplists:proplist() | tuple()) -> orddict:orddict() | tuple().
merge_configs(PropList, ThatPropList) when is_list(PropList) and is_list(ThatPropList) ->
    MergeA = orddict:from_list(PropList),
    MergeB = orddict:from_list(ThatPropList),
    orddict:merge(fun(_, VarsA, VarsB) ->
                          merge_configs(VarsA, VarsB)
                  end, MergeA, MergeB);
merge_configs(_, Value) ->
    Value.

-spec major_release(version() | release()) -> pos_integer().
major_release({_, Release}) ->
    major_release(Release);
major_release(Release) ->
    {Major, _, _} = parse_release(Release),
    Major.

%% @doc Parallel Map: Runs function F for each item in list L, then
%%      returns the list of results
-spec pmap(F :: fun(), L :: list()) -> list().
pmap(F, L) ->
    Parent = self(),
    lists:foldl(
    fun(X, N) ->
          spawn_link(fun() ->
                        Parent ! {pmap, N, F(X)}
                end),
          N+1
    end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

-spec parse_release(version() | release()) -> { pos_integer(), pos_integer(), pos_integer() }.
parse_release({_, Release}) ->
    parse_release(Release);
parse_release(Release) ->
    list_to_tuple([list_to_integer(Token) || Token <- string:tokens(Release, ".")]).

-spec term_serialized_form(term()) -> string().
term_serialized_form(Term) ->
    io_lib:format("~p.", [Term]).

-spec version_to_string(version()) -> string().
version_to_string({Product, Release}) ->
    string:join([atom_to_list(Product), Release], "-").

%% @doc Utility function used to construct test predicates. Retries the
%%      function `Fun' until it returns `true', or until the maximum
%%      number of retries is reached. The retry limit is based on the
%%      provided `rt_max_receive_wait_time' and `rt_retry_delay' parameters in
%%      specified `riak_test' config file.
%%
%% @since 1.1.0
-spec wait_until(function()) -> wait_result().
wait_until(Fun) when is_function(Fun) ->
    MaxTime = rt_config:get(rt_max_receive_wait_time),
    Delay = rt_config:get(rt_retry_delay),
    Retry = MaxTime div Delay,
    wait_until(Fun, Retry, Delay).

%% @doc Convenience wrapper for wait_until for the myriad functions that
%% take a node as single argument.
%%
%% @since 1.1.0
-spec wait_until(node(), function()) -> wait_result().
wait_until(Node, Fun) when is_atom(Node), is_function(Fun) ->
    wait_until(fun() -> Fun(Node) end).

%% @doc Retry `Fun' until it returns `Retry' times, waiting `Delay'
%% milliseconds between retries. This is our eventual consistency bread
%% and butter
%%
%% @since 1.1.0
-spec wait_until(function(), pos_integer(), pos_integer()) -> wait_result().
wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
    true ->
        ok;
    _ when Retry == 1 ->
        {fail, Res};
    _ ->
        timer:sleep(Delay),
        wait_until(Fun, Retry-1, Delay)
    end.

-ifdef(TEST).

-spec wait_until_no_pending_changes(rt_host:host(), [node()]) -> ok | fail.
wait_until_no_pending_changes(Host, Nodes) ->
    lager:info("Wait until no pending changes for nodes ~p on ~p", [Nodes, Host]),
    F = fun() ->
            rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
            {Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
            Changes = [ riak_core_ring:pending_changes(Ring) =:= [] || {ok, Ring} <- Rings ],
            BadNodes =:= [] andalso length(Changes) =:= length(Nodes) andalso lists:all(fun(T) -> T end, Changes)
    end,
    wait_until(F).

test_success_fun() ->
    ok.

setup_test_env() ->
    application:ensure_started(exec),
    rt_config:set(rt_max_receive_wait_time, 600000),
    rt_config:set(rt_retry_delay, 1000),

    %% TODO Consider loading up the riak_test.config to get this information
    add_deps(filename:join([os:getenv("HOME"), "rt", ".riak-builds", "riak_ee-head", "deps"])),

    {ok, _} = exec:run("epmd -daemon", [sync, stdout, stderr]),
    net_kernel:start(['riak_test@127.0.0.1']),
    erlang:set_cookie(node(), riak).

base_dir_for_version_test_() ->
    [?_assertEqual("foo/riak_ee-2.0.5", base_dir_for_version("foo", {riak_ee, "2.0.5"}))].

%% TODO Refactor into an EQC model ... 
ip_addr_to_string_test_() ->
    [?_assertEqual("127.0.0.1", ip_addr_to_string({127, 0, 0, 1}))].

maybe_append_when_not_endswith_test_() ->
    [?_assertEqual("foobar", maybe_append_when_not_endswith("foobar", "bar")),
     ?_assertEqual("foobar", maybe_append_when_not_endswith("foo", "bar"))].

maybe_call_fun_test_() ->
    [?_assertEqual(ok, maybe_call_fun(ok, ?MODULE, test_success_fun, [])),
     ?_assertEqual({error, test_failure}, maybe_call_fun({error, test_failure}, ?MODULE, test_success_fun, [])) ].

%% TODO Refactor into an EQC model ... 
major_version_test_() ->
    [?_assertEqual(1, major_release("1.3.4")),
     ?_assertEqual(1, major_release({riak_ee, "1.3.4"}))].

%% TODO Refactor into an EQC model ... 
merge_configs_test_() ->
    [?_assertEqual([{a,1},{b,2},{c,3},{d,4}], merge_configs([{a,1},{b,2}],[{c,3},{d,4}])),
     ?_assertEqual([{a,1},{b,3},{c,3},{d,4}], merge_configs([{a,1},{b,2}],[{b,3},{c,3},{d,4}])),
     ?_assertEqual([{a, [{b,3}, {c,5}, {d,6}]}, {e,7}], merge_configs([{a, [{b,2}, {c,5}]}], [{a, [{b,3}, {d,6}]}, {e,7}]))].

%% TODO Refactor into an EQC model ... 
parse_release_test_() ->
    [?_assertEqual({1, 3, 4}, parse_release("1.3.4")),
     ?_assertEqual({1, 3, 4}, parse_release({riak_ee, "1.3.4"}))].

version_to_string_test_() ->
    [?_assertEqual("riak_ee-2.0.5", version_to_string({riak_ee, "2.0.5"}))].

-endif.
