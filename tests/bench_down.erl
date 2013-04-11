-module(bench_down).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt:config(rt_harness))).

confirm() ->
    csv("/tmp/values.csv", ["time","min","mean","p50","p90","p99","max"]),
    csv("/tmp/events.csv", ["time","event"]),
    %% Delay = 5000,
    Delay = 15000,
    %% io:format("~p~n", [code:which(lager)]),
    Vsn = master,
    %% Vsn = current,
    Count = 2,
    Config = [{riak_core, [{ring_creation_size, 1024}]},
              {riak_kv, [{storage_backend, riak_kv_memory_backend},
                         {anti_entropy,{off,[]}}]}],
    %% Config = [{riak_core, [{ring_creation_size, 256}]}],
    Nodes = rt:deploy_nodes(lists:duplicate(Count, {Vsn, Config})),
    [Node1, Node2|_] = Nodes,

    rt:load_modules_on_nodes([?MODULE], [Node1]),
    FakeSeen = fake_seen(100, 150),
    rpc:call(Node1, riak_core_ring_manager, ring_trans, [fun expand_seen/2, FakeSeen]),

    rt:build_cluster2(Nodes),
    _ = {Node1, Node2},
    %% spawn_wait(2, write(100, Node1)),
    %% Workers = 5,
    %% timer:sleep(60000),
    timer:sleep(Delay),
    Workers = 1,
    Ranges = partition_range(1, 1000000, Workers),
    %% io:format("Ranges: ~p~n", [Ranges]),
    init_elapsed(),
    event("start_load"),
    %% Pids = [],
    Pids =
        pmap(fun({Start, End}) ->
                     %% io:format("~p: Writing: ~p/~p~n", [self(), Start, End]),
                     write(Start, End, Node1)
                     %% io:format("~p: done writing~n", [self()])
             end, Ranges),
    timer:sleep(Delay),
    %% timer:sleep(60000),
    %% timer:sleep(5000),
%%    timer:sleep(180000),
    event("stop"),
    stop_and_wait(Node2),
    timer:sleep(Delay),
    event("mark_down"),
    rt:down(Node1, Node2),
    timer:sleep(Delay),
    event("start"),
    rt:start(Node2),
    rt:wait_until_nodes_ready(Nodes),
    rt:wait_until_ring_converged(Nodes),
    timer:sleep(Delay),
    event("done"),
    pmap_kill(Pids),
    %% _ = pmap_wait(Ranges),
    %% throw(done),
    ok.

stop_and_wait(Node) ->
    ?HARNESS:stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)).

%% write(Num, Node) ->
%%     fun() ->
%%             PB = rt:pbc(Node),
%%             write(0, Num, PB, [])
%%     end.

write(Start, End, Node) ->
    PB = rt:pbc(Node),
    NextWindow = next_window(os:timestamp()),
    write(Start, End, PB, NextWindow, []).

write(End, End, _, _, Acc) ->
    (Acc /= []) andalso value(report(Acc)),
    ok;
write(X, End, PB, NextWindow, Acc) ->
    T0 = os:timestamp(),
    R = rt:pbc_write(PB, <<"test">>, <<X:64/integer>>, <<X:64/integer>>),
    case R of
        ok ->
            ok;
        _ ->
            io:format("R: ~p~n", [R])
    end,
    T1 = os:timestamp(),
    Diff = timer:now_diff(T1, T0),
    Acc2 = [Diff|Acc],
    case T0 > NextWindow of
        true ->
            NextWindow2 =  next_window(T1),
            value(report(Acc)),
            write(X+1, End, PB, NextWindow2, []);
        false ->
            write(X+1, End, PB, NextWindow, Acc2)
    end.

next_window({Mega,Sec,Micro}) ->
    {Mega, Sec+1, Micro}.

loopfun(End, F) ->
    fun() ->
            loop(End, F)
    end.

loop(End, F) ->
    loop(0, End, F).

loop(End, End, _) ->
    ok;
loop(X, End, F) ->
    F(),
    loop(X+1, End, F).


spawn_wait(N, F) ->
    spawn_n(N, F),
    wait(N).

spawn_n(0, _) ->
    ok;
spawn_n(N, F) ->
    Self = self(),
    spawn_link(fun() ->
                       F(),
                       Self ! done
               end),
    spawn_n(N-1, F).

wait(0) ->
    ok;
wait(N) ->
    receive
        done ->
            wait(N-1)
    end.

random_binary(0, Bin) ->
    Bin;
random_binary(N, Bin) ->
    X = random:uniform(255),
    random_binary(N-1, <<Bin/binary, X:8/integer>>).

report(Values) ->
    Sorted = lists:sort(Values),
    T = list_to_tuple(Sorted),
    Min = element(1, T),
    Max = element(tuple_size(T), T),
    Sum = lists:sum(Sorted),
    Avg = Sum div tuple_size(T),
    %% [{min, Min},
    %%  {max, Max},
    %%  {avg, Avg}| percentiles(T, [0.5, 0.75, 0.9, 0.95, 0.99, 0.999])].
    [P50, P90, P99] = percentiles(T, [0.5, 0.9, 0.99]),
    [Min, Avg, P50, P90, P99, Max].
     
percentiles(T, Percentiles) ->
    N = tuple_size(T),
    [begin
         Element = round(Percentile * N),
         %% {Percentile, element(Element, T)}
         element(Element, T)
     end || Percentile <- Percentiles].

pmap(F, L) ->
    Parent = self(),
    {Pids, _} =
        lists:mapfoldl(
          fun(X, N) ->
                  Pid = spawn(fun() ->
                                      Parent ! {pmap, N, F(X)}
                              end),
                  {Pid, N+1}
          end, 0, L),
    Pids.

pmap_wait(L) ->
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

pmap_kill(Pids) ->
    [exit(Pid, kill) || Pid <- Pids].

partition(Items, Bins) ->
    Limit = div_ceiling(length(Items), Bins),
    partition(Items, 0, Limit, [], []).

partition([], _Count, _Limit, Current, Acc) ->
    lists:reverse([lists:reverse(Current)|Acc]);
partition([X|Rest], Count, Limit, Current, Acc) ->
    case Count >= Limit of
        true ->
            Acc2 = [lists:reverse(Current)|Acc],
            partition(Rest, 1, Limit, [X], Acc2);
        false ->
            partition(Rest, Count+1, Limit, [X|Current], Acc)
    end.

partition_range(Start, End, Num) ->
    Span = div_ceiling(End - Start, Num),
    [{RS, erlang:min(RS + Span - 1, End)} || RS <- lists:seq(Start, End, Span)].

div_ceiling(A, B) ->
    (A + B - 1) div B.

log(Term) ->
    io:format(">> ~p~n", [Term]),
    try
        file:write_file("/tmp/data", io_lib:format("~p,~n", [Term]), [append])
    catch _:_ ->
            ok
    end.

log(File, Term) ->
    io:format(">> ~p~n", [Term]),
    try
        file:write_file(File, io_lib:format("~p,~n", [Term]), [append])
    catch _:_ ->
            ok
    end.

ensure_list(L) when is_list(L) ->
    L;
ensure_list(X) ->
    [X].

csv(File, L) ->
    _ = File,
    io:format(">> ~p~n", [L]),
    %% io:format(">>> ~p~n", [to_csv(L)]),
    try
        ok
        %% file:write_file(File, to_csv(L) ++ [$\n], [append])
    catch _A:_B ->
            io:format("Error: ~p~n", [{_A, _B}]),
            ok
    end.

to_csv(L) ->
    [H|T] = L,
    HS = io_lib:format("~p", [H]),
    CSV = [io_lib:format(",~p", [X]) || X <- T],
    [HS|CSV].

event(Term) ->
    csv("/tmp/events.csv", [elapsed(), Term]).

value(Term) ->
    L = ensure_list(Term),
    csv("/tmp/values.csv", [elapsed()|L]).

init_elapsed() ->
    T0 = os:timestamp(),
    mochiglobal:put(t_start, T0).

elapsed() ->
    T0 = mochiglobal:get(t_start),
    Now = os:timestamp(),
    timer:now_diff(Now, T0) div 1000000.

fake_seen(Start, End) ->
    Nodes = [list_to_atom(lists:flatten(io_lib:format("dev~b@127.0.0.1", [I])))
             || I <- lists:seq(Start, End, 1)],
    VClock = lists:foldl(fun(Node, VC) ->
                                 vclock:increment(Node, VC)
                         end, vclock:fresh(), Nodes),
    [{Node, VClock} || Node <- Nodes].

expand_seen(Ring, FakeSeen) ->
    Seen = element(10, Ring),
    Seen2 = Seen ++ FakeSeen,
    Ring2 = setelement(10, Ring, Seen2),
    {new_ring, Ring2}.

