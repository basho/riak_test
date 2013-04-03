-module(bench_down).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% io:format("~p~n", [code:which(lager)]),
    Vsn = master,
    Count = 4,
    Config = [{riak_core, [{ring_creation_size, 16}]}],
    %% Config = [{riak_core, [{ring_creation_size, 256}]}],
    Nodes = rt:build_cluster(lists:duplicate(Count, {Vsn, Config})),
    [Node1, Node2|_] = Nodes,
    _ = {Node1, Node2},
    spawn_wait(2, write(100, Node1)),
    %% stop_and_wait(Node2),
    %% rt:down(Node1, Node2),
    %% %% timer:sleep(10000),
    %% rt:start(Node2),
    %% rt:wait_until_nodes_ready(Nodes),
    throw(done),
    ok.

stop_and_wait(Node) ->
    rtdev:stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)).

write(Num, Node) ->
    PB = rt:pbc(Node),
    write(0, Num, PB).

write(End, End, _) ->
    ok;
write(X, End, PB) ->
    rt:pbc_write(PB, <<"test">>, <<X:64/integer>>, <<X:64/integer>>),
    write(X+1, End, PB).

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
