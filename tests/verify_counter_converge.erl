%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2012, Basho Technologies
%%% @doc
%%% riak_test for riak_dt counter convergence,
%%% @end

-module(verify_counter_converge).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    inets:start(),
    Key = a,

    Config = [
            {riak_core,
             [
              {handoff_manager_timeout, 1000},
              {vnode_management_timer, 1000},
              {vnode_inactivity_timeout, 1000}
             ]}
           ],

    [N1, N2, N3, N4]=Nodes = rt:build_cluster(4, Config),
    [HP1, HP2, HP3, HP4]=Hosts=  get_host_ports(Nodes),

    increment_counter(HP1, Key),
    increment_counter(HP2, Key, 10),

    [?assertEqual(11, get_counter(HP, Key)) || HP <- Hosts],

    decrement_counter(HP3, Key),
    decrement_counter(HP4, Key, 2),

    [?assertEqual(8, get_counter(HP, Key)) || HP <- Hosts],

    lager:debug("Partition cluster in two."),

    PartInfo = rt:partition([N1, N2], [N3, N4]),

    %% increment one side
    increment_counter(HP1, Key, 5),

    %% check vaue on one side is different from other
    [?assertEqual(13, get_counter(HP, Key)) || HP <- [HP1, HP2]],
    [?assertEqual(8, get_counter(HP, Key)) || HP <- [HP3, HP4]],

    %% decrement other side
    decrement_counter(HP3, Key, 2),

    %% verify values differ
    [?assertEqual(13, get_counter(HP, Key)) || HP <- [HP2, HP2]],
    [?assertEqual(6, get_counter(HP, Key)) || HP <- [HP3, HP4]],

    %% heal
    ok = rt:heal(PartInfo),
    lager:debug("Heal and check merged values"),
    timer:sleep(1000),

    %% verify all nodes agree
    [?assertEqual(11, get_counter(HP, Key)) || HP <- Hosts],

    pass.

get_host_ports(Nodes) ->
    {ResL, []} = rpc:multicall(Nodes, application, get_env, [riak_core, http]),
    [{Host, Port} || {ok, [{Host, Port}]} <- ResL].

%% Counter API
get_counter(HostPort, Key) ->
    get(HostPort, Key).

increment_counter(HostPort, Key) ->
    increment_counter(HostPort, Key, 1).

increment_counter(HostPort, Key, Amt) ->
    update_counter(HostPort, Key, increment, Amt).

decrement_counter(HostPort, Key) ->
    decrement_counter(HostPort, Key, 1).

decrement_counter(HostPort, Key, Amt) ->
    update_counter(HostPort, Key, decrement, Amt).

update_counter(HostPort, Key, Action, Amt) ->
    post(HostPort, [Key, Action], integer_to_list(Amt)).

%% HTTP API
url({Host, Port}, PathElements) ->
    Path = path([counters|PathElements]),
    lists:flatten(io_lib:format("http://~s:~p~s", [Host, Port, Path])).

path(Elements) ->
    [lists:concat(['/', E]) || E <- Elements].

get(HostPort, Key) ->
    Url = url(HostPort, [Key]),
    {ok, {{_Version, 200, _}, _, Body}} =
        httpc:request(Url),
    list_to_integer(Body).

post(HostPort, PathElements, Body) ->
    Url = url(HostPort, PathElements),
    {ok, {{_Version, 204, _}, _, _}} =
        httpc:request(post, {Url, [], [], Body}, [], []),
    ok.
