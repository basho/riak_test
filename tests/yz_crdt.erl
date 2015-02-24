-module(yz_crdt).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).
-define(INDEX, <<"maps">>).
-define(TYPE, <<"maps">>).
-define(KEY, "Chris Meiklejohn").
-define(BUCKET, {?TYPE, <<"testbucket">>}).

-define(CONF, [
        {riak_core,
            [{ring_creation_size, 8}]
        },
        {yokozuna,
            [{enabled, true}]
        }]).

confirm() ->
    rt:set_advanced_conf(all, ?CONF),

    %% Configure cluster.
    [Nodes] = rt:build_clusters([1]),
    [Node|_] = Nodes,

    %% Create PB connection.
    Pid = rt:pbc(Node),
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    %% Create index.
    riakc_pb_socket:create_search_index(Pid, ?INDEX, <<"_yz_default">>, []),

    %% Create bucket type for maps.
    rt:create_and_activate_bucket_type(Node,
                                       ?TYPE,
                                       [{datatype, map},
                                        {search_index, ?INDEX}]),

    %% Write some sample data.
    Map = riakc_map:update(
            {<<"name">>, register},
            fun(R) ->
                riakc_register:set(
                    list_to_binary(?KEY), R)
            end, riakc_map:new()),
    ok = riakc_pb_socket:update_type(
            Pid,
            ?BUCKET,
            ?KEY,
            riakc_map:to_op(Map)),

    %% Wait for yokozuna index to trigger.
    timer:sleep(1000),

    %% Perform a simple query.
    {ok, {search_results, Results, _, _}} = riakc_pb_socket:search(
            Pid, ?INDEX, <<"name_register:Chris*">>),
    ?assertEqual(length(Results), 1),
    lager:info("~p~n", [Results]),

    %% Stop PB connection.
    riakc_pb_socket:stop(Pid),

    %% Clean cluster.
    rt:clean_cluster(Nodes),

    pass.