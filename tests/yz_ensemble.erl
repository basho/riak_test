-module(yz_ensemble).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(CFG,
        [
         {riak_core,
          [
           {ring_creation_size, 8}
          ]},
         {yokozuna,
          [
           {enabled, true}
          ]}
        ]).

confirm() ->
    NumNodes = 3,
    NVal = 3,
    ConfigB = ensemble_util:fast_config(NVal),
    Config = ConfigB ++ [{yokozuna, [{enabled, true}]}],
    lager:info("Building cluster and waiting for ensemble to stablize"),
    Nodes = build_cluster_with_yz_support(NumNodes, Config, NVal),
    rt:wait_for_cluster_service(Nodes, yokozuna),
    vnode_util:load(Nodes),
    Node = hd(Nodes),

    lager:info("Creating/activating 'strong' bucket type"),
    rt_bucket_types:create_and_activate_bucket_type(Node, <<"strong">>,
                                       [{consistent, true}, {n_val, NVal}]),

    Bucket = {<<"strong">>, <<"test">>},
    Index = <<"testi">>,
    create_index(Node, Index),
    set_bucket_props(Node, Bucket, Index),

	verify_ensemble_delete_support(Node, Bucket, Index),

    pass.


%% @private
%% @doc Populates then deletes from SC bucket
verify_ensemble_delete_support(Node, Bucket, Index) ->
    %% Yz only supports UTF-8 compatible keys
    Keys = [<<N:64/integer>> || N <- lists:seq(1,2000),
        not lists:any(fun(E) -> E > 127 end,binary_to_list(<<N:64/integer>>))],

    PBC = rt_pb:pbc(Node),

    lager:info("Writing ~p keys", [length(Keys)]),
    [ok = rt_pb:pbc_write(PBC, Bucket, Key, Key, "text/plain") || Key <- Keys],

    %% soft commit wait, then check that last key is indexed
    lager:info("Search for keys to verify they exist"),
    timer:sleep(1000),
    LKey = lists:last(Keys),
    rt:wait_until(fun() ->
        {M, _} = riakc_pb_socket:search(PBC, Index, query_value(LKey)),
        ok == M
    end),
    [{ok, _} =
        riakc_pb_socket:search(PBC, Index, query_value(Key)) || Key <- Keys],

    lager:info("Deleting keys"),
    [riakc_pb_socket:delete(PBC, Bucket, Key) || Key <- Keys],
    timer:sleep(1000),
    rt:wait_until(fun() ->
        case riakc_pb_socket:search(PBC, Index, query_value(LKey)) of
            {ok,{search_results,Res,_,_}} ->
            	lager:info("RES: ~p ~p~n", [Res, LKey]),
            	Res == [];
            S ->
            	lager:info("OTHER: ~p ~p~n", [S, LKey]),
            	false
        end
    end),
    [ {ok,{search_results,[],_,_}} =
        riakc_pb_socket:search(PBC, Index, query_value(Key)) || Key <- Keys],

    ok.


%% @private
%% @doc build a cluster from ensemble_util + yz support
%%
%% NOTE: There's a timing issue that causes join_cluster to hang the r_t
%% node when adding yokozuna and ensemble support. Waiting for yokozuna
%% to load on each node allows join_cluster to complete consistently
build_cluster_with_yz_support(Num, Config, NVal) ->
    Nodes = rt_cluster:deploy_nodes(Num, Config),
    [rt:wait_for_cluster_service([N], yokozuna) || N <- Nodes],
    Node = hd(Nodes),
    rt_cluster:join_cluster(Nodes),
    ensemble_util:wait_until_cluster(Nodes),
    ensemble_util:wait_for_membership(Node),
    ensemble_util:wait_until_stable(Node, NVal),
    Nodes.

%% @private
%% @doc Builds a simple riak key query
query_value(Value) ->
    V2 = iolist_to_binary(re:replace(Value, "\"", "%22")),
    V3 = iolist_to_binary(re:replace(V2, "\\\\", "%5C")),
    <<"_yz_rk:\"",V3/binary,"\"">>.

%% pulled from yz_rt

%% @private
create_index(Node, Index) ->
    lager:info("Creating index ~s [~p]", [Index, Node]),
    ok = rpc:call(Node, yz_index, create, [Index]).

%% @private
set_bucket_props(Node, Bucket, Index) ->
    Props = [{search_index, Index}],
    rpc:call(Node, riak_core_bucket, set_bucket, [Bucket, Props]).
