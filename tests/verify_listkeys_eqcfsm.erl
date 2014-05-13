-module(verify_listkeys_eqcfsm).
-compile(export_all).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(riak_test).
-export([confirm/0]).

-define(NUM_TESTS, 5).
-define(PREFIX, {x, x}).
-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).

-record(state, {
          bucket_type = undefined,
          bucket = undefined,
          nodes_up = [],
          nodes_down = [],
          cluster_nodes = [],
          num_keys = 0,
          key_filter = undefined
       }).

%% ====================================================================
%% riak_test callback
%% ====================================================================
confirm() ->
    ?assert(eqc:quickcheck(eqc:numtests(?NUM_TESTS, ?MODULE:prop_test()))),
    pass.
%% ====================================================================
%% EQC generators
%% ====================================================================
g_num_nodes() ->
    oneof([2, 3, 4, 5]).

g_num_keys() ->
    choose(10, 1000).

g_uuid() ->
    noshrink(eqc_gen:bind(eqc_gen:bool(), fun(_) -> druuid:v4_str() end)).

g_bucket_type() ->
    oneof(bucket_types()).

g_key_filter() ->
    %% Create a key filter function.
    %% There will always be at least 10 keys
    %% due to the lower bound of object count
    %% generator.
    MatchKeys = [list_to_binary(integer_to_list(X)) || X <- lists:seq(1,10)],
    KeyFilter =
        fun(X) ->
                lists:member(X, MatchKeys)
        end,
    frequency([{4, none}, {2, KeyFilter}]).

%% ====================================================================
%% EQC Properties
%% ====================================================================
prop_test() ->
    ?FORALL(Cmds, noshrink(commands(?MODULE)),
            ?WHENFAIL(
               begin
                   _ = lager:error("*********************** FAILED!!!!"
                                   "*******************")
               end,
               ?TRAPEXIT(
                  begin
                      lager:info("======================== Will run commands:"),
                      [lager:info(" Command : ~p~n", [Cmd]) || Cmd <- Cmds],
                      {H, S, Res} = run_commands(?MODULE, Cmds),
                      lager:info("======================== Ran commands"),
                      clean_nodes(S),
                      aggregate(zip(state_names(H),command_names(Cmds)), 
                          equals(Res, ok))
                  end))).

%% ====================================================================
%% EQC FSM state transitions
%% ====================================================================
initial_state() ->
    building_cluster.

building_cluster(_S) ->
    [
     {preloading_data, {call, ?MODULE, setup_cluster, [g_num_nodes()]}}
    ].

preloading_data(S) ->
    [
     {verifying_data, {call, ?MODULE, preload_data, [g_bucket_type(), g_uuid(), hd(S#state.nodes_up),
                                                      g_num_keys(), g_key_filter()]}}
    ].

verifying_data(S) ->
    [
     {tearing_down_nodes, {call, ?MODULE, verify, [S#state.bucket_type, S#state.bucket, S#state.nodes_up,
                                                   S#state.num_keys, S#state.key_filter]}}
    ].

tearing_down_nodes(S) ->
    [
     {stopped, {call, ?MODULE, clean_nodes, [S#state.nodes_up]}}
    ].

stopped(_S) ->
    [].

%% ====================================================================
%% EQC FSM State Data
%% ====================================================================
initial_state_data() ->
    #state{}.

next_state_data(building_cluster, preloading_data, S, _, {call, _, setup_cluster, [NumNodes]}) ->
    S#state{ nodes_up = node_list(NumNodes) };
next_state_data(preloading_data, verifying_data, S, _, {call, _, preload_data, 
                                                            [{BucketType, _}, Bucket, _Nodes, NumKeys, KeyFilter]}) ->
    S#state{ bucket_type = BucketType, bucket = Bucket, num_keys = NumKeys, key_filter = KeyFilter };
next_state_data(_From, _To, S, _R, _C) ->
    S.

%% ====================================================================
%% EQC FSM preconditions
%% ====================================================================
precondition(_From,_To,_S,{call,_,_,_}) ->
    true.

%% ====================================================================
%% EQC FSM postconditions
%% ====================================================================
postcondition(_From,_To,_S,{call,_,setup_cluster,_},Res) ->
    ok == Res;
postcondition(_From,_To,_S,{call,_,verify,_},{error, Reason}) ->
    lager:info("Error: ~p", [Reason]),
    false;
postcondition(_From,_To,S,{call,_,verify,_},KeyLists) ->
    ExpectedKeys = expected_keys(S#state.num_keys, S#state.key_filter),
    lists:all(fun(true) -> true; (_) -> false end,
              [assert_equal(ExpectedKeys, Keys) || Keys <- KeyLists]);
postcondition(_From,_To,_S,{call,_,_,_},_Res) ->
    true.

%% ====================================================================
%% callback functions
%% ====================================================================
clean_nodes({stopped, _S}) ->
    lager:info("Clean-up already completed.");
clean_nodes({_, S}) ->
    lager:info("Running clean_nodes with S:~p", [S]),
    clean_nodes(S#state.nodes_up);
clean_nodes([]) ->
    lager:info("clean_nodes: no cluster to clean");
clean_nodes(Nodes) ->
    lager:info("Running clean_nodes with Nodes:~p", [Nodes]),
    CleanupFun =
       fun(N) ->
           lager:info("Wiping out node ~p for good", [N]),
                      rt:clean_data_dir(N)
       end,
    lager:info("======================== Taking all nodes down ~p", [Nodes]),
    rt:pmap(CleanupFun, Nodes),
    rt:teardown().

preload_data({BucketType, _}, Bucket, Node, NumKeys, _KeyFilter) ->
    lager:info("*******************[CMD]  First node ~p", [Node]),
    lager:info("Writing to bucket ~p", [Bucket]),
    put_keys(Node, {BucketType, Bucket}, NumKeys).

setup_cluster(NumNodes) ->
    lager:info("Deploying cluster of size ~p", [NumNodes]),
    Nodes = rt:build_cluster(NumNodes),
    rt:wait_until_nodes_ready(Nodes),
    Node = hd(Nodes),
    rt:wait_until_transfers_complete(Nodes),
    [begin
         rt:create_and_activate_bucket_type(Node, BucketType, [{n_val, NVal}]),
         rt:wait_until_bucket_type_status(BucketType, active, Nodes)
     end || {BucketType, NVal} <- bucket_types()],
    ok.

verify(BucketType, Bucket, Nodes, _NumKeys, KeyFilter) ->
    [list_filter_sort(Node, {BucketType, Bucket}, KeyFilter) || Node <- Nodes].

%% ====================================================================
%% Helpers
%% ====================================================================
assert_equal(Expected, Actual) ->
    case Expected -- Actual of
        [] -> ok;
        Diff -> lager:info("Expected -- Actual: ~p", [Diff])
    end,
    length(Actual) == length(Expected)
        andalso Actual == Expected.

bucket_types() ->
    [{<<"n_val_one">>, 1},
     {<<"n_val_two">>, 2},
     {<<"n_val_three">>, 3},
     {<<"n_val_four">>, 4},
     {<<"n_val_five">>, 5}].

expected_keys(NumKeys, FilterFun) ->
    KeysPair = {ok, [list_to_binary(["", integer_to_list(Ki)]) ||
                        Ki <- lists:seq(0, NumKeys - 1)]},
    sort_keys(filter_keys(KeysPair, FilterFun)).

filter_keys({ok, Keys}, none) ->
    Keys;
filter_keys({ok, Keys}, FilterFun) ->
    lists:filter(FilterFun, Keys);
filter_keys({error, _}=Error, _) ->
    Error.

list_filter_sort(Node, Bucket, KeyFilter) ->
    %% Move client to state
    {ok, C} = riak:client_connect(Node),
    sort_keys(filter_keys(riak_client:list_keys(Bucket, C), KeyFilter)).

node_list(NumNodes) ->
    NodesN = lists:seq(1, NumNodes),
    [?DEV(N) || N <- NodesN].

put_keys(Node, Bucket, Num) ->
    lager:info("*******************[CMD]  Putting ~p keys into bucket ~p on node ~p", [Num, Bucket, Node]),
    Pid = rt:pbc(Node),
    try
        Keys = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
        Vals = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
        [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val)) || {Key, Val} <- lists:zip(Keys, Vals)]
    after
        catch(riakc_pb_socket:stop(Pid))
    end.

sort_keys({error, _}=Error) ->
    Error;
sort_keys(Keys) ->
    lists:usort(Keys).

-endif. % EQC
