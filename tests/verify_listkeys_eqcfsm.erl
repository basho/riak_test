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
-define(MAX_CLUSTER_SIZE, 5).
-record(state, {
          buckets = orddict:new(),
          nodes_up = [],
          nodes_down = [],
          cluster_nodes = [],
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
                      Nodes = setup_cluster(random:uniform(?MAX_CLUSTER_SIZE)),
                      lager:info("======================== Will run commands with Nodes:~p:", [Nodes]),
                      [lager:info(" Command : ~p~n", [Cmd]) || Cmd <- Cmds],
                      {H, _S, Res} = run_commands(?MODULE, Cmds, [{nodelist, Nodes}]),
                      lager:info("======================== Ran commands"),
                      rt_cluster:clean_cluster(Nodes),
                      aggregate(zip(state_names(H),command_names(Cmds)), 
                          equals(Res, ok))
                 end))).

%% ====================================================================
%% EQC FSM state transitions
%% ====================================================================
initial_state() ->
    preloading_data.

preloading_data(S) ->
    [
     {history, {call, ?MODULE, preload_data, [g_bucket_type(), g_uuid(), {var, nodelist},
                                              g_num_keys(), g_key_filter()]}},
     {verifying_data, {call, ?MODULE, log_transition, [S]}}
    ].

verifying_data(S) ->
    [
     {stopped, {call, ?MODULE, verify, [S#state.buckets, 
                                        S#state.nodes_up,
                                        S#state.key_filter]}}
    ].

stopped(_S) ->
    [].

%% ====================================================================
%% EQC FSM State Data
%% ====================================================================
initial_state_data() ->
    #state{}.

next_state_data(preloading_data, preloading_data, S, _, {call, _, preload_data, 
                    [{BucketType, _}, Bucket, Nodes, NumKeys, KeyFilter]}) ->
    S#state{ buckets = orddict:update_counter({Bucket, BucketType}, NumKeys, S#state.buckets), 
             key_filter = KeyFilter,
             nodes_up = Nodes 
    };
next_state_data(_From, _To, S, _R, _C) ->
    S.

%% ====================================================================
%% EQC FSM state transition weights
%% ====================================================================
weight(preloading_data,preloading_data,{call,verify_listkeys_eqcfsm,preload_data,[_,_,_,_,_]}) -> 80;
weight(preloading_data,verifying_data,{call,verify_listkeys_eqcfsm,log_transition,[_]}) -> 10;
weight(verifying_data,stopped,{call,verify_listkeys_eqcfsm,verify,[_,_,_]}) -> 10. 

%% ====================================================================
%% EQC FSM preconditions
%% ====================================================================
precondition(_From,_To,_S,{call,_,_,_}) ->
    true.

%% ====================================================================
%% EQC FSM postconditions
%% ====================================================================
postcondition(_From,_To,_S,{call,_,verify,_},{error, Reason}) ->
    lager:info("Error: ~p", [Reason]),
    false;
postcondition(_From,_To,S,{call,_,verify,_},KeyDict) ->
    Res = audit_keys_per_node(S, KeyDict),
    not lists:member(false, Res);
postcondition(_From,_To,_S,{call,_,_,_},_Res) ->
    true.

audit_keys_per_node(S, KeyDict) ->
    [ [ assert_equal(
            expected_keys(orddict:fetch({Bucket, BucketType}, S#state.buckets), 
                          S#state.key_filter), 
            NodeKeyList)
        || NodeKeyList <- orddict:fetch({Bucket, BucketType}, KeyDict)  ]
     || {Bucket, BucketType} <- orddict:fetch_keys(S#state.buckets) ].
%% ====================================================================
%% callback functions
%% ====================================================================
preload_data({BucketType, _}, Bucket, Nodes, NumKeys, _KeyFilter) ->
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),
    lager:info("*******************[CMD]  First node ~p", [Node]),
    lager:info("Writing to bucket ~p", [Bucket]),
    put_keys(Node, {BucketType, Bucket}, NumKeys).

verify(undefined, _Nodes, _KeyFilter) ->
    lager:info("Nothing to compare.");
verify(Buckets, Nodes,  KeyFilter) ->
    Keys = orddict:fold(fun({Bucket, BucketType}, _, Acc) ->
                            ListVal = [ list_filter_sort(Node, {BucketType, Bucket}, KeyFilter) 
                                        || Node <- Nodes ],
                            orddict:append({Bucket, BucketType}, hd(ListVal), Acc)
                        end,
                        orddict:new(),
                        Buckets),
    Keys.
    
log_transition(S) ->
    lager:debug("Buckets and key counts at transition:"),
    orddict:fold(fun({Bucket, BucketType} = _Key, NumKeys, _Acc) -> 
                     lager:debug("Bucket:~p, BucketType:~p, NumKeys:~p", [Bucket,BucketType,NumKeys])
                 end,
                 [],
                 S#state.buckets).

%% ====================================================================
%% Helpers
%% ====================================================================
setup_cluster(NumNodes) ->
    Nodes = rt_cluster:build_cluster(NumNodes),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_transfers_complete(Nodes)),
    Node = hd(Nodes),
    [begin
         rt:create_and_activate_bucket_type(Node, BucketType, [{n_val, NVal}]),
         rt:wait_until_bucket_type_status(BucketType, active, Nodes),
         rt:wait_until_bucket_type_visible(Nodes, BucketType)
     end || {BucketType, NVal} <- bucket_types()],
    Nodes.

assert_equal(Expected, Actual) ->
    case Expected -- Actual of
        [] ->
            ok;
        Diff -> lager:info("Expected:~p~nActual:~p~nExpected -- Actual: ~p", 
                            [length(Expected), length(Actual), length(Diff)])
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
