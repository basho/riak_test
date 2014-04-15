%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(verify_listkeys_eqc).
-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(riak_test).
-export([confirm/0]).

-behaviour(eqc_statem).
-export([initial_state/0]).

-define(BUCKET, <<"BUCKET">>).
-define(NUM_KEYS, 5).
-define(NUM_BUCKETS, 2).
-define(NUM_TESTS, 4).
-define(LAZY_TIMER, 20).
-define(PREFIX, {x, x}).
-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(THRESHOLD_SECS, 10).

-record(state, {
          bucket_type = undefined,
          bucket = undefined,
          node_joining = undefined,
          nodes_up = [],
          nodes_down = [],
          nodes_ready_count = 0,
          cluster_nodes = [],
          ring_size = 0,
          num_keys = 0,
          preload_complete = false,
          setup_complete = false
         }).

-record(node, {name, context}).

%% ====================================================================
%% riak_test callback
%% ====================================================================
confirm() ->
%    lager:set_loglevel(lager_console_backend, warning),
%    OutputFun = fun(Str, Args) -> lager:error(Str, Args) end,
%    ?assert(eqc:quickcheck(eqc:on_output(OutputFun, eqc:numtests(?NUM_TESTS, ?MODULE:prop_test())))),
    ?assert(eqc:quickcheck(eqc:numtests(?NUM_TESTS, ?MODULE:prop_test()))),

    pass.

%% ====================================================================
%% EQC generators
%% ====================================================================
gen_numnodes() ->
    oneof([2, 3, 4, 5]).

num_keys() ->
    elements([10, 100, 1000]).

g_uuid() ->
    eqc_gen:bind(eqc_gen:bool(), fun(_) -> druuid:v4_str() end).

g_bucket_type() ->
    oneof(bucket_types()).

%% ====================================================================
%% EQC Properties
%% ====================================================================

%% @TODO Make a separate property to test bucket listing
%% put_buckets(Node, ?NUM_BUCKETS),

prop_test() ->
    ?FORALL(Cmds, noshrink(commands(?MODULE)),
            ?WHENFAIL(
               begin
                   _ = lager:error("*********************** FAILED!!!!"
                                   "*******************")
               end,
               ?TRAPEXIT(
                  begin
                      %% rt:setup_harness(dummy, dummy),
                      lager:info("======================== Will run commands:"),
                      [lager:info(" Command : ~p~n", [Cmd]) || Cmd <- Cmds],
                      {H, S, R} = run_commands(?MODULE, Cmds),
                      lager:info("======================== Ran commands"),
                      #state{nodes_up = NU, cluster_nodes=CN} = S,
                      Destroy =
                          fun({node, N, _}) ->
                                  lager:info("Wiping out node ~p for good", [N]),
                                  rt:clean_data_dir(N),
                                  rt:stop(N)
                          end,
                      Nodes = lists:usort(NU ++ CN),
                      lager:info("======================== Taking all nodes down ~p", [Nodes]),
                      lists:foreach(Destroy, Nodes),
                      eqc_gen:with_parameter(show_states,
                                             true,
                                             pretty_commands(?MODULE, Cmds, {H, S, R}, equals(ok, R)))
                  end))).

%% ====================================================================
%% EQC commands (using group commands)
%% ====================================================================

%% -- initial_state --
initial_state() ->
    #state{}.

%% -- add_nodes --
add_nodes_pre(S) ->
    S#state.nodes_up == [].

add_nodes_args(_S) ->
    ?LET(Num, gen_numnodes(), [Num]).

add_nodes(NumNodes) ->
    lager:info("Deploying cluster of size ~p", [NumNodes]),
    Nodes = rt:build_cluster(NumNodes),
    rt:wait_until_nodes_ready(Nodes).

add_nodes_next(S, _, [NumNodes]) ->
    Nodes = node_list(NumNodes),
    NodeList = [ #node{ name = Node, context = [] } || Node <- Nodes ],
    S#state{ nodes_up = NodeList }.

add_nodes_post(_S, _Args, ok) ->
    true;
add_nodes_post(_S, _Args, _) ->
    false.

setup_pre(S) when S#state.nodes_up /= [],
                  S#state.setup_complete == false ->
    true;
setup_pre(_) ->
    false.

setup_args(S) ->
    [S#state.nodes_up].

setup(Nodes) ->
    NodeList = [ N#node.name || N <- Nodes ],
    Node = hd(NodeList),
    rt:wait_until_transfers_complete(NodeList),
    [begin
         rt:create_and_activate_bucket_type(Node, BucketType, [{n_val, NVal}]),
         rt:wait_until_bucket_type_status(BucketType, active, NodeList)
     end || {BucketType, NVal} <- bucket_types()].

setup_next(S, _, _) ->
    S#state{setup_complete=true}.

preload_pre(S) when S#state.nodes_up /= [],
                    S#state.setup_complete == true,
                    S#state.preload_complete == false ->
    true;
preload_pre(_) ->
    false.

preload_args(S) ->
    [g_bucket_type(), g_uuid(), S#state.nodes_up, num_keys()].

preload({BucketType, _}, Bucket, Nodes, NumKeys) ->
    Node = hd(Nodes),
    NodeName = Node#node.name,
    lager:info("*******************[CMD]  First node ~p", [NodeName]),
    lager:info("Writing to bucket ~p", [Bucket]),
    put_keys(NodeName, {BucketType, Bucket}, NumKeys).

preload_next(S, _, [{BucketType, _}, Bucket, _, NumKeys]) ->
    S#state{bucket_type = BucketType,
            bucket = Bucket,
            num_keys = NumKeys,
            preload_complete=true}.

preload_post(_S, [_BucketType, _Bucket, _Nodes, _NumKeys], _R) ->
    %% lager:info("In preload_post, Bucket:~p, Nodes:~p, NumKeys:~p", [Bucket, Nodes, NumKeys]),
    %% KeyRes = [ list_keys(Node, Bucket, NumKeys, true) || {node, Node, _} <- Nodes ],
    %% false == lists:member(false, KeyRes).
    true.

verify_pre(#state{preload_complete=true}) ->
    true;
verify_pre(_) ->
    false.

verify_args(S) ->
    [S#state.bucket_type, S#state.bucket, S#state.nodes_up, S#state.num_keys].

verify(BucketType, Bucket, Nodes, NumKeys) ->
    [list_keys(Node#node.name, {BucketType, Bucket}, NumKeys, true) || Node <- Nodes].

verify_post(_S, _Args, R) ->
    false == lists:member(false, R).

%% ====================================================================
%% Helpers
%% ====================================================================

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

put_buckets(Node, Num) ->
    lager:info("[CMD] Putting ~p buckets on ~p", [Num, Node]),
    Pid = rt:pbc(Node),
    try
        Buckets = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
        {Key, Val} = {<<"test_key">>, <<"test_value">>},
        [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val)) || Bucket <- Buckets]
    after
        catch(riakc_pb_socket:stop(Pid))
    end.

list_keys(Node, Bucket, Num, ShouldPass) ->
    %% Move client to state
    Pid = rt:pbc(Node),
    try
        lager:info("Listing keys on node ~p.", [Node]),
        Res = riakc_pb_socket:list_keys(Pid, Bucket),
        lager:info("Result is ~p", [Res]),
        case ShouldPass of
            true ->
                {ok, Keys} = Res,
                ActualKeys = lists:usort(Keys),
                ExpectedKeys = lists:usort([list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)]),
                assert_equal(ExpectedKeys, ActualKeys);
            _ ->
                {Status, Message} = Res,
                Status == error andalso <<"insufficient_vnodes_available">> == Message
        end
    after
        catch(riakc_pb_socket:stop(Pid))
    end.

assert_equal(Expected, Actual) ->
    case Expected -- Actual of
        [] -> ok;
        Diff -> lager:info("Expected -- Actual: ~p", [Diff])
    end,
    length(Actual) == length(Expected)
        andalso Actual == Expected.

node_list(NumNodes) ->
    NodesN = lists:seq(1, NumNodes),
    [?DEV(N) || N <- NodesN].

bucket_types() ->
    [{<<"n_val_one">>, 1},
     {<<"n_val_two">>, 2},
     {<<"n_val_three">>, 3},
     {<<"n_val_four">>, 4},
     {<<"n_val_five">>, 5}].
