%%
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
-module(verify_2i_eqc).
-compile(export_all).

-ifdef(EQC).
-include_lib("riakc/include/riakc.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(riak_test).
-export([confirm/0]).

-define(NUM_TESTS, 5).
-define(MAX_CLUSTER_SIZE, 1).
-define(MAX_BUCKETS, 1).
-define(BUCKETS, [iolist_to_binary(["bucket", integer_to_binary(N)])
                  || N <- lists:seq(1, ?MAX_BUCKETS)]).
-define(MAX_FIELDS, 1).
-define(FIELDS, ["i" ++ integer_to_list(N) || N <- lists:seq(1, ?MAX_FIELDS)]).
-define(VALUE, <<"v">>).

-type index_field() :: {int | bin, binary()}.
-type index_value() :: binary().
-type index_pair() :: {index_term(), [index_value()]}.
-type index_data() :: {index_field(), [index_pair()]}.

-record(bucket_data, {
          keys = [] :: [key()],
          indexes = [] :: list(index_data())
         }).
-record(state, {
          nodes = [],
          clients,
          bucket,
          data = #bucket_data{}
         }).

-record(query, {
          bucket,
          field,
          start_term,
          end_term,
          page_size,
          continuation
         }).

%% Initialize counter used to use a different bucket per run.
init_bucket_counter() ->
    ets:new(bucket_table, [named_table, public]),
    ets:insert_new(bucket_table, [{bucket_number, 0}]).

confirm() ->
    init_bucket_counter(),
    Size = random:uniform(?MAX_CLUSTER_SIZE),
    TestingTime = rt_config:get(eqc_testing_time, 120),
    lager:info("Will run in cluster of size ~p for ~p seconds.",
               [Size, TestingTime]),
    Nodes = rt:build_cluster(Size),
    %% Run for 2 minutes by default.
    ?assert(eqc:quickcheck(
              eqc_statem:show_states(
                eqc:testing_time(TestingTime, ?MODULE:prop_test(Nodes))))),
    pass.

%% ====================================================================
%% EQC Properties
%% ====================================================================
prop_test(Nodes) ->
    ?FORALL(Cmds, noshrink(commands(?MODULE)),
            ?WHENFAIL(
               begin
                   _ = lager:error("*********************** FAILED!!!!"
                                   "*******************")
               end,
               ?TRAPEXIT(
                  begin
                      lager:info("========================"
                                 " Will run commands with Nodes:~p:", [Nodes]),
                      [lager:info(" Command : ~p~n", [Cmd]) || Cmd <- Cmds],
                      {H, _S, Res} = run_commands(?MODULE, Cmds,
                                                  [{nodelist, Nodes}]),
                      lager:info("======================== Ran commands"),
                      aggregate(zip(state_names(H),command_names(Cmds)), 
                          equals(Res, ok))
                 end))).

%% ====================================================================
%% Value generators
%% ====================================================================

gen_bucket() ->
    oneof(?BUCKETS).

gen_field() ->
    oneof(?FIELDS).

gen_int_term() ->
    choose(0, 9999).

gen_bin_term() ->
    binary().

gen_key() ->
    gen_int_term().

to_bin_key(N) ->
    iolist_to_binary(io_lib:format("~5..0b", [N])).

gen_term() ->
    oneof([gen_int_term(), gen_bin_term()]).

gen_page_size() ->
    oneof([undefined, gen_small_page_size(), choose(1, 10000)]).

gen_small_page_size() ->
    ?LET(N, nat(), N + 1).

%% Generate a {bucket, key} for an existing object.
%% Will fail if no objects exist, so the command will not be generated
%% if no key exists.
gen_existing_key(#state{data = #bucket_data {keys = Keys}}) ->
    oneof(Keys).

new_query(Bucket, Field, Term1, Term2, PageSize) when Term1 > Term2 ->
    #query{bucket = Bucket, field = Field,
           start_term = Term2, end_term = Term1,
           page_size = PageSize};
new_query(Bucket, Field, Term1, Term2, PageSize) ->
    #query{bucket = Bucket, field = Field,
           start_term = Term1, end_term = Term2,
           page_size = PageSize}.

gen_query(S) ->
    oneof([gen_some_query(S), gen_all_query(S), gen_random_query(S)]).

gen_random_query(#state{bucket = Bucket}) ->
    oneof([gen_int_query(Bucket), gen_bin_query(Bucket)]).

first_term(TermKeys) ->
    {Term, _} = hd(TermKeys),
    Term.

last_term(TermKeys) ->
    {Term, _} = lists:last(TermKeys),
    Term.

%% Query that includes all terms for a given field.
gen_all_query(#state{bucket = Bucket, data = BData}) ->
    ?LET({{{_Type, Field}, Terms}, PageSize},
         {oneof(BData#bucket_data.indexes), gen_page_size()},
         new_query(Bucket, Field, first_term(Terms), last_term(Terms),
                      PageSize)).

%% Start from an existing term and end on another.
gen_some_query(#state{bucket = Bucket, data = BData}) ->
    ?LET({{{_Type, Field}, Terms}, PageSize},
         {oneof(BData#bucket_data.indexes), gen_page_size()},
         ?LET({{Term1, _}, {Term2, _}}, {oneof(Terms), oneof(Terms)},
              new_query(Bucket, Field, Term1, Term2, PageSize))).

gen_int_query(Bucket) ->
    ?LET({Field, Term1, Term2, PageSize},
         {gen_field(), gen_int_term(), gen_int_term(), gen_page_size()},
         new_query(Bucket, Field, Term1, Term2, PageSize)).

gen_bin_query(Bucket) ->
    ?LET({Field, Term1, Term2, PageSize},
         {gen_field(), gen_bin_term(), gen_bin_term(), gen_page_size()},
         new_query(Bucket, Field, Term1, Term2, PageSize)).

%% ======================================================
%% States spec
%% ======================================================
initial_state() ->
    pre_setup_state1.

initial_state_data() ->
    #state{}.

pre_setup_state1(_S) ->
    [{pre_setup_state2, {call, ?MODULE, create_clients, [{var, nodelist}]}}].

pre_setup_state2(_S) ->
    [{default_state, {call, ?MODULE, choose_bucket, []}}].

default_state(S) ->
    #state{clients = Clients, nodes = Nodes, bucket = Bucket} = S,
    [
     {default_state, {call, ?MODULE, index_objects,
                      [Clients, Nodes, Bucket, gen_key(), choose(1, 100), 
                       gen_field(), gen_term()]}},
     {default_state, {call, ?MODULE, index_objects2,
                      [Clients, Nodes, Bucket, gen_key(), choose(1, 100), 
                       gen_field(), gen_term()]}},
     {default_state, {call, ?MODULE, delete_one,
                      [Clients, Nodes, Bucket, gen_existing_key(S)]}},
     {default_state, {call, ?MODULE, query_range,
                      [Clients, Nodes, gen_query(S)]}}
    ].

%% Deletes are rare. Queries are less frequent than indexing objects.
weight(default_state, default_state, {call, _, delete_one, _}) ->
    1;
weight(_, _, _) ->
    100.

next_state_data(_, _, S, Clients, {call, _, create_clients, [Nodes]}) ->
    S#state{nodes = Nodes, clients = Clients};
next_state_data(_, _, S, Bucket, {call, _, choose_bucket, []}) ->
    S#state{bucket = Bucket};
next_state_data(default_state, default_state, S, _,
                {call, _, index_objects,
                 [_, _, _Bucket, IntKey, NKeys, Field, Term]}) ->
    #state{data = BData0} = S,
    #bucket_data{keys = Keys0, indexes = Idx0} = BData0,
    TField = to_tfield(Field, Term),
    Keys1 = to_key_list(IntKey, NKeys),
    Keys2 = lists:umerge(Keys0, Keys1),
    Idx2 = orddict:update(TField, update_fterm_fn(Term, Keys1),
                          [{Term, Keys1}], Idx0),
    BData1 = BData0#bucket_data{keys = Keys2, indexes = Idx2},
    S1 = S#state{data = BData1},
    S1;
next_state_data(default_state, default_state, S, _,
                {call, _, index_objects2,
                 [_, _, _Bucket, IntKey, NKeys, Field, Term]}) ->
    #state{data = BData0} = S,
    #bucket_data{keys = Keys0, indexes = Idx0} = BData0,
    TField = to_tfield(Field, Term),
    Keys1 = to_key_list(IntKey, NKeys),
    Keys2 = lists:umerge(Keys0, Keys1),
    TermKeys = to_term_keys(NKeys, IntKey, Term),
    Idx2 = orddict:update(TField, update_fterm2_fn(TermKeys),
                          TermKeys, Idx0),
    BData1 = BData0#bucket_data{keys = Keys2, indexes = Idx2},
    S1 = S#state{data = BData1},
    S1;
next_state_data(default_state, default_state, S, _,
                {call, _, delete_one, [_, _, _Bucket, Key]}) ->
    #state{data = BData0} = S,
    #bucket_data{keys = Keys0, indexes = Idx0} = BData0,
    Keys1 = lists:delete(Key, Keys0),
    Idx1 = delete_key_from_idx(Key, Idx0),
    BData1 = BData0#bucket_data{keys = Keys1, indexes = Idx1},
    S#state{data = BData1};
next_state_data(_, _, S, _, _) ->
    %% Assume anything not handled leaves the state unchanged.
    S.

precondition(_From, _To, _S, {call, _, _, _}) ->
    true.

postcondition(_, _, S, {call, _, query_range, [_, _, Query]}, {error, Err}) ->
    {state, S, query, Query, error, Err};
postcondition(_, _, S, {call, _, query_range, [_, _, Query]}, Keys) ->
    ExpectedKeys = query_state(S, Query),
    case lists:usort(Keys) =:= ExpectedKeys of
        true -> true;
        false -> {state, S, query, Query, expected, ExpectedKeys, actual, Keys}
    end;
postcondition(_, _, _, _Call, _) ->
    true.

%% ======================================================
%% Internal
%% ======================================================

to_key_list(BaseKey, N) ->
    [Key || Key <- lists:seq(BaseKey, BaseKey + N)].

update_keys_fn(NewKeys) ->
    fun(OldKeys) ->
            lists:umerge(OldKeys, NewKeys)
    end.

update_fterm_fn(Term, Keys) ->
    fun(TermKeys) ->
            orddict:update(Term, update_keys_fn(Keys), Keys, TermKeys)
    end.

update_fterm2_fn(NewTermKeys) ->
    fun(TermKeys) ->
            orddict:merge(fun(_K, Keys1, Keys2) ->
                                  lists:umerge(Keys1, Keys2)
                          end, TermKeys, NewTermKeys)
    end.

update_ft_add_fn(Term, Key) ->
    fun(OldTermKeys) ->
            orddict:update(Term, update_termkey_add_fn(Key), OldTermKeys)
    end.

update_termkey_add_fn(Key) ->
    fun(OldKeys) ->
            lists:usort([Key|OldKeys])
    end.

to_tfield(FieldName, Term) ->
    case is_integer(Term) of
        true -> {int, FieldName};
        false -> {bin, FieldName}
    end.

to_term_keys(Count, Key, Term) when is_integer(Term) ->
    [{Term + N, [Key]} || N <- lists:seq(0, Count - 1)];
to_term_keys(Count, Key, Term) when is_binary(Term) ->
    [begin
         Suffix = iolist_to_binary(io_lib:format("~0..5b", [N])),
         {<<Term/binary, Suffix/binary>>, [Key]}
     end || N <- lists:seq(0, Count - 1)].

delete_key_from_term_keys(Key, TermKeys) ->
    [{Term, lists:delete(Key, Keys)} || {Term, Keys} <- TermKeys].

delete_key_from_idx(Key, Idx) ->
    [{Field, delete_key_from_term_keys(Key, TermKeys)}
     || {Field, TermKeys} <- Idx].

%% Query against the modeled data.
query_state(#state{ data = #bucket_data{indexes = Idx}},
            #query{ field = Field,
                    start_term = Start, end_term = End }) ->
    TField = to_tfield(Field, Start),
    %% Collect all keys with terms within the given range, ignore others.
    Scanner = fun({Term, Keys}, Acc) when Term >= Start, End >= Term ->
                      [Keys | Acc];
                 ({_Term, _Keys}, Acc) ->
                      Acc
              end,
    case orddict:find(TField, Idx) of
        error ->
            [];
        {ok, FieldIdx} ->
            KeyGroups = lists:foldl(Scanner, [], FieldIdx),
            IntKeys = lists:umerge(KeyGroups),
            [to_bin_key(Key) || Key <- IntKeys]
    end.

pb_field_term(Field, Term) when is_integer(Term) ->
    {Field ++ "_int", Term};
pb_field_term(Field, Term) when is_binary(Term) ->
    {Field ++ "_bin", Term}.

pb_field(Field, Term) when is_integer(Term) ->
    {integer_index, Field};
pb_field(Field, Term) when is_binary(Term) ->
    {binary_index, Field}.

%% ======================================================
%% Transition functions.
%% ======================================================
create_clients(Nodes) ->
    dict:from_list([{Node, rt:pbc(Node)} || Node <- Nodes]).

%% Returns a different bucket name each time it's called.
choose_bucket() ->
    N = ets:update_counter(bucket_table, bucket_number, 1),
    NBin = integer_to_binary(N),
    <<"bucket", NBin/binary>>.

index_objects(Clients, Nodes, Bucket, Key0, NKeys, Field, Term) ->
    Node = hd(Nodes),
    lager:info("Indexing ~p in ~p starting at key ~p "
               "under (~p, ~p) on node ~p",
               [NKeys, Bucket, Key0, Field, Term, Node]),
    PB = dict:fetch(Node, Clients),
    FT = pb_field_term(Field, Term),
    [begin
         Key = to_bin_key(N),
         Obj =
         case riakc_pb_socket:get(PB, Bucket, Key) of
             {ok, O} ->
                 MD = riakc_obj:get_metadata(O),
                 IndexMD2 =
                 case dict:find(<<"index">>, MD) of
                     {ok, IndexMD} ->
                         lists:usort([FT | IndexMD]);
                     error ->
                         [FT]
                 end,
                 MD2 = dict:store(<<"index">>, IndexMD2, MD),
                 riakc_obj:update_metadata(O, MD2);
             {error, notfound} ->
                 O = riakc_obj:new(Bucket, Key, Key),
                 MD = dict:from_list([{<<"index">>, [FT]}]),
                 riakc_obj:update_metadata(O, MD)
         end,
         ok = riakc_pb_socket:put(PB, Obj)
     end || N <- to_key_list(Key0, NKeys)],
    ok.

index_objects2(Clients, Nodes, Bucket, Key0, NKeys, Field, Term0) ->
    Node = hd(Nodes),
    lager:info("Indexing (2) ~p in ~p starting at key ~p, term ~p "
               "under ~p, on node ~p",
               [NKeys, Bucket, Key0, Term0, Field, Node]),
    PB = dict:fetch(Node, Clients),
    [begin
         Key = to_bin_key(N),
         FT = pb_field_term(Field, Term),
         Obj =
         case riakc_pb_socket:get(PB, Bucket, Key) of
             {ok, O} ->
                 MD = riakc_obj:get_metadata(O),
                 IndexMD2 =
                 case dict:find(<<"index">>, MD) of
                     {ok, IndexMD} ->
                         lists:usort([FT | IndexMD]);
                     error ->
                         [FT]
                 end,
                 MD2 = dict:store(<<"index">>, IndexMD2, MD),
                 riakc_obj:update_metadata(O, MD2);
             {error, notfound} ->
                 O = riakc_obj:new(Bucket, Key, Key),
                 MD = dict:from_list([{<<"index">>, [FT]}]),
                 riakc_obj:update_metadata(O, MD)
         end,
         ok = riakc_pb_socket:put(PB, Obj)
     end || {Term, [N]} <- to_term_keys(NKeys, Key0, Term0)],
    ok.

delete_one(Clients, Nodes, Bucket, IntKey) ->
    Key = to_bin_key(IntKey),
    Node = hd(Nodes),
    PB = dict:fetch(Node, Clients),
    lager:info("Deleting key ~p from bucket ~p", [Key, Bucket]),
    ok = riakc_pb_socket:delete(PB, Bucket, Key),
    ok = rt:wait_until(fun() -> rt:pbc_really_deleted(PB, Bucket, [Key]) end),
    ok.
    
is_true(true) -> true;
is_true(_) -> false.

query_range(Clients, Nodes, Query) ->
    Node = hd(Nodes),
    PB = dict:fetch(Node, Clients),
    Keys = lists:sort(query_range_pb(PB, Query, [])),
    lager:info("Query ~p, ~p  from ~p to  ~p, page = ~p, returned ~p keys.",
               [Query#query.bucket, Query#query.field, Query#query.start_term,
                Query#query.end_term, Query#query.page_size, length(Keys)]),
    %% Re-run with page sizes 1 -> 100, verify it's always the same result.
    PageChecks =
    [begin
         Q2 = Query#query{page_size = PSize},
         OKeys = lists:sort(query_range_pb(PB, Q2, [])),
         OKeys =:= Keys
     end || PSize <- lists:seq(1, 100)],
    case lists:all(fun is_true/1, PageChecks) of
        true ->
            Keys;
        false ->
            {error, mismatch_when_paged}
    end.

query_range_pb(PB,
               #query { bucket = Bucket, field = FieldName,
                        start_term = Start, end_term = End,
                        page_size = PageSize, continuation = Cont } = Query,
               AccKeys) ->
    Field = pb_field(FieldName, Start),
    case riakc_pb_socket:get_index_range(PB, Bucket, Field, Start, End,
                                         [{max_results, PageSize},
                                          {continuation, Cont}]) of
        {ok, ?INDEX_RESULTS{keys = Keys, continuation = undefined}} ->
            AccKeys ++ Keys;
        {ok, ?INDEX_RESULTS{keys = Keys, continuation = Cont1}} ->
            Query1 = Query#query{continuation = Cont1},
            query_range_pb(PB, Query1, AccKeys ++ Keys)
    end.

-endif.
