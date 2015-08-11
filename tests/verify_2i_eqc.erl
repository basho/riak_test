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
%% @doc EQC test for secondary indexing using eqc_fsm to generate
%% sequences of indexing and query commands.
%%
%% The state machine is very simple. Mostly it:
%% - Indexes a bunch of keys under a single integer or binary term.
%% - Indexes a bunch of keys under an equal number of consecutive integer
%%   or binary terms.
%% - Deletes a single item and all its associated index entries.
%% - Generates random queries and verifies results match the model.
%%   Notice how we are only checking against the entire set of results and
%%   not against each page of the results. I suggest that as an improvement.
%%
%% A couple of dummy states exist just to ensure that each run starts by
%% first creating a bunch of clients, then choosing a new unique bucket.
%%
%% The test model stores a list of keys
%% and index data for a configurable number of fields. The keys are all
%% numeric for simpler presentation and get converted to and from binary
%% as needed. For example, if two objects are created and indexed like this:
%%
%% - key 10, "i1_int" -> 1, "i1_bin" -> "a"
%% - key 20, "i1_int" -> 1, "i1_bin" -> "b"
%%
%% The model data would look like this:
%%
%% keys = [10, 20]
%% indexes = 
%%   [
%%     {{bin, "i1"}, [
%%                      {<<"a">>, [10]},
%%                      {<<"b">>, [20]}
%%                   ]},
%%     {{int, "i1"}, [
%%                      {1, [10, 20]}
%%                   ]}
%%   ]       
%%
%% All lists in the indexes field are sorted and manipulated using orddict.
%% The indexes data structure is an orddict that maps a typed field to
%% an orddict mapping terms to lists of keys.
%% As in Riak, here "i1_int" and "i1_bin" are the fields, and the values
%% such as 1 or "a" are called terms.
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

-define(MAX_CLUSTER_SIZE, 1).
-define(MAX_FIELDS, 1).
-define(FIELDS, ["i" ++ integer_to_list(N) || N <- lists:seq(1, ?MAX_FIELDS)]).
-define(CLIENT_TYPES, [pb]).

-type index_field() :: {int | bin, binary()}.
-type index_value() :: binary().
-type index_pair() :: {index_term(), [index_value()]}.
-type index_data() :: {index_field(), [index_pair()]}.

-record(state, {
          nodes = [],
          clients = [],
          bucket,
          keys = [] :: [key()],
          indexes = [] :: list(index_data())
         }).

-record(query, {
          bucket,
          field,
          start_term,
          end_term,
          page_size,
          continuation
         }).

confirm() ->
    %% Set up monotonic bucket name generator.
    init_bucket_counter(),
    Size = random:uniform(?MAX_CLUSTER_SIZE),
    %% Run for 2 minutes by default.
    TestingTime = rt_config:get(eqc_testing_time, 120),
    lager:info("Will run in cluster of size ~p for ~p seconds.",
               [Size, TestingTime]),
    Nodes = rt:build_cluster(Size),
    ?assert(eqc:quickcheck(
              eqc:testing_time(TestingTime, ?MODULE:prop_test(Nodes)))),
    pass.

%% ====================================================================
%% EQC Properties
%% ====================================================================
prop_test(Nodes) ->
    InitState = #state{nodes = Nodes},
    ?FORALL(Cmds, commands(?MODULE, {initial_state(), InitState}),
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
                      {H, {_SName, S}, Res} = run_commands(?MODULE, Cmds),
                      lager:info("======================== Ran commands"),
                      %% Each run creates a new pool of clients. Clean up.
                      close_clients(S#state.clients),
                      %% Record stats on what commands were generated on
                      %% successful runs. This is printed after the test
                      %% finishes.
                      aggregate(zip(state_names(H),command_names(Cmds)), 
                          equals(Res, ok))
                 end))).

%% ====================================================================
%% Value generators and utilities.
%% ====================================================================

gen_node(S) ->
    oneof(S#state.nodes).

gen_client_id(S) ->
    {oneof(S#state.nodes), oneof(?CLIENT_TYPES)}.

%% Generates a key in the range 0-999.
%% TODO: How to determine an optimal range for coverage?
%% If too large, we wouldn't update the same key very often, for example.
gen_key() ->
    choose(0, 999).

%% Pick one of a fixed list of possible base field names.
gen_field() ->
    oneof(?FIELDS).

%% Produces either a binary or integer term value.
gen_term() ->
    oneof([gen_int_term(), gen_bin_term()]).

%% Generates, with equal likelihood, either a smallish or a largish integer.
gen_int_term() ->
    oneof([int(), largeint()]).

%% Generates a random binary.
gen_bin_term() ->
    binary().

%% Generates a list of integer keys without duplicates.
gen_key_list() ->
    ?LET(L, non_empty(list(gen_key())), lists:usort(L)).

%% Generates non-empty lists of {Key, Field, Term} triplets.
gen_key_field_terms() ->
    non_empty(list({gen_key(), gen_field(), gen_term()})).

%% Produces, with equal likelihood, either no page size, a smallish one or 
%% a largish one.
gen_page_size() ->
    oneof([undefined, gen_small_page_size(), gen_large_page_size()]).

%% Based on EQC's nat() so numbers tend to be smallish.
%% Adjusting with LET to avoid zero, which is invalid.
gen_small_page_size() ->
    ?LET(N, nat(), N + 1).

%% Adjusts largeint() to make the result strictly positive.
gen_large_page_size() ->
    choose(1, 16#ffffFFFF).

%% Chooses one of the keys in the model at random.
gen_existing_key(#state{keys = Keys}) ->
    oneof(Keys).

%% Generates either a query on an integer or binary field that:
%% - Uses a couple of existing terms  as start/ends
%% - Includes all terms in the index
%% - Generates start/end terms randomly, which may not span any existing items.
gen_range_query(S) ->
    oneof([gen_some_query(S), gen_all_query(S), gen_random_query(S)]).

gen_random_query(#state{bucket = Bucket}) ->
    oneof([gen_int_query(Bucket), gen_bin_query(Bucket)]).

%% Query that includes all terms for a given field.
gen_all_query(#state{bucket = Bucket, indexes = Idx}) ->
    ?LET({{{_Type, Field}, Terms}, PageSize},
         {oneof(Idx), gen_page_size()},
         new_query(Bucket, Field, first_term(Terms), last_term(Terms),
                      PageSize)).

%% Chooses two existing terms as start and end.
gen_some_query(#state{bucket = Bucket, indexes = Idx}) ->
    ?LET({{{_Type, Field}, Terms}, PageSize},
         {oneof(Idx), gen_page_size()},
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

%% Populates a new query record. For convenience, corrects the order of the
%% start and end terms so that start is always less than or equal to end.
%% That way we don't need any generator tricks for those.
new_query(Bucket, Field, Term1, Term2, PageSize) when Term1 > Term2 ->
    #query{bucket = Bucket, field = Field,
           start_term = Term2, end_term = Term1,
           page_size = PageSize};
new_query(Bucket, Field, Term1, Term2, PageSize) ->
    #query{bucket = Bucket, field = Field,
           start_term = Term1, end_term = Term2,
           page_size = PageSize}.

%% First term in a term to keys orddict.
first_term(TermKeys) ->
    {Term, _} = hd(TermKeys),
    Term.

%% Last term in a term to keys orddict.
last_term(TermKeys) ->
    {Term, _} = lists:last(TermKeys),
    Term.

%% ======================================================
%% States spec
%% ======================================================
initial_state() ->
    pre_setup_state1.

initial_state_data() ->
    #state{}.

pre_setup_state1(S) ->
    #state{nodes = Nodes} = S,
    [{pre_setup_state2, {call, ?MODULE, tx_create_clients, [Nodes]}}].

pre_setup_state2(_S) ->
    [{default_state, {call, ?MODULE, tx_next_bucket, []}}].

default_state(S) ->
    #state{clients = Clients, bucket = Bucket} = S,
    [
     {default_state, {call, ?MODULE, tx_index_single_term,
                      [Clients, gen_client_id(S), Bucket,
                       gen_key_list(), gen_field(), gen_term()]}},
     {default_state, {call, ?MODULE, tx_index_multi_term,
                      [Clients, gen_client_id(S), Bucket,
                       gen_key_field_terms()]}},
     {default_state, {call, ?MODULE, tx_delete_one,
                      [Clients, gen_client_id(S), Bucket,
                       gen_existing_key(S)]}},
     {default_state, {call, ?MODULE, tx_query_range,
                      [Clients, gen_client_id(S), gen_range_query(S)]}}
    ].

%% Tweak transition weights such that deletes are rare.
%% Indexing a bunch or querying a bunch of items are equally likely.
weight(default_state, default_state, {call, _, tx_delete_one, _}) ->
    1;
weight(_, _, _) ->
    100.

%% State data mutations for each transition.
next_state_data(_, _, S, Clients, {call, _, tx_create_clients, [_]}) ->
    S#state{clients = Clients};
next_state_data(_, _, S, Bucket, {call, _, tx_next_bucket, []}) ->
    S#state{bucket = Bucket};
next_state_data(default_state, default_state, S, _,
                {call, _, tx_index_single_term,
                 [_, _, _, NewKeys, Field, Term]}) ->
    #state{keys = Keys0, indexes = Idx0} = S,
    Keys1 = lists:umerge(NewKeys, Keys0),
    Idx1 = model_index(NewKeys, Field, Term, Idx0),
    S#state{keys = Keys1, indexes = Idx1};
next_state_data(default_state, default_state, S, _,
                {call, _, tx_index_multi_term,
                 [_, _, _, KeyFieldTerms]}) ->
    #state{keys = Keys0, indexes = Idx0} = S,
    %% Add to list of keys and dedupe.
    NewKeys = [K || {K, _, _} <- KeyFieldTerms],
    Keys1 = lists:umerge(NewKeys, Keys0),
    Idx1 = model_index(KeyFieldTerms, Idx0),
    S#state{keys = Keys1, indexes = Idx1};
next_state_data(default_state, default_state, S, _,
                {call, _, tx_delete_one, [_, _, _, Key]}) ->
    #state{keys = Keys0, indexes = Idx0} = S,
    Keys1 = lists:delete(Key, Keys0),
    Idx1 = model_delete_key(Key, Idx0),
    S#state{keys = Keys1, indexes = Idx1};
next_state_data(_, _, S, _, _) ->
    %% Any other transition leaves state unchanged.
    S.

%% No precondition checks. Among other things, that means that shrinking may
%% end up issuing deletes to keys that do not exist, which is harmless.
%% Any indexing, deleting or querying command can be issued at any point
%% in the sequence. 
precondition(_From, _To, _S, {call, _, _, _}) ->
    true.

%% Signal a test failure if there is an explicit error from the query or
%% if the results do not match what is in the model.
postcondition(_, _, S, {call, _, tx_query_range, [_, _, Query]}, {error, Err}) ->
    {state, S, query, Query, error, Err};
postcondition(_, _, S, {call, _, tx_query_range, [_, _, Query]}, Keys) ->
    #state{indexes = Idx} = S,
    ExpectedKeys = model_query_range(Query, Idx),
    case lists:usort(Keys) =:= ExpectedKeys of
        true -> true;
        false -> {state, S, query, Query, expected, ExpectedKeys, actual, Keys}
    end;
postcondition(_, _, _, _Call, _) ->
    true.

%% ======================================================
%% State transition functions.
%% ======================================================

%% Returns a dictionary that stores a client object per each node
%% and client type.
%% {Node, Type} -> {Type, Client}
tx_create_clients(Nodes) ->
    orddict:from_list([{{N, T}, {T, create_client(N, T)}}
                    || N <- Nodes, T <- ?CLIENT_TYPES]).

%% Returns a different bucket name each time it's called.
tx_next_bucket() ->
    N = ets:update_counter(bucket_table, bucket_number, 1),
    NBin = integer_to_binary(N),
    <<"bucket", NBin/binary>>.

%% Index a bunch of keys under the same field/term.
tx_index_single_term(Clients, ClientId, Bucket, Keys, Field, Term) ->
    Client = get_client(ClientId, Clients),
    lager:info("Indexing in ~p under (~p, ~p) using client ~p: ~p",
               [Bucket, Field, Term, ClientId, Keys]),
    [index_object(Client, Bucket, Key, Field, Term) || Key <- Keys],
    ok.

%% Index a number of keys each under a different term.
tx_index_multi_term(Clients, ClientId, Bucket, KeyFieldTerms) ->
    Client = get_client(ClientId, Clients),
    lager:info("Indexing in ~p with client ~p: ~p",
               [Bucket, ClientId, KeyFieldTerms]),
    [index_object(Client, Bucket, Key, Field, Term)
     || {Key, Field, Term} <- KeyFieldTerms],
    ok.

%% Delete a single object and all its associated index entries.
tx_delete_one(Clients, ClientId, Bucket, IntKey) ->
    Client = get_client(ClientId, Clients),
    lager:info("Deleting key ~p from bucket ~p using ~p",
               [IntKey, Bucket, ClientId]),
    delete_key(Client, Bucket, IntKey),
    ok.
    
tx_query_range(Clients, ClientId, Query) ->
    Client = get_client(ClientId, Clients),
    Keys = lists:sort(query_range(Client, Query, [])),
    lager:info("Query ~p, ~p  from ~p to  ~p, page = ~p, returned ~p keys.",
               [Query#query.bucket, Query#query.field, Query#query.start_term,
                Query#query.end_term, Query#query.page_size, length(Keys)]),
    %% Re-run with page sizes 1 -> 100, verify it's always the same result.
    PageChecks =
    [begin
         Q2 = Query#query{page_size = PSize},
         OKeys = lists:sort(query_range(Client, Q2, [])),
         OKeys =:= Keys
     end || PSize <- lists:seq(1, 100)],
    case lists:all(fun is_true/1, PageChecks) of
        true ->
            Keys;
        false ->
            {error, mismatch_when_paged}
    end.

%% ======================================================
%% Client utilities.
%% ======================================================

create_client(Node, pb) ->
    rt:pbc(Node);
create_client(Node, http) ->
    rt:httpc(Node).

get_client(ClientId, Clients) ->
    orddict:fetch(ClientId, Clients).

%% Convert field/term pair to pb client argument format.
pb_field_term(Field, Term) when is_integer(Term) ->
    {Field ++ "_int", Term};
pb_field_term(Field, Term) when is_binary(Term) ->
    {Field ++ "_bin", Term}.

pb_field(Field, Term) when is_integer(Term) ->
    {integer_index, Field};
pb_field(Field, Term) when is_binary(Term) ->
    {binary_index, Field}.

index_object({pb, PB}, Bucket, Key0, Field, Term) ->
    Key = to_bin_key(Key0),
    FT = pb_field_term(Field, Term),
    Obj =
    case riakc_pb_socket:get(PB, Bucket, Key) of
        {ok, O} ->
            %% Existing object, add index tag and de-duplicate.
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
            %% New object, just add index tag.
            O = riakc_obj:new(Bucket, Key, Key),
            MD = dict:from_list([{<<"index">>, [FT]}]),
            riakc_obj:update_metadata(O, MD)
    end,
    ok = riakc_pb_socket:put(PB, Obj),
    ok.

delete_key({pb, PB}, Bucket, IntKey) ->
    Key = to_bin_key(IntKey),
    ok = riakc_pb_socket:delete(PB, Bucket, Key),
    %% Wait until all tombstones have been reaped.
    ok = rt:wait_until(fun() -> rt:pbc_really_deleted(PB, Bucket, [Key]) end);
delete_key({http, C}, Bucket, IntKey) ->
    Key = to_bin_key(IntKey),
    ok = rhc:delete(C, Bucket, Key),
    %% Wait until all tombstones have been reaped.
    ok = rt:wait_until(fun() -> rt:httpc_really_deleted(C, Bucket, [Key]) end).

%% Execute range query using a client, fetching multiple pages if necessary.
query_range({pb, PB} = Client,
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
            query_range(Client, Query1, AccKeys ++ Keys)
    end.

%% Close all clients, ignore errors.
close_clients(Clients) ->
    [catch riakc_pb_socket:stop(Client) || {pb, Client} <- Clients],
    ok.

%% ======================================================
%% Model data utilities
%% ======================================================

model_index([], Idx) ->
    Idx;
model_index([{Keys, Field, Term} | More], Idx) ->
    Idx1 = model_index(Keys, Field, Term, Idx),
    model_index(More, Idx1).

model_index(NewKeys0, Field, Term, Idx) when is_list(NewKeys0) ->
    TField = to_tfield(Field, Term),
    NewKeys = lists:usort(NewKeys0),
    TermKeys1 =
    case orddict:find(TField, Idx) of
        {ok, TermKeys0} ->
            case orddict:find(Term, TermKeys0) of
                {ok, Keys0} ->
                    MergedKeys = lists:umerge(NewKeys, Keys0),
                    orddict:store(Term, MergedKeys, TermKeys0);
                _ ->
                    orddict:store(Term, NewKeys, TermKeys0)
            end;
        _ ->
            [{Term, NewKeys}]
    end,
    orddict:store(TField, TermKeys1, Idx);
model_index(NewKey, Field, Term, Idx) ->
    model_index([NewKey], Field, Term, Idx).

model_delete_key(Key, Idx) ->
    [{Field, delete_key_from_term_keys(Key, TermKeys)}
     || {Field, TermKeys} <- Idx].

delete_key_from_term_keys(Key, TermKeys) ->
    [{Term, lists:delete(Key, Keys)} || {Term, Keys} <- TermKeys].

%% Produces a typed field id. For example, "i1"/43 -> {int, "i1"}
to_tfield(FieldName, Term) ->
    case is_integer(Term) of
        true -> {int, FieldName};
        false -> {bin, FieldName}
    end.

%% Query against the modeled data.
model_query_range(Query, Idx) ->
    #query{ field = Field, start_term = Start, end_term = End } = Query,
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

%% ======================================================
%% Internal
%% ======================================================

is_true(true) -> true;
is_true(_) -> false.

%% Initialize counter used to use a different bucket per run.
init_bucket_counter() ->
    ets:new(bucket_table, [named_table, public]),
    ets:insert_new(bucket_table, [{bucket_number, 0}]).

%% Convert integer object key to binary form.
to_bin_key(N) when is_integer(N) ->
    iolist_to_binary(io_lib:format("~5..0b", [N]));
to_bin_key(Key) when is_binary(Key) ->
    Key.

-endif.
