%%%-------------------------------------------------------------------
%%% @author Joseph DeVivo <>
%%% @copyright (C) 2013, Joseph DeVivo
%%% @doc
%%%
%%% @end
%%% Created : 29 Jan 2013 by Joseph DeVivo <>
%%%-------------------------------------------------------------------
-module(loaded_upgrade_worker).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-behaviour(gen_server).

%% API
-export([start_link/2, assert_equal/3]).

-export([list_keys_tester/3, kv_tester/3, mapred_tester/3, 
         twoi_tester/3, search_tester/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          name,
          node=undefined,
          list_keys,
          mapred,
          kv,
          twoi,
          search,
          state=active
        }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name, Node) ->
    lager:debug("Spawning loaded_upgrade_worker for ~p", [Name]),
    gen_server:start_link({local, Name}, ?MODULE, [Name, Node], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Name, Node]) ->
    lager:debug("loaded_upgrade_worker:init([~p]) ~p", [Node, self()]),

    ListKeysPBC = rt:pbc(Node),
    ListKeysPid = spawn_link(?MODULE, list_keys_tester, [Name, 0, ListKeysPBC]),

    MapRedPBC = rt:pbc(Node),
    MapRedPid = spawn_link(?MODULE, mapred_tester, [Name, 0, MapRedPBC]),
    
    KVPBC = rt:pbc(Node),
    KVPid = spawn_link(?MODULE, kv_tester, [Name, 0, KVPBC]),
    
    %% TODO: pass backend in as an init param
    TestMetaData = riak_test_runner:metadata(self()),
    %% Only run 2i for level
    Backend = proplists:get_value(backend, TestMetaData),
    lager:info("BACKEND! ~p", [Backend]),
    TwoIPid = case Backend of
        eleveldb ->
            TwoIPBC = rt:pbc(Node),
            spawn_link(?MODULE, twoi_tester, [Name, 0, TwoIPBC]);
        _ -> undefined
    end,

    SearchPBC = rt:pbc(Node),
    SearchPid = spawn_link(?MODULE, search_tester, [Name, 0, SearchPBC]),
    {ok, #state{name=Name, 
                node=Node, 
                list_keys=ListKeysPid, 
                mapred=MapRedPid,
                kv=KVPid,
                twoi=TwoIPid,
                search=SearchPid
               }}.

%    PBC = rt:pbc(Node),
%    HTTPC = rt:httpc(Node),
%    lager:debug("Created Clients ~p", [Name]),
%    State = #state{
%       node = Node,
%       name = Name,
%       pbc = PBC,
%       httpc = HTTPC
%      },
%    Pid = spawn_link(?MODULE, test_fun, [State]),
%    lager:debug("Spawned tester ~p", [Name]),
%    {ok, State#state{runner=Pid}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%test_fun(State=#state{count=Count}) ->
%%    [ F() || F <- generate_test_funs(State)],
%%    test_fun(State#state{count=Count+1}).

%%generate_test_funs(#state{name=Name, pbc=PBC, httpc=_HTTPC, count=Count}) ->    
%%    [
%%     %% List Keys
%%     fun() ->
%%        io:format("< ~p > listkeys test #~p", [Name, Count]),
%%        {ok, Keys} = riakc_pb_socket:list_keys(PBC, <<"objects">>), 
%%        ActualKeys = lists:usort(Keys),
%%        ExpectedKeys = lists:usort([list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, 100)]),
%%        assert_equal(Name, ExpectedKeys, ActualKeys)    
%%     end,
%%     %% KV
%%     fun() ->
%%        io:format("< ~p > kv test #~p", [Name, Count]),
%%        Key = Count rem 8000,
%%        Val = rt:pbc_read(PBC, new_loaded_upgrade:bucket(kv), new_loaded_upgrade:int_to_key(Key)),
%%        ?assertEqual(new_loaded_upgrade:kv_valgen(Key), riakc_obj:get_value(Val))
%%     end,
     %% MapRed
%%     fun() ->
%%        io:format("< ~p > mapred test #~p", [Name, Count]),
%%        case riakc_pb_socket:mapred(PBC, new_loaded_upgrade:bucket(mapred), new_loaded_upgrade:erlang_mr()) of
%%            {ok, [{1, [10000]}]} ->
%%                ?assert(true);
%%            {ok, R} ->
%%                io:format("< ~p > bad mapred result: ~p", [Name, R]),
%%                ?assert(false);
%%            {error, Reason} ->
%%                io:format("< ~p > mapred error: ~p", [Name, Reason]),
%%                ?assert(false)
%%        end
%%     End
%%    ].

list_keys_tester(Name, Count, PBC) ->
    io:format("<~p> listkeys test #~p", [Name, Count]),
    {ok, Keys} = riakc_pb_socket:list_keys(PBC, <<"objects">>), 
    ActualKeys = lists:usort(Keys),
    ExpectedKeys = lists:usort([new_loaded_upgrade:int_to_key(K) || K <- lists:seq(0, 100)]),
    assert_equal(Name, ExpectedKeys, ActualKeys),
    list_keys_tester(Name, Count + 1, PBC).

kv_tester(Name, Count, PBC) ->
    io:format("<~p> kv test #~p", [Name, Count]),
    Key = Count rem 8000,
    Val = rt:pbc_read(PBC, new_loaded_upgrade:bucket(kv), new_loaded_upgrade:int_to_key(Key)),
    ?assertEqual(new_loaded_upgrade:kv_valgen(Key), riakc_obj:get_value(Val)),
    kv_tester(Name, Count + 1, PBC).


mapred_tester(Name, Count, PBC) ->
    io:format("<~p> mapred test #~p", [Name, Count]),
    case riakc_pb_socket:mapred(PBC, new_loaded_upgrade:bucket(mapred), new_loaded_upgrade:erlang_mr()) of
        {ok, [{1, [10000]}]} ->
            ?assert(true);
        {ok, R} ->
            io:format("< ~p > bad mapred result: ~p", [Name, R]),
            ?assert(false);
        {error, Reason} ->
            io:format("< ~p > mapred error: ~p", [Name, Reason]),
            ?assert(false)
    end,
    mapred_tester(Name, Count + 1, PBC).

twoi_tester(Name, Count, PBC) ->
    io:format("<~p> 2i test #~p", [Name, Count]),
    Key = Count rem 8000,
    ExpectedKeys = [new_loaded_upgrade:int_to_key(Key)],
    {ok, BinKeys} = riakc_pb_socket:get_index(
                              PBC, 
                              new_loaded_upgrade:bucket(twoi), 
                              {binary_index, "plustwo"}, 
                              new_loaded_upgrade:int_to_key(Key + 2)),
    {ok, IntKeys} = riakc_pb_socket:get_index(
                              PBC, 
                              new_loaded_upgrade:bucket(twoi), 
                              {integer_index, "plusone"}, 
                              Key + 1),
    assert_equal(Name, ExpectedKeys, BinKeys),
    assert_equal(Name, ExpectedKeys, IntKeys),
    twoi_tester(Name, Count + 1, PBC).

search_tester(Name, Count, PBC) ->
    io:format("<~p> search test #~p", [Name, Count]),
    
    {ok, Results1} = riakc_pb_socket:search(PBC, new_loaded_upgrade:bucket(search), <<"mx.example.net">>),
    {ok, Results2} = riakc_pb_socket:search(PBC, new_loaded_upgrade:bucket(search), <<"ZiaSun">>),
    {ok, Results3} = riakc_pb_socket:search(PBC, new_loaded_upgrade:bucket(search), <<"headaches">>),
    {ok, Results4} = riakc_pb_socket:search(PBC, new_loaded_upgrade:bucket(search), <<"YALSP">>),
    {ok, Results5} = riakc_pb_socket:search(PBC, new_loaded_upgrade:bucket(search), <<"mister">>),
    {ok, Results6} = riakc_pb_socket:search(PBC, new_loaded_upgrade:bucket(search), <<"prohibiting">>),

    ?assertEqual(187, Results1#search_results.num_found),
    ?assertEqual(1, Results2#search_results.num_found),
    ?assertEqual(4, Results3#search_results.num_found),
    ?assertEqual(3, Results4#search_results.num_found),
    ?assertEqual(0, Results5#search_results.num_found),
    ?assertEqual(5, Results6#search_results.num_found),

    search_tester(Name, Count + 1, PBC).

assert_equal(Name, Expected, Actual) ->
    case Expected -- Actual of 
        [] -> ok;
        Diff -> lager:info("<~p> Expected -- Actual: ~p", [Name, Diff])
    end,
    ?assertEqual(length(Actual), length(Expected)),
    ?assertEqual(Actual, Expected).

