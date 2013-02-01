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
-export([start_link/3, assert_equal/3]).

-export([list_keys_tester/4, kv_tester/4, mapred_tester/4, 
         twoi_tester/4, search_tester/4]).

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
start_link(Name, Node, Backend) ->
    lager:debug("Spawning loaded_upgrade_worker for ~p", [Name]),
    gen_server:start_link({local, Name}, ?MODULE, [Name, Node, Backend], []).

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
init([Name, Node, Backend]) ->
    rt:wait_for_service(Node, riak_kv),
    lager:debug("loaded_upgrade_worker:init([~p]) ~p", [Node, self()]),

    ListKeysPBC = rt:pbc(Node),
    ListKeysPid = spawn_link(?MODULE, list_keys_tester, [Name, Node, 0, ListKeysPBC]),

    MapRedPBC = rt:pbc(Node),
    MapRedPid = spawn_link(?MODULE, mapred_tester, [Name, Node, 0, MapRedPBC]),
    
    KVPBC = rt:pbc(Node),
    KVPid = spawn_link(?MODULE, kv_tester, [Name, Node, 0, KVPBC]),
    
    TwoIPid = case Backend of
        eleveldb ->
            TwoIPBC = rt:pbc(Node),
            spawn_link(?MODULE, twoi_tester, [Name, Node, 0, TwoIPBC]);
        _ -> undefined
    end,

    SearchPBC = rt:pbc(Node),
    SearchPid = spawn_link(?MODULE, search_tester, [Name, Node, 0, SearchPBC]),
    {ok, #state{name=Name, 
                node=Node, 
                list_keys=ListKeysPid, 
                mapred=MapRedPid,
                kv=KVPid,
                twoi=TwoIPid,
                search=SearchPid
               }}.


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

list_keys_tester(Name, Node, Count, PBC) ->
    %%lager:debug("<~p> listkeys test #~p", [Name, Count]),
    case riakc_pb_socket:list_keys(PBC, <<"objects">>) of
        {ok, Keys} ->
            ActualKeys = lists:usort(Keys),
            ExpectedKeys = lists:usort([new_loaded_upgrade:int_to_key(K) || K <- lists:seq(0, 100)]),
            case assert_equal(Name, ExpectedKeys, ActualKeys) of
                true -> cool;
                _ -> loaded_upgrade ! {listkeys, not_equal}
            end,
            list_keys_tester(Name, Node, Count + 1, PBC);
        {error, timeout} ->
            loaded_upgrade ! {listkeys, timeout},
            list_keys_tester(Name, Node, Count + 1, PBC);
        {error, {timeout, _}} ->
            loaded_upgrade ! {listkeys, timeout},
            list_keys_tester(Name, Node, Count + 1, PBC);
        {error, Reason} ->
            lager:debug("<~p> list keys connection error ~p", [Name, Reason]),
            list_keys_tester(Name, Node, Count, pb_pid_recycler(PBC, Node))            
    end.


kv_tester(Name, Node, Count, PBC) ->
    %%lager:debug("<~p> kv test #~p", [Name, Count]),
    Key = Count rem 8000,
    case riakc_pb_socket:get(PBC, new_loaded_upgrade:bucket(kv), new_loaded_upgrade:int_to_key(Key)) of
        {ok, Val} ->
            case new_loaded_upgrade:kv_valgen(Key) == riakc_obj:get_value(Val) of
                true -> cool;
                _ -> loaded_upgrade ! {kv, not_equal}
            end,
            kv_tester(Name, Node, Count + 1, PBC);            
        {error, disconnected} ->
            kv_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {error, Reason} ->
            loaded_upgrade ! {kv, {error, Reason}},
            kv_tester(Name, Node, Count, pb_pid_recycler(PBC, Node))
    end.

mapred_tester(Name, Node, Count, PBC) ->
    %%lager:debug("<~p> mapred test #~p", [Name, Count]),
    case riakc_pb_socket:mapred(PBC, new_loaded_upgrade:bucket(mapred), new_loaded_upgrade:erlang_mr()) of
        {ok, [{1, [10000]}]} ->
            mapred_tester(Name, Node, Count + 1, PBC);
        {ok, R} ->
            io:format("< ~p > bad mapred result: ~p", [Name, R]),
            loaded_upgrade ! {mapred, bad_result},
            mapred_tester(Name, Node, Count + 1, PBC);
        {error, disconnected} ->
            %%lager:debug("<~p> mapred connection error: ~p", [Name, disconnected]),
            mapred_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        %% Finkmaster Flex says timeouts are ok
        {error, timeout} ->
            mapred_tester(Name, Node, Count + 1, PBC);
        {error, {timeout, _}} ->
            mapred_tester(Name, Node, Count + 1, PBC);
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"{badmatch,{'EXIT',noproc}}", _/binary>>} ->
            mapred_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {error, <<"{\"phase\":0,\"error\":\"badarg", _/binary>>} ->
            mapred_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {error, <<"{\"phase\":0,\"error\":\"[preflist_exhausted]", _/binary>>} ->
            mapred_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {error, <<"{\"phase\":0,\"error\":\"{badmatch,{'EXIT',timeout}}", _/binary>>} ->
            mapred_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"function_clause\",\"input\":\"{cover,", _/binary>>} ->
            mapred_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"badarg\",\"input\":\"{cover,", _/binary>>} ->
            mapred_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {error, <<"Error processing stream message: exit:{ucs,{bad_utf8_character_code}}:[{xmerl_ucs,", _/binary>>} ->
            mapred_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {error, <<"{\"phase\":0,\"error\":\"[{vnode_down,{shutdown,{gen_fsm,sync_send_event,", _/binary>>} ->
            mapred_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {error, <<"{\"phase\":0,\"error\":\"[{vnode_down,noproc}]", _/binary>>} ->
            mapred_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {error, Reason} ->
            lager:debug("< ~p > mapred error: ~p", [Name, Reason]),
            loaded_upgrade ! {error, Reason}
    end.

twoi_tester(Name, Node, Count, PBC) ->
    %%lager:debug("<~p> 2i test #~p", [Name, Count]),
    Key = Count rem 8000,
    ExpectedKeys = [new_loaded_upgrade:int_to_key(Key)],
    case {
      riakc_pb_socket:get_index(
                              PBC, 
                              new_loaded_upgrade:bucket(twoi), 
                              {binary_index, "plustwo"}, 
                              new_loaded_upgrade:int_to_key(Key + 2)),
      riakc_pb_socket:get_index(
                              PBC, 
                              new_loaded_upgrade:bucket(twoi), 
                              {integer_index, "plusone"}, 
                              Key + 1)
     } of 
        {{ok, BinKeys}, {ok, IntKeys}} ->           
            case {assert_equal(Name, ExpectedKeys, BinKeys), assert_equal(Name, ExpectedKeys, IntKeys)} of
                {true, true} -> cool;
                {false, false} ->
                    loaded_upgrade ! {twoi, bolth_no_match};
                {false, true} ->
                    loaded_upgrade ! {twoi, bin_no_match};
                {true, false} ->
                    loaded_upgrade ! {twoi, int_no_match}
            end,
            twoi_tester(Name, Node, Count + 1, PBC);
        {{error, Reason}, _} ->
            lager:debug("<~p> 2i connection error: ~p", [Name, Reason]),
            twoi_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));
        {_, {error, Reason}} ->
            lager:debug("<~p> 2i connection error: ~p", [Name, Reason]),
            twoi_tester(Name, Node, Count, pb_pid_recycler(PBC, Node))
    end.

search_tester(Name, Node, Count, PBC) ->
    %%lager:debug("<~p> search test #~p", [Name, Count]),
    {Term, Size} = search_check(Count),
    case riakc_pb_socket:search(PBC, new_loaded_upgrade:bucket(search), Term) of
        {ok, Result} ->
            ?assertEqual(Size, Result#search_results.num_found),
            search_tester(Name, Node, Count + 1, PBC);
        {error, disconnected} ->
            %% oh well, reconnect
            search_tester(Name, Node, Count, pb_pid_recycler(PBC, Node)); 
        {error, Reason} when is_binary(Reason) ->
            case string:str(binary_to_list(Reason), "badfun") of
                0 -> %% This is not badfun, probably a connection issue
                    lager:debug("<~p> search connection error: ~p", [Name, Reason]),
                    search_tester(Name, Node, Count, pb_pid_recycler(PBC, Node));      
                _ -> %% this is badfun
                    ?assert(rt:is_mixed_cluster(Node)),
                    search_tester(Name, Node, Count + 1, PBC)
            end
    end.

search_check(Count) ->
    case Count rem 6 of
        0 -> { <<"mx.example.net">>, 187};
        1 -> { <<"ZiaSun">>, 1};
        2 -> { <<"headaches">>, 4};
        3 -> { <<"YALSP">>, 3};
        4 -> { <<"mister">>, 0};
        5 -> { <<"prohibiting">>, 5}
    end.

assert_equal(Name, Expected, Actual) ->
    case Expected -- Actual of 
        [] -> ok;
        Diff -> lager:info("<~p> Expected -- Actual: ~p", [Name, Diff])
    end,
    Actual == Expected.

pb_pid_recycler(Pid, Node) ->
    case riakc_pb_socket:is_connected(Pid) of
        true ->
            Pid;
        _ ->
            rt:pbc(Node)
    end.
    
