%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
    rt:wait_for_service(Node, [riak_search,riak_kv,riak_pipe]),

    ListKeysPid = spawn_link(?MODULE, list_keys_tester, [Name, Node, 0, undefined]),

    MapRedPid = spawn_link(?MODULE, mapred_tester, [Name, Node, 0, undefined]),
    
    KVPid = spawn_link(?MODULE, kv_tester, [Name, Node, 0, undefined]),
    
    TwoIPid = case Backend of
        eleveldb ->
            spawn_link(?MODULE, twoi_tester, [Name, Node, 0, undefined]);
        _ -> undefined
    end,

    SearchPid = spawn_link(?MODULE, search_tester, [Name, Node, 0, undefined]),
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

list_keys_tester(Name, Node, Count, Pid) ->
    PBC = pb_pid_recycler(Pid, Node),
    case riakc_pb_socket:list_keys(PBC, <<"objects">>) of
        {ok, Keys} ->
            ActualKeys = lists:usort(Keys),
            ExpectedKeys = lists:usort([loaded_upgrade:int_to_key(K) || K <- lists:seq(0, 100)]),
            case assert_equal(Name, ExpectedKeys, ActualKeys) of
                true -> cool;
                _ -> loaded_upgrade ! {listkeys, not_equal}
            end;
        {error, timeout} ->
            loaded_upgrade ! {listkeys, timeout};
        {error, {timeout, _}} ->
            loaded_upgrade ! {listkeys, timeout};
        Unexpected ->
            loaded_upgrade ! {listkeys, Unexpected}
    end,
    list_keys_tester(Name, Node, Count + 1, PBC).

kv_tester(Name, Node, Count, Pid) ->
    PBC = pb_pid_recycler(Pid, Node),
    Key = Count rem 8000,
    case riakc_pb_socket:get(PBC, loaded_upgrade:bucket(kv), loaded_upgrade:int_to_key(Key)) of
        {ok, Val} ->
            case loaded_upgrade:kv_valgen(Key) == riakc_obj:get_value(Val) of
                true -> cool;
                _ -> loaded_upgrade ! {kv, not_equal}
            end;
        {error, disconnected} ->
            ok;
        Unexpected ->
            loaded_upgrade ! {kv, Unexpected}
    end,
    kv_tester(Name, Node, Count + 1, PBC).

mapred_tester(Name, Node, Count, Pid) ->
    PBC = pb_pid_recycler(Pid, Node),
    %%lager:debug("<~p> mapred test #~p", [Name, Count]),
    case riakc_pb_socket:mapred(PBC, loaded_upgrade:bucket(mapred), loaded_upgrade:erlang_mr()) of
        {ok, [{1, [10000]}]} ->
            ok;
        {ok, _R} ->
            loaded_upgrade ! {mapred, bad_result};
        {error, disconnected} ->
            ok;
        %% Finkmaster Flex says timeouts are ok
        {error, timeout} ->
            ok;
        {error, {timeout, _}} ->
            ok;
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"{badmatch,{'EXIT',noproc}}", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"{badmatch,{'EXIT',timeout}}", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":0,\"error\":\"badarg", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":0,\"error\":\"[preflist_exhausted]", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":0,\"error\":\"{badmatch,{'EXIT',timeout}}", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"function_clause\",\"input\":\"{cover,", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"badarg\",\"input\":\"{cover,", _/binary>>} ->
            ok;
        {error, <<"Error processing stream message: exit:{ucs,{bad_utf8_character_code}}:[{xmerl_ucs,", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":0,\"error\":\"[{vnode_down,{shutdown,{gen_fsm,sync_send_event,", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":0,\"error\":\"[{vnode_down,noproc}]", _/binary>>} ->
            ok;
        Unexpected ->
            loaded_upgrade ! {mapred, Unexpected}
    end,
    mapred_tester(Name, Node, Count + 1, PBC).

twoi_tester(Name, Node, Count, Pid) ->
    PBC = pb_pid_recycler(Pid, Node),
    Key = Count rem 8000,
    ExpectedKeys = [loaded_upgrade:int_to_key(Key)],
    case {
      riakc_pb_socket:get_index(
                              PBC,
                              loaded_upgrade:bucket(twoi),
                              {binary_index, "plustwo"},
                              loaded_upgrade:int_to_key(Key + 2)),
      riakc_pb_socket:get_index(
                              PBC, 
                              loaded_upgrade:bucket(twoi),
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
            end;
        {{error, Reason}, _} ->
            loaded_upgrade ! {twoi, {error, Reason}};
        {_, {error, Reason}} ->
            loaded_upgrade ! {twoi, {error, Reason}};
        Unexpected ->
            loaded_upgrade ! Unexpected
    end,
    twoi_tester(Name, Node, Count + 1, PBC).

search_tester(Name, Node, Count, Pid) ->
    PBC = pb_pid_recycler(Pid, Node),
    {Term, Size} = search_check(Count),
    case riakc_pb_socket:search(PBC, loaded_upgrade:bucket(search), Term) of
        {ok, Result} ->
            ?assertEqual(Size, Result#search_results.num_found);
        {error, disconnected} ->
            %% oh well, reconnect
            ok;

        {error, <<"Error processing incoming message: throw:{timeout,range_loop}:[{riak_search_backend", _/binary>>} ->
            case rt:is_mixed_cluster(Node) of
                true -> 
                    ok;
                _ ->
                    loaded_upgrade ! {search, {timeout, range_loop}}
            end;

        {error,<<"Error processing incoming message: error:{case_clause,", _/binary>>} ->
            %% although it doesn't say so, this is the infamous badfun
            case rt:is_mixed_cluster(Node) of
                true -> 
                    ok;
                _ ->
                    loaded_upgrade ! {search, {error, badfun}}
            end;
        %%{error, Reason} when is_binary(Reason) ->
        %%    case string:str(binary_to_list(Reason), "badfun") of
        %%        0 -> %% This is not badfun, probably a connection issue
        %%            ok;
        %%        _ -> %% this is badfun
        %%            ?assert(rt:is_mixed_cluster(Node))
        %%    end
        Unexpected ->
            loaded_upgrade ! {search, Unexpected}
    end,
    search_tester(Name, Node, Count + 1, PBC).

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

pb_pid_recycler(undefined, Node) ->
    rt:pbc(Node);
pb_pid_recycler(Pid, Node) ->
    case riakc_pb_socket:is_connected(Pid) of
        true ->
            Pid;
        _ ->
            rt:pbc(Node)
    end.
    
