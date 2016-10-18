%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
-module(cluster_meta_proxy_server).

-compile(export_all).

-behaviour(gen_server).

%% API
-export([start_link/0, history/0, is_empty/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {history :: [any()],
                last_msg_ts :: non_neg_integer(),
                msg_q :: ets:tab(),
                send_count :: non_neg_integer()}).

-define(SERVER, ?MODULE).
-define(msg_q_ets, msq_q_ets).
-define(send_mode_ets, send_mode_ets).

start_link() ->
  gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

init([]) ->
  maybe_init_ets(?msg_q_ets, [bag, named_table]),
  {ok, #state{history = [], send_count = 0}}.

history() ->
    gen_server:call({global, ?SERVER}, history, infinity).

is_empty(ThresholdSecs) ->
    gen_server:call({global, ?SERVER}, {is_empty, ThresholdSecs}, infinity).

burst_send() ->
    gen_server:call({global, ?SERVER}, burst_send, infinity).

handle_call({is_empty, ThresholdSecs}, _From, State=#state{last_msg_ts=LastMsgTs}) ->
    Now = moment(),
    Reply = case process_info(self(), message_queue_len) of
                {message_queue_len, 0} when (Now - LastMsgTs) > ThresholdSecs ->
                    true;
                {message_queue_len, _} ->
                    false
            end,
    {reply, Reply, State};
handle_call(history, _From, State=#state{history=History}) ->
    {reply, lists:reverse(History), State};
handle_call(burst_send, _From, State) ->
    randomized_send(?msg_q_ets),
    ets:delete_all_objects(?msg_q_ets),
    {reply, ok, State}.

handle_cast({From, Server, To, Msg}, State) ->
    State1 = add_to_history({From, To, Msg}, State),
    State2 = State1#state{ send_count = State1#state.send_count + 1 },
    ets:insert(?msg_q_ets, {State2#state.send_count, Server, To, Msg}),
    {noreply, State2}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

add_to_history(Entry, State=#state{history=History}) ->
    State#state{history=[Entry | History], last_msg_ts = moment()}.

moment() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()).

maybe_init_ets(Tab, Type) ->
  case ets:info(Tab) of
      undefined -> 
          Tab = ets:new(Tab, Type);
      _ ->
          ok
  end.

randomized_send(Tab) ->
    MsgList = ets:tab2list(Tab),
    [send(T) || {_,T} <- lists:sort([{random:uniform(), N} || N <- MsgList])].
    
send({MsgId, Server, To, Msg}) ->        
    lager:info("Running gen_server:cast with id (count):~p Server:~p, To:~p, Msg:~p", 
               [MsgId, Server, To, Msg]),
    gen_server:cast({Server, To}, Msg).
    
