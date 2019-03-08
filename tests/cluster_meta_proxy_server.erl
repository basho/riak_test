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

-compile([export_all, nowarn_export_all]).

-behaviour(gen_server).

%% API
-export([start_link/0, history/0, is_empty/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {history :: [any()],
                last_msg_ts :: non_neg_integer()}).

-define(SERVER, ?MODULE).

start_link() ->
  gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

history() ->
    gen_server:call({global, ?SERVER}, history, infinity).

is_empty(ThresholdSecs) ->
    gen_server:call({global, ?SERVER}, {is_empty, ThresholdSecs}, infinity).

init([]) ->
  {ok, #state{history = []}}.

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
    {reply, lists:reverse(History), State}.

handle_cast({From, Server, To, Msg}, State) ->
%  {Keep, State1} = keep_msg(node_name(From), node_name(To), State),
    State1 = add_to_history({From, To, Msg}, State),
    gen_server:cast({Server, To}, Msg),
%     true -> event_logger:event({dropped, node_name(From), node_name(To), Msg}) end,
    {noreply, State1};
handle_cast(_Msg, State) ->
  {noreply, State}.

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


%% keep_msg(From, To, State) ->
%%   Key = {From, To},
%%   Cfg = State#state.config,
%%   {Keep, Cfg1} =
%%     case proplists:get_value(Key, Cfg, []) of
%%       []             -> {true,  Cfg};
%%       [{keep, N}|Xs] -> {true,  store(Key, [{keep, N - 1} || N > 1] ++ Xs, Cfg)};
%%       [{drop, N}|Xs] -> {false, store(Key, [{drop, N - 1} || N > 1] ++ Xs, Cfg)}
%%     end,
%%   {Keep, State#state{ config = Cfg1 }}.

%% store(Key, [], Cfg) -> lists:keydelete(Key, 1, Cfg);
%% store(Key, Xs, Cfg) -> lists:keystore(Key, 1, Cfg, {Key, Xs}).

%% node_name(Node) ->
%%   list_to_atom(hd(string:tokens(atom_to_list(Node),"@"))).
