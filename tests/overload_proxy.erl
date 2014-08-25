%%%-------------------------------------------------------------------
%%% @author Jon Anderson <>
%%% @copyright (C) 2014, Jon Anderson
%%% @doc
%%%
%%% @end
%%% Created : 18 Jul 2014 by Jon Anderson <>
%%%-------------------------------------------------------------------
-module(overload_proxy).

-behaviour(gen_server).
%% API
-export([start_link/0, increment_count/0, get_count/0, is_settled/1, stop/0]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, { get_fsm_count,
                 last_msg_ts
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
start_link() ->
    gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

increment_count() ->
    gen_server:cast({global, ?SERVER}, increment_count, infinity).

get_count() ->
    gen_server:call({global, ?SERVER}, get_count, infinity).

is_settled(ThresholdSecs) ->
      gen_server:call({global, ?SERVER}, {is_settled, ThresholdSecs}, infinity).

stop() ->  
    gen_server:cast({global, ?SERVER}, stop).
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
init([]) ->
    {ok, #state{get_fsm_count = 0}}.

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
handle_call(get_count, _From, State) ->
    Reply = State#state.get_fsm_count,
    {reply, Reply, State};
handle_call({is_settled, ThresholdSecs}, _From, State=#state{last_msg_ts=LastMsgTs}) ->
      Now = moment(),
      Reply = case process_info(self(), message_queue_len) of
                  {message_queue_len, 0} when (Now - LastMsgTs) > ThresholdSecs ->
                      true;
                  {message_queue_len, _} ->
                      false
              end,
      {reply, Reply, State};
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
handle_cast(increment_count, State) ->
    NewState = State#state{get_fsm_count = State#state.get_fsm_count + 1},
    {noreply, NewState};
handle_cast(stop, State) ->
    {stop, normal, State};
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
moment() ->
      calendar:datetime_to_gregorian_seconds(calendar:universal_time()).
