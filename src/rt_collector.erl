-module(rt_collector).

-behaviour(gen_server).

-include_lib("eunit/include/eunit.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { 
    nodes=[],
    search_errors=0,
    search_successes=0,
    search_dont_care=0
    }).

init([Nodes]) ->
    #state{nodes=Nodes}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(search_error, State) ->
    NewState = case rt:is_mixed_cluster(State#state.nodes) of
        true -> State#state{search_dont_care=State#state.search_dont_care + 1};
        _ -> State#state{search_errors=State#state.search_errors + 1}
    end,
    {noreply, NewState};
handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, _State) ->
    ok.

terminate(_Reason, State) ->
    ?assertEqual(0, State#state.search_errors),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.