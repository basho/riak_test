-module(riak_test_lager_backend).

-behavior(gen_event).

-export([init/1,
         handle_call/2,
         handle_event/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {level, verbose, log = []}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile([{parse_transform, lager_transform}]).
-endif.

-include("deps/lager/include/lager.hrl").

init(Level) when is_atom(Level) ->
    case lists:member(Level, ?LEVELS) of
        true ->
            {ok, #state{level=lager_util:level_to_num(Level), verbose=false}};
        _ ->
            {error, bad_log_level}
    end;
init([Level, Verbose]) ->
    case lists:member(Level, ?LEVELS) of
        true ->
            {ok, #state{level=lager_util:level_to_num(Level), verbose=Verbose}};
        _ ->
            {error, bad_log_level}
    end.
    
handle_event({log, Dest, Level, {Date, Time}, [LevelStr, Location, Message]},
    #state{level=L, verbose=Verbose, log = Logs} = State) when Level > L ->
    case lists:member(riak_test_lager_backend, Dest) of
        true ->
            Log = case Verbose of
                true ->
                    [Date, " ", Time, " ", LevelStr, Location, Message];
                _ ->
                    [Time, " ", LevelStr, Message]
            end,
            {ok, State#state{log=[Log|Logs]}};
        false ->
            {ok, State}
    end;
handle_event({log, Level, {Date, Time}, [LevelStr, Location, Message]},
  #state{level=LogLevel, verbose=Verbose, log = Logs} = State) when Level =< LogLevel ->
    Log = case Verbose of
        true ->
            [Date, " ", Time, " ", LevelStr, Location, Message];
        _ ->
            [Time, " ", LevelStr, Message]
        end,
    {ok, State#state{log=[Log|Logs]}};
handle_event(_Event, State) ->
    {ok, State}.

handle_call(get_loglevel, #state{level=Level} = State) ->
    {ok, Level, State};
handle_call({set_loglevel, Level}, State) ->
    case lists:member(Level, ?LEVELS) of
        true ->
            {ok, ok, State#state{level=lager_util:level_to_num(Level)}};
        _ ->
            {ok, {error, bad_log_level}, State}
    end;
handle_call(_, State) -> 
    {ok, ok, State}.

handle_info(_, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{log=Logs}) ->
    {ok, lists:reverse(Logs)}.
