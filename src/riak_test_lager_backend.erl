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

%% @doc This lager backend keeps a buffer of logs in memory and returns them all
%% when the handler terminates.

-module(riak_test_lager_backend).

-behavior(gen_event).

%% gen_event callbacks
-export([init/1,
         handle_call/2,
         handle_event/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% holds the log messages for retreival on terminate
-record(state, {level, verbose, log = []}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile([{parse_transform, lager_transform}]).
-endif.

-include_lib("lager/include/lager.hrl").

-spec(init(integer()|atom()|[term()]) -> {ok, #state{}} | {error, atom()}).
%% @private
%% @doc Initializes the event handler
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

-spec(handle_event(tuple(), #state{}) -> {ok, #state{}}).
%% @private
%% @doc handles the event, adding the log message to the gen_event's state.
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

-spec(handle_call(any(), #state{}) -> {ok, any(), #state{}}).
%% @private
%% @doc gets and sets loglevel. This is part of the lager backend api.
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

-spec(handle_info(any(), #state{}) -> {ok, #state{}}).
%% @private
%% @doc gen_event callback, does nothing.
handle_info(_, State) ->
    {ok, State}.

-spec(code_change(any(), #state{}, any()) -> {ok, #state{}}).
%% @private
%% @doc gen_event callback, does nothing.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec(terminate(any(), #state{}) -> {ok, list()}).
%% @doc gen_event callback, does nothing.
terminate(_Reason, #state{log=Logs}) ->
    {ok, lists:reverse(Logs)}.

-ifdef(TEST).

log_test_() ->
    {foreach,
        fun() ->
                error_logger:tty(false),
                application:load(lager),
                application:set_env(lager, handlers, [{riak_test_lager_backend, debug}]),
                application:set_env(lager, error_logger_redirect, false),
                lager:start()
        end,
        fun(_) ->
                application:stop(lager),
                error_logger:tty(true)
        end,
        [
            {"Test logging",
                fun() ->
                        lager:info("Here's a message"),
                        lager:debug("Here's another message"),
                        {ok, Logs} = gen_event:delete_handler(lager_event, riak_test_lager_backend, []),
                        ?assertEqual(3, length(Logs)),
                        
                        ?assertMatch([_, "[debug]", "Lager installed handler riak_test_lager_backend into lager_event"], re:split(lists:nth(1, Logs), " ", [{return, list}, {parts, 3}])),
                        ?assertMatch([_, "[info]", "Here's a message"], re:split(lists:nth(2, Logs), " ", [{return, list}, {parts, 3}])),
                        ?assertMatch([_, "[debug]", "Here's another message"], re:split(lists:nth(3, Logs), " ", [{return, list}, {parts, 3}]))
                        
                end
            }
        ]
    }.


set_loglevel_test_() ->
    {foreach,
        fun() ->
                error_logger:tty(false),
                application:load(lager),
                application:set_env(lager, handlers, [{riak_test_lager_backend, info}]),
                application:set_env(lager, error_logger_redirect, false),
                lager:start()
        end,
        fun(_) ->
                application:stop(lager),
                error_logger:tty(true)
        end,
        [
            {"Get/set loglevel test",
                fun() ->
                        ?assertEqual(info, lager:get_loglevel(riak_test_lager_backend)),
                        lager:set_loglevel(riak_test_lager_backend, debug),
                        ?assertEqual(debug, lager:get_loglevel(riak_test_lager_backend))
                end
            },
            {"Get/set invalid loglevel test",
                fun() ->
                        ?assertEqual(info, lager:get_loglevel(riak_test_lager_backend)),
                        ?assertEqual({error, bad_log_level},
                            lager:set_loglevel(riak_test_lager_backend, fatfinger)),
                        ?assertEqual(info, lager:get_loglevel(riak_test_lager_backend))
                end
            }

        ]
    }.

-endif.
