%% -------------------------------------------------------------------
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
-module(rt_logger).

-behaviour(gen_server).

-export([
         log/2,
         plugin_logger/1,
         unplug_logger/1,
         get_logs/1
]).

-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {logs = [] :: list()}).


log(Log, LoggerConfig) ->
    LagerBackendPid = maps:get(config, LoggerConfig),
    {FModule, FConfig} = maps:get(formatter, LoggerConfig),
    gen_server:cast(LagerBackendPid, {node(), FModule:format(Log, FConfig)}).

plugin_logger(RiakNode) ->
    {Module, Binary, Filename} = code:get_object_code(rt_logger),
    rpc:call(RiakNode, code, load_binary, [Module, Filename, Binary]),
    LocalLogger =
        case whereis(riak_test_logger) of
            undefined ->
                {ok, NewLogger} =
                    gen_server:start(
                        {local, riak_test_logger},
                        ?MODULE, [], []),
                    NewLogger;
            ExistingLogger ->
                ExistingLogger
        end,
    LoggerConfig = maps:put(config, LocalLogger, maps:new()),
    ok  = rpc:call(
        RiakNode,
        logger,
        add_handler,
        [riak_test_logger, ?MODULE, LoggerConfig]).

unplug_logger(RiakNode) ->
    case whereis(riak_test_logger) of
        undefined ->
            ok;
        _ ->
            ok = gen_server:stop(riak_test_logger)
    end,
    ok = rpc:call(RiakNode, logger, remove_handler, [riak_test_logger]).

get_logs(Node) ->
    gen_server:call(riak_test_logger, {get_logs, Node}, 5000).


init([]) ->
    {ok, #state{}}.

handle_call({get_logs, Node}, _From, State) ->
    {NodeLogs, OtherLogs} =
        case lists:keyfind(Node, 1, State#state.logs) of
            {Node, CurrentNodeLogs} ->
                {CurrentNodeLogs, lists:keydelete(Node, 1, State#state.logs)};
            false ->
                {[], State#state.logs}
        end,
    {reply, NodeLogs, State#state{logs = OtherLogs}};
handle_call(Msg, _From, State) ->
    io:format("Not implemented ignoring ~p", [Msg]),
    {reply, not_implemented, State}.

handle_cast({Node, LogEvent}, State) ->
    Logs =
        case lists:keyfind(Node, 1, State#state.logs) of
            {Node, NodeLogs} ->
                lists:keyreplace(
                    Node,
                    1,
                    State#state.logs,
                    {Node, [LogEvent|NodeLogs]});
            false ->
                [{Node, [LogEvent]}|State#state.logs]
        end,
    {noreply, State#state{logs = Logs}}.

handle_info(Msg, State) ->
    io:format("Not implemented ignoring ~p", [Msg]),
    {noreply, State}.

terminate(Reason, _State) ->
    io:format("Closing ~w", [Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.