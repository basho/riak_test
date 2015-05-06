%%-------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%% @author Brett Hazen
%% @copyright (C) 2015, Basho Technologies
%% @doc
%%
%% @end
%% Created : 31. Mar 2015 10:25 AM
%%-------------------------------------------------------------------
-module(rt_reporter).
-author("Brett Hazen").

-behaviour(gen_server).

-define(HEADER, [<<"Test">>, <<"Result">>, <<"Reason">>, <<"Test Duration">>]).

%% API
-export([start_link/3,
         stop/0,
         send_result/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    % Collection of log files
    % Running summary of test results: {test, pass/fail, duration}
    summary :: list(),
    log_dir :: string(),
    %% True if results should be uploaded to GiddyUp
    giddyup :: boolean(),
    %% PID of escript used to update results
    notify_pid :: pid()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(boolean(), string(), pid()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(UploadToGiddyUp, LogDir, NotifyPid) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [UploadToGiddyUp, LogDir, NotifyPid], []).

%%--------------------------------------------------------------------
%% @doc
%% Stops the server
%%
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop, infinity).


%% @doc Send an asychronous message to the reporter
%% -spec send_cast(term()) -> ok.
%% send_cast(Msg) ->
%%     gen_server:cast(?MODULE, Msg).

%% @doc Send a sychronous message to the reporter
%% -spec send_call(term()) -> ok.
%% send_call(Msg) ->
%%     gen_server:call(?MODULE, Msg).

%% @doc Send the test result to the reporter
-spec send_result(term()) -> ok.
send_result(Msg) ->
    %% TODO: Determine proper timeout
    gen_server:call(?MODULE, Msg, 30000).


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
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([UploadToGiddyUp, LogDir, NotifyPid]) ->
    {ok, #state{summary=[],
                log_dir=LogDir,
                giddyup=UploadToGiddyUp,
                notify_pid=NotifyPid}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({test_result, Result}, From, State) ->
    Results = State#state.summary,
    State#state.notify_pid ! {From, {test_result, Result}},
    report_and_gather_logs(State#state.giddyup, State#state.log_dir, Result),
    {reply, ok, State#state{summary=[Result|Results]}};
handle_call(done, From, State) ->
    State#state.notify_pid ! {From, done},
    print_summary(State#state.summary, undefined, true),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
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
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
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
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
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
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Dump the summary of all of the log runs to the console
%%
%% @spec print_summary(TestResults, _CoverResult, Verbose) -> ok
%% @end
%%--------------------------------------------------------------------
-spec(print_summary(list(), term(), boolean()) -> ok).
print_summary(TestResults, _CoverResult, Verbose) ->
    %% TODO Log vs console output ... -jsb
    lager:notice("Test Results:"),

    {StatusCounts, RowList} = lists:foldl(fun test_summary_fun/2, {{0,0,0}, []}, TestResults),
    Rows = lists:reverse(RowList),

    case Verbose of
        true ->
            %% TODO: Remove once clique table is fixed
            [lager:debug("ROW ~p", [Row]) || Row <- Rows],
            Table = clique_table:autosize_create_table(?HEADER, Rows),
            [lager:notice(string:tokens(lists:flatten(FormattedRow), "\n")) || FormattedRow <- Table];
        false ->
            ok
    end,

    {PassCount, FailCount, SkippedCount} = StatusCounts,
    lager:notice("---------------------------------------------"),
    lager:notice("~w Tests Failed", [FailCount]),
    lager:notice("~w Tests Skipped", [SkippedCount]),
    lager:notice("~w Tests Passed", [PassCount]),
    Percentage = case PassCount == 0 andalso FailCount == 0 of
                     true -> 0;
                     false -> (PassCount / (PassCount + FailCount + SkippedCount)) * 100
                 end,
    lager:notice("That's ~w% for those keeping score", [Percentage]),

    %% case CoverResult of
    %%     cover_disabled ->
    %%         ok;
    %%     {Coverage, AppCov} ->
    %%         io:format("Coverage : ~.1f%~n", [Coverage]),
    %%         [io:format("    ~s : ~.1f%~n", [App, Cov])
    %%          || {App, Cov, _} <- AppCov]
    %% end,
    ok.

%% @doc Convert Milliseconds into human-readable string
-spec(test_summary_format_time(integer()) -> string()).
test_summary_format_time(Milliseconds) ->
    Mills = trunc(((Milliseconds / 1000000) - (Milliseconds div 1000000)) * 1000000),
    TotalSecs = (Milliseconds - Mills) div 1000000,
    TotalMins = TotalSecs div 60,
    Hours = TotalSecs div 3600,
    Secs = TotalSecs - (TotalMins * 60),
    Mins = TotalMins - (Hours * 60),
    list_to_binary(io_lib:format("~ph ~pm ~p.~ps", [Hours, Mins, Secs, Mills])).

%% @doc Count the number of passed, failed and skipped tests
test_summary_fun(Result = {_, pass, _}, {{Pass, _Fail, _Skipped}, Rows}) ->
    FormattedRow = format_test_row(Result),
    {{Pass+1, _Fail, _Skipped}, [FormattedRow|Rows]};
test_summary_fun(Result = {_, {fail, _}, _}, {{_Pass, Fail, _Skipped}, Rows}) ->
    FormattedRow = format_test_row(Result),
    {{_Pass, Fail+1, _Skipped}, [FormattedRow|Rows]};
test_summary_fun(Result = {_, {skipped, _}, _}, {{_Pass, _Fail, Skipped}, Rows}) ->
    FormattedRow = format_test_row(Result),
    {{_Pass, _Fail, Skipped+1}, [FormattedRow|Rows]}.

%% @doc Format a row for clique
format_test_row({TestPlan, Result, Duration}) ->
    TestName = rt_test_plan:get_name(TestPlan),
    {Status, Reason} = case Result of
        {FailOrSkip, Failure} when is_list(Failure) ->
            {FailOrSkip, lists:flatten(Failure)};
        {FailOrSkip, Failure} ->
            {FailOrSkip, lists:flatten(io_lib:format("~p", [Failure]))};
        pass ->
            {"pass", "N/A"}
    end,
    [TestName, Status, Reason, test_summary_format_time(Duration)].

-spec(report_to_giddyup(term(), list()) -> list).
report_to_giddyup(TestResult, Logs) ->
    {TestPlan, Reason, _Duration} = TestResult,
    giddyup:post_result(TestPlan, Reason),
    [giddyup:post_artifact(TestPlan, Label, Filename) || {Label, Filename} <- Logs].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather all of the log files from the nodes and either upload to
%% GiddyUp or copy them to the directory of your choice. Also upload
%% latest test result, if necessary.
%%
%% @spec report_and_gather_logs(Directory) -> ok
%% @end
%%--------------------------------------------------------------------
-spec(report_and_gather_logs(boolean(), string(), term()) -> ok).
report_and_gather_logs(UploadToGiddyUp, LogDir, TestResult = {TestPlan, _, _}) ->
    SubDir = filename:join([LogDir, rt_test_plan:get_name(TestPlan)]),
    LogFile = filename:join([SubDir, "riak_test.log"]),
    Logs = rt:get_node_logs(LogFile, SubDir),
    case UploadToGiddyUp of
        true ->
            report_to_giddyup(TestResult, Logs);
        _ ->
            Logs
    end.

%%
%% RetList = [{test, TestModule}, {status, Status}, {log, Log}, {backend, Backend} | proplists:delete(backend, TestMetaData)],
%% case Status of
%% fail -> RetList ++ [{reason, iolist_to_binary(io_lib:format("~p", [Reason]))}];
%% _ -> RetList
%% end.
