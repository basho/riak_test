-module(riak_test_executor).

-behavior(gen_fsm).

%% API
-export([start_link/5,
         send_event/1,
         stop/0]).

%% gen_fsm callbacks
-export([init/1,
         gather_properties/2,
         gather_properties/3,
         request_nodes/2,
         request_nodes/3,
         launch_test/2,
         launch_test/3,
         wait_for_completion/2,
         wait_for_completion/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-type execution_mode() :: serial | parallel.
-record(state, {pending_tests :: [rt_test_plan:test_plan()],
                running_tests=[] :: [rt_test_plan:test_plan()],
                waiting_tests=[] :: [rt_test_plan:test_plan()],
                upgrade_list :: [string()],
                test_properties :: [proplists:proplist()],
                runner_pids=[] :: [pid()],
                log_dir :: string(),
                execution_mode :: execution_mode(),
                continue_on_fail :: boolean(),
                reporter_pid :: pid()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the test executor
-spec start_link([rt_test_plan:test_plan()], string(), string()|giddyup, [string()], pid()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Tests, LogDir, Platform, UpgradeList, NotifyPid) ->
    Args = [Tests, LogDir, Platform, UpgradeList, NotifyPid],
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, Args, []).

send_event(Msg) ->
    gen_fsm:send_event(?MODULE, Msg).

%% @doc Stop the executor
-spec stop() -> ok | {error, term()}.
stop() ->
    gen_fsm:sync_send_all_state_event(?MODULE, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([Tests, LogDir, Platform, UpgradeList, NotifyPid]) ->
    %% TODO Change the default when parallel execution support is implemented -jsb
    ExecutionMode = rt_config:get(rt_execution_mode, serial),

    ContinueOnFail = rt_config:get(continue_on_fail),

    UploadToGiddyUp = case Platform of
                      undefined -> false;
                      _ -> true
                  end,
    {ok, Reporter} = rt_reporter:start_link(UploadToGiddyUp, LogDir, NotifyPid),

    lager:notice("Starting the Riak Test executor in ~p execution mode", [ExecutionMode]),
    State = #state{pending_tests=Tests,
                   log_dir=LogDir,
                   upgrade_list=UpgradeList,
                   execution_mode=ExecutionMode,
                   continue_on_fail=ContinueOnFail,
                   reporter_pid=Reporter},
    {ok, gather_properties, State, 0}.

%% @doc there are no all-state events for this fsm
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Handle synchronous events that should be handled
%% the same regardless of the current state.
-spec handle_sync_event(term(), term(), atom(), #state{}) ->
                               {reply, term(), atom(), #state{}}.
handle_sync_event(_Event, _From, _StateName, _State) ->
    {reply, ok, ok, _State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(normal, _StateName, _State) ->
    rt_reporter:send_result(done),
    rt_reporter:stop(),
    ok;
terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc this fsm has no special upgrade process
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%% Asynchronous call handling functions for each FSM state

%% TODO: Modify property gathering to account for `upgrade_path'
%% specified via the command line and replace accordingly in
%% properties record.
gather_properties(timeout, State) ->
    OverrideProps = override_props(State),
Properties = test_properties(State#state.pending_tests, OverrideProps),
    {next_state, request_nodes, State#state{test_properties=Properties}, 0};
gather_properties(_Event, _State) ->
    {next_state, gather_properties, _State}.

request_nodes(timeout, State) ->
    #state{pending_tests=[NextTest | _],
           test_properties=PropertiesList} = State,
    %% Find the properties for the next pending test
    {NextTest, TestProps} = lists:keyfind(NextTest, 1, PropertiesList),
    
    ok = maybe_reserve_nodes(NextTest, TestProps),
    
    {next_state, launch_test, State};
request_nodes({test_complete, Test, Pid, _Results}, State) ->
    #state{pending_tests=Pending,
           waiting_tests=Waiting,
           running_tests=Running,
           runner_pids=Pids,
           execution_mode=ExecutionMode}= State,
    UpdState = State#state{running_tests=lists:delete(Test, Running),
                           runner_pids=lists:delete(Pid, Pids),
                           pending_tests=Pending++Waiting,
                           waiting_tests=[],
                           execution_mode=ExecutionMode},
    {next_state, request_nodes, UpdState};
request_nodes(_Event, _State) ->
    {next_state, request_nodes, _State}.

launch_test(insufficient_versions_available, State) ->
    lager:debug("riak_test_executor:launch_test insufficient_versions_available"),
    #state{pending_tests=[HeadPending | RestPending],
           execution_mode=ExecutionMode} = State,
    rt_reporter:send_result({test_result, {HeadPending, {skipped, insufficient_versions}, 0}}),
    UpdState = State#state{pending_tests=RestPending,
                           execution_mode=ExecutionMode},
    launch_test_transition(UpdState);
launch_test(not_enough_nodes, State) ->
    %% Move head of pending to waiting and try next test if there is
    %% one left in pending.
    lager:debug("riak_test_executor:launch_test not_enough_nodes"),
    #state{pending_tests=[HeadPending | RestPending],
           waiting_tests=Waiting,
           execution_mode=ExecutionMode} = State,
    rt_reporter:send_result({test_result, {HeadPending, {skipped, not_enough_nodes}, 0}}),
    UpdState = State#state{pending_tests=RestPending,
                           waiting_tests=[HeadPending | Waiting],
                           execution_mode=ExecutionMode},
    launch_test_transition(UpdState);
launch_test({nodes, Nodes, NodeMap}, State) ->
    %% Spawn a test runner for the head of pending. If pending is now
    %% empty transition to `wait_for_completion'; otherwise,
    %% transition to `request_nodes'.
    #state{pending_tests=[NextTestPlan | RestPending],
           execution_mode=ExecutionMode,
           test_properties=PropertiesList,
           runner_pids=Pids,
           running_tests=Running,
           continue_on_fail=ContinueOnFail,
           reporter_pid=ReporterPid,
           log_dir=LogDir} = State,
    NextTestModule = rt_test_plan:get_module(NextTestPlan),
    lager:debug("Executing test ~p in mode ~p", [NextTestModule, ExecutionMode]),
    {NextTestPlan, TestProps} = lists:keyfind(NextTestPlan, 1, PropertiesList),
    UpdTestProps = rt_properties:set([{node_map, NodeMap}, {node_ids, Nodes}],
                                     TestProps),
    {RunnerPids, RunningTests} = run_test(ExecutionMode, NextTestPlan, UpdTestProps,
                                          Pids, Running, ContinueOnFail, ReporterPid, LogDir),
    UpdState = State#state{pending_tests=RestPending,
                           execution_mode=ExecutionMode,
                           runner_pids=RunnerPids,
                           running_tests=RunningTests},

    launch_test_transition(UpdState);
launch_test({test_complete, TestPlan, Pid, _Results}, State) ->
    #state{pending_tests=Pending,
           waiting_tests=Waiting,
           running_tests=Running,
           runner_pids=Pids,
           execution_mode=ExecutionMode} = State,
    UpdState = State#state{running_tests=lists:delete(TestPlan, Running),
                           runner_pids=lists:delete(Pid, Pids),
                           pending_tests=Pending++Waiting,
                           waiting_tests=[],
                           execution_mode=ExecutionMode},
    {next_state, launch_test, UpdState};
launch_test(Event, State) ->
    lager:error("Unknown event ~p with state ~p.", [Event, State]),
    ok.

maybe_reserve_nodes(NextTestPlan, TestProps) ->
    %% TODO: Clean up upgrade resolution.  Go either with executor or test plan.
    %% VersionsToTest = versions_to_test(TestProps),
    VersionsToTest = [rt_config:convert_to_string(rt_test_plan:get(version, NextTestPlan))],
    maybe_reserve_nodes(erlang:function_exported(rt_test_plan:get_module(NextTestPlan), confirm, 1),
                        NextTestPlan, VersionsToTest, TestProps).

maybe_reserve_nodes(true, NextTest, VersionsToTest, TestProps) ->
    NodeCount = rt_properties:get(node_count, TestProps),
    
    %% Send async request to node manager
    lager:notice("Requesting ~p nodes for the next test, ~p", [NodeCount, rt_test_plan:get_name(NextTest)]),
    node_manager:reserve_nodes(NodeCount,
                               VersionsToTest,
                               reservation_notify_fun());
maybe_reserve_nodes(false, NextTest, VersionsToTest, _TestProps) ->
    lager:warning("~p is an old style test that requires conversion.", [rt_test_plan:get_name(NextTest)]),
    node_manager:reserve_nodes(0, VersionsToTest, reservation_notify_fun()),
    ok.

wait_for_completion({test_complete, Test, Pid, Results}, State) ->
    lager:debug("Test ~p complete", [rt_test_plan:get_module(Test)]),
    #state{pending_tests=Pending,
           waiting_tests=Waiting,
           running_tests=Running,
           runner_pids=Pids,
           execution_mode=ExecutionMode} = State,
    UpdState = State#state{running_tests=lists:delete(Test, Running),
                           runner_pids=lists:delete(Pid, Pids),
                           pending_tests=Pending++Waiting,
                           waiting_tests=[],
                           execution_mode=ExecutionMode},
    wait_for_completion_transition(Results, UpdState);
wait_for_completion(_Event, _State) ->
    ok.

%% Synchronous call handling functions for each FSM state

gather_properties(_Event, _From, _State) ->
    ok.

request_nodes(_Event, _From, _State) ->
    ok.

launch_test(_Event, _From, _State) ->
    ok.

wait_for_completion(_Event, _From, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

wait_for_completion_transition({_Status, _Reason}, State=#state{continue_on_fail=ContinueOnFail}) when ContinueOnFail == false ->
    {stop, normal, State};
wait_for_completion_transition(_Result, State=#state{pending_tests=[],
                                            running_tests=[]}) ->
    {stop, normal, State};
wait_for_completion_transition(_Result, State=#state{pending_tests=[]}) ->
    {next_state, wait_for_completion, State};
wait_for_completion_transition(_Result, State) ->
    {next_state, request_nodes, State, 0}.

launch_test_transition(State=#state{pending_tests=PendingTests,
                                    execution_mode=ExecutionMode}) when PendingTests == [] orelse ExecutionMode == serial ->
    PendingModules = [rt_test_plan:get_module(Test) || Test <- PendingTests],
    lager:debug("Waiting for completion: execution mode ~p with pending tests ~p", [ExecutionMode, PendingModules]),
    {next_state, wait_for_completion, State};
launch_test_transition(State) ->
    {next_state, request_nodes, State, 0}.

%%launch_test_transition(State) ->
%%    {next_state, wait_for_completion, State}.

reservation_notify_fun() ->
    fun(X) ->
            ?MODULE:send_event(X)
    end.

test_properties(Tests, OverriddenProps) ->
    lists:foldl(test_property_fun(OverriddenProps), [], Tests).

test_property_fun(OverrideProps) ->
    fun(TestPlan, Acc) ->
            {PropsMod, PropsFun} = riak_test_runner:function_name(properties,
                                                                  rt_test_plan:get_module(TestPlan),
                                                                  0,
                                                                  rt_cluster),
            Properties = rt_properties:set(OverrideProps, PropsMod:PropsFun()),
            [{TestPlan, Properties} | Acc]
    end.

%% An `upgrade_path' specified on the command line overrides the test
%% property setting. If the `rolling_upgrade' property is is `false'
%% then the `start_version' property of the test is the only version
%% tested.
%% versions_to_test(Properties) ->
%%     versions_to_test(Properties, rt_properties:get(rolling_upgrade, Properties)).
%%
%% versions_to_test(Properties, true) ->
%%     case rt_properties:get(upgrade_path, Properties) of
%%         undefined ->
%%             versions_to_test(Properties, false);
%%         UpgradePath ->
%%             [rt_config:convert_to_string(Upgrade) || Upgrade <- UpgradePath]
%%     end;
%% versions_to_test(Properties, false) ->
%%     InitialVersion = rt_properties:get(start_version, Properties),
%%     [rt_config:convert_to_string(InitialVersion)].

%% Function to abstract away the details of what properties
%% can be overridden on the command line.
override_props(State) ->
    case State#state.upgrade_list of
        undefined ->
            [];
        UpgradeList ->
            [{upgrade_path, UpgradeList}]
    end.

-spec run_test(parallel | serial, atom(), proplists:proplist(), [pid()], [rt_test_plan:test_plan()], boolean(), pid(), string()) -> {[pid()], [atom()]}.
run_test(parallel, TestPlan, Properties, RunningPids, RunningTests, ContinueOnFail, ReporterPid, LogDir) ->
    Pid = spawn_link(riak_test_runner, start, [TestPlan, Properties, ContinueOnFail, ReporterPid, LogDir]),
    {[Pid | RunningPids], [TestPlan | RunningTests]};
run_test(serial, TestPlan, Properties, RunningPids, RunningTests, ContinueOnFail, ReporterPid, LogDir) ->
    riak_test_runner:start(TestPlan, Properties, ContinueOnFail, ReporterPid, LogDir),
    {RunningPids, RunningTests}.

