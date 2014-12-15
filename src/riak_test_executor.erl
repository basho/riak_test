-module(riak_test_executor).

-behavior(gen_fsm).

%% API
-export([start_link/6,
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

-record(state, {pending_tests :: [atom()],
                running_tests=[] :: [atom()],
                waiting_tests=[] :: [atom()],
                notify_pid :: pid(),
                backend :: atom(),
                upgrade_list :: [string()],
                test_properties :: [proplists:proplist()],
                runner_pids=[] :: [pid()],
                log_dir :: string(),
                report_info :: string()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the test executor
-spec start_link(atom(), atom(), string(), string(), [string()], pid()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Tests, Backend, LogDir, ReportInfo, UpgradeList, NotifyPid) ->
    Args = [Tests, Backend, LogDir, ReportInfo, UpgradeList, NotifyPid],
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

init([Tests, Backend, LogDir, ReportInfo, UpgradeList, NotifyPid]) ->
    State = #state{pending_tests=Tests,
                   backend=Backend,
                   log_dir=LogDir,
                   report_info=ReportInfo,
                   upgrade_list=UpgradeList,
                   notify_pid=NotifyPid},
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

terminate(normal, _StateName, State) ->
    report_done(State),
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
    lager:notice("Starting next test: ~p", [NextTest]),
    %% Find the properties for the next pending test
    {NextTest, TestProps} = lists:keyfind(NextTest, 1, PropertiesList),
    %% Send async request to node manager
    VersionsToTest = versions_to_test(TestProps),
    node_manager:reserve_nodes(rt_properties:get(node_count, TestProps),
                               VersionsToTest,
                               reservation_notify_fun()),
    {next_state, launch_test, State};
request_nodes({test_complete, Test, Pid, Results, Duration}, State) ->
    #state{pending_tests=Pending,
           waiting_tests=Waiting,
           running_tests=Running,
           runner_pids=Pids} = State,
    %% Report results
    report_results(Test, Results, Duration, State),
    UpdState = State#state{running_tests=lists:delete(Test, Running),
                           runner_pids=lists:delete(Pid, Pids),
                           pending_tests=Pending++Waiting,
                           waiting_tests=[]},
    {next_state, request_nodes, UpdState};
request_nodes(_Event, _State) ->
    {next_state, request_nodes, _State}.

launch_test(insufficient_versions_available, State) ->
    #state{pending_tests=[HeadPending | RestPending]} = State,
    report_results(HeadPending, {skipped, insufficient_versions}, 0, State),
    UpdState = State#state{pending_tests=RestPending},
    launch_test_transition(UpdState);
launch_test(not_enough_nodes, State) ->
    %% Move head of pending to waiting and try next test if there is
    %% one left in pending.
    #state{pending_tests=[HeadPending | RestPending],
           waiting_tests=Waiting} = State,
    UpdState = State#state{pending_tests=RestPending,
                           waiting_tests=[HeadPending | Waiting]},
    launch_test_transition(UpdState);
launch_test({nodes, Nodes, NodeMap}, State) ->
    %% Spawn a test runner for the head of pending. If pending is now
    %% empty transition to `wait_for_completion'; otherwise,
    %% transition to `request_nodes'.
    #state{pending_tests=[NextTest | RestPending],
           backend=Backend,
           test_properties=PropertiesList,
           runner_pids=Pids,
           running_tests=Running} = State,
    {NextTest, TestProps} = lists:keyfind(NextTest, 1, PropertiesList),
    UpdTestProps = rt_properties:set([{node_map, NodeMap}, {node_ids, Nodes}],
                                     TestProps),
    Pid = spawn_link(riak_test_runner, start, [NextTest, Backend, UpdTestProps]),
    UpdState = State#state{pending_tests=RestPending,
                           runner_pids=[Pid | Pids],
                           running_tests=[NextTest | Running]},
    launch_test_transition(UpdState);
launch_test({test_complete, Test, Pid, Results, Duration}, State) ->
    #state{pending_tests=Pending,
           waiting_tests=Waiting,
           running_tests=Running,
           runner_pids=Pids} = State,
    %% Report results
    report_results(Test, Results, Duration, State),
    UpdState = State#state{running_tests=lists:delete(Test, Running),
                           runner_pids=lists:delete(Pid, Pids),
                           pending_tests=Pending++Waiting,
                           waiting_tests=[]},
    {next_state, launch_test, UpdState};
launch_test(_Event, _State) ->
    ok.

wait_for_completion({test_complete, Test, Pid, Results, Duration}, State) ->
    #state{pending_tests=Pending,
           waiting_tests=Waiting,
           running_tests=Running,
           runner_pids=Pids} = State,
    %% Report results
    report_results(Test, Results, Duration, State),
    UpdState = State#state{running_tests=lists:delete(Test, Running),
                           runner_pids=lists:delete(Pid, Pids),
                           pending_tests=Pending++Waiting,
                           waiting_tests=[]},
    wait_for_completion_transition(UpdState);
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

report_results(Test, Results, Duration, #state{notify_pid=NotifyPid}) ->
    NotifyPid ! {self(), {test_result, {Test, Results, Duration}}},
     ok.

report_done(#state{notify_pid=NotifyPid}) ->
    NotifyPid ! {self(), done},
    ok.

wait_for_completion_transition(State=#state{pending_tests=[],
                                            running_tests=[]}) ->
    {stop, normal, State};
wait_for_completion_transition(State=#state{pending_tests=[]}) ->
    {next_state, wait_for_completion, State};
wait_for_completion_transition(State) ->
    {next_state, request_nodes, State, 0}.

launch_test_transition(State=#state{pending_tests=[]}) ->
    {next_state, wait_for_completion, State};
launch_test_transition(State) ->
    {next_state, request_nodes, State, 0}.

reservation_notify_fun() ->
    fun(X) ->
            ?MODULE:send_event(X)
    end.

test_properties(Tests, OverriddenProps) ->
    lists:foldl(test_property_fun(OverriddenProps), [], Tests).

test_property_fun(OverrideProps) ->
    fun(TestModule, Acc) ->
            {PropsMod, PropsFun} = riak_test_runner:function_name(properties,
                                                                  TestModule,
                                                                  0,
                                                                  rt_cluster),
            Properties = rt_properties:set(OverrideProps, PropsMod:PropsFun()),
            [{TestModule, Properties} | Acc]
    end.

versions_to_test(Properties) ->
    versions_to_test(Properties, rt_properties:get(rolling_upgrade, Properties)).

%% An `upgrade_path' specified on the command line overrides the test
%% property setting. If the `rolling_upgrade' property is is `false'
%% then the `start_version' property of the test is the only version
%% tested.
versions_to_test(Properties, true) ->
    case rt_properties:get(upgrade_path, Properties) of
        undefined ->
            [rt_properties:get(start_version, Properties)];
        UpgradePath ->
            UpgradePath
    end;
versions_to_test(Properties, false) ->
    [rt_properties:get(start_version, Properties)].

%% Function to abstract away the details of what properties
%% can be overridden on the command line.
override_props(State) ->
    case State#state.upgrade_list of
        undefined ->
            [];
        UpgradeList ->
            [{upgrade_path, UpgradeList}]
    end.
