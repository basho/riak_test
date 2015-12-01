%%
%% QuickCheck test to verify that the vnode proxy overloads when the
%% mailbox estimate is in overload and that it recovers from overload
%% state once the vnode has processed the message queue.  In the real
%% system, messages are sent to the vnode outside of the proxy so this
%% is also modelled.
%%
%% Properties -
%%   1) If the vnode message queue has cleared, the vnode proxy should not
%%   be in overload.
%%   2) If the proxy has been sent more messages than the overload threshold
%%      while the vnode is suspended, it should be in overload.
%%   3) If the proxy has been sent less than or equal to the overload threshold
%%      it should not be in overload.
%%
%% Whenever the vnode is resumed, the test checks the message queues are
%% cleared and the model is reset.  The main goal is to advance the proxy
%% into interesting new states.
%%
%% This test can be run outside of riak_test while working on it. 
%% Symlink the source into a release build and run
%%   c(proxy_overload_recovery). 
%%   proxy_overload_recovery:run(300). % Run for 5 mins
%%
%% On failure you can re-run counter examples *and* print out the internal
%% state with the run.
%%   proxy_overload_recovery:check(). 
%%
%% TODO/Questions:
%% 1) Is there a better way to do the initialization step?
%% 2) Remove the riak_kv dependency so it's a pure riak_core test.

-module(proxy_overload_recovery).
-behaviour(riak_test).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-export([confirm/0]).
-compile(export_all).

confirm() ->
    %% disable AAE to prevent vnode start delay opening AAE leveldb
    Opts = [{riak_kv, [{anti_entropy, {off, []}}]}],
    Nodes = [Node1] = rt:deploy_nodes(1, Opts),

    case setup_eqc(Nodes) of
        ok ->
            %% Distribute this code to the remote and add EQC
            rt:load_modules_on_nodes([?MODULE], Nodes),

            TestingTime = rt_config:get(eqc_testing_time, 120),
            lager:info("Running vnode proxy overload property for ~p seconds\n",
                       [TestingTime]),
            ?assertEqual(true, rpc:call(Node1, ?MODULE, rtrun, [TestingTime])),
            pass;
        _ ->
            lager:warning("EQC was unavailable on this machine - PASSing "
                          "the test, but it did not run. Wish we could skip.\n", []),
            pass
    end.




-record(tstate, {
          rt, % Runtime information - either symbolic state or #rt{} record
          vnode_running = true, % true if running, false if suspended
          proxy_msgs = 0,  % messages proxied while vnode suspended
          direct_msgs = 0, % messages sent directly while vnode suspended
          threshold}).     % threshold for overload
-record(rt, {id, % Partition id being used for the test
             pl, % Preflist to send messages via the proxy
             ppid,  % Proxy pid
             vpid}).% Vnode pid

%% Riak test run - to be executed on the node
rtrun(Seconds) ->
    case run(Seconds) of
        true ->
            true;
        Res ->
            %% Hopefully rpc will set the group leader to r_t so we can see
            io:format("=== FAILED - running check to see if it was "
                      "transient and print model states ===\n", []),
            check(),
            Res
    end.

%%
%% Runners - on failure may be helpful to connect to the node and run
%% check / recheck to verify a fix or try reshrinking.
%%
run() ->
    run(60).

run(Seconds) ->
    eqc:quickcheck(eqc:testing_time(Seconds, ?MODULE:prop_proxy_recovery())).

trap_run(Seconds) ->
    eqc:quickcheck(eqc:testing_time(Seconds, ?TRAPEXIT(?MODULE:prop_proxy_recovery()))).

check() ->
    eqc:check(eqc_statem:show_states(?MODULE:prop_proxy_recovery())).

recheck() ->
    eqc:recheck(?MODULE:prop_proxy_recovery()).

cover(Seconds) ->
    cover:start(),
    cover:compile(?MODULE),
    eqc:quickcheck(eqc:testing_time(Seconds, ?MODULE:prop_proxy_recovery())),
    cover:analyze_to_file(?MODULE),
    cover:stop().

%%
%% Quickcheck Property - setup and run the statem
%%
prop_proxy_recovery() ->
    ?FORALL(Cmds,commands(?MODULE),
            begin
                Start = os:timestamp(),
                {H,S,Res} = run_commands(?MODULE, Cmds),
                End = os:timestamp(),
                Msecs = timer:now_diff(End, Start) div 1000,
                ?WHENFAIL(
                   begin
                       RT = S#tstate.rt,
                       case RT of
                           undefined ->
                               ok;
                           #rt{ppid = PPid, vpid = VPid} ->
                               eqc:format("\n\nPPid state = ~w\n",
                                          [catch sys:get_status(PPid)]),
                               eqc:format("Vnode MsgQLen = ~p\n",
                                          [catch msgq_len(VPid)])
                       end
                   end,
                   measure(duration, Msecs, 
                   aggregate(with_title("Commands"), command_names(Cmds),
                             pretty_commands(?MODULE, Cmds, {H, S, Res},
                                             Res == ok))))
            end).
%%
%% Generators
%%
pos_int() ->
    ?LET(X, int(), 1 + abs(X)).

%% Generate the threshold seed - see get_params
threshold_seed() ->
    frequency([%% test proxy disabled - 1%
               {1, undefined},
               %% test with all parameters given
               {4, {choose(1,25),choose(1,25),choose(1,50)}},
               %% test with the interval and threshold
               {5, {choose(1, 50), choose(1, 50)}},
               %% just provide a threshold, defaults for others
               {90, choose(4, 100)}]).

%%
%% initial_state
%%
initial_state() ->
    #tstate{}.

%%
%% Run prepare once before any other calls and then never again.
%%
precondition_common(#tstate{rt = undefined}, {call, _M, F, _A}) ->
    F == prepare;
precondition_common(_, {call, _M, F, _A}) ->
    F /= prepare.

%% %% Make sure we're still running what we think we're running - uncomment 
%% %% if having process death issues
%% invariant(#tstate{rt = undefined}) ->
%%     true;
%% invariant(#tstate{rt = #rt{id = Index, ppid = PPid, vpid = VPid}}) ->
%%     RegName = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Index),
%%     PPid = whereis(RegName), % Check process we think it is.    
%%     true = is_process_alive(PPid),
%%     true = is_process_alive(VPid),
%%     true.


%%
%% Prepare runtime state, pick a vnode and find pids - precondition_common
%% ensures this runs first.
%%
prepare_args(_S) ->
    [threshold_seed()].

prepare(ThresholdSeed) ->
    {RequestInterval, Interval, Threshold} = get_params(ThresholdSeed),
    Index = 0, % Mess with the same vnode proxy each time - could rotate if we wanted

    %% Set (or unset) appenvs for the test
    prep_env(vnode_check_request_interval, RequestInterval),
    prep_env(vnode_check_interval, Interval),
    prep_env(vnode_overload_threshold, Threshold),
    prep_env(inactivity_timeout, infinity),

    %% Reset the vnode and the proxy to get back into a known state.
    %% Ask the supervisor to do it for us to avoid messy messages in the logs.
    Id = 0,
    {ok, VPid0} = riak_core_vnode_manager:get_vnode_pid(Id, riak_kv_vnode),
    sys:resume(VPid0),
    ok = supervisor:terminate_child(riak_core_vnode_sup, VPid0),
    false = is_process_alive(VPid0),

    %% Reset the proxy pid to make sure it resets state and picks up the new
    %% environment variables
    ok = supervisor:terminate_child(riak_core_vnode_proxy_sup, {riak_kv_vnode, Id}),
    RegName = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Index),
    undefined = whereis(RegName),
    %% Fail if we get back the dead vnode
    {ok, VPid1} = riak_core_vnode_manager:get_vnode_pid(Index, riak_kv_vnode),
    case VPid1 of
        VPid0 ->
            lager:debug("Vnode PID didn't change after killing vnode - was ~p", [VPid0]),
            VnodeTab = riak_core_vnode_manager:get_tab(),
            lager:debug("~w", [VnodeTab]),
            file:write_file("vnode_tab.terms", term_to_binary(VnodeTab)),
            error("VNode PID didn't change");
        _ ->
            ok
    end,

    {ok, PPid} = supervisor:restart_child(riak_core_vnode_proxy_sup, {riak_kv_vnode, Id}),

    %% Find the proxy pid and check it's alive and matches the supervisor
    RegName = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Index),
    PPid = whereis(RegName),

    %% Send a message through the vnode proxy (the timeout message is ignored)
    %% and return the Pid so we know we have the same Pid.
    {ok, VPid} = riak_core_vnode_proxy:command_return_vnode(
                   {riak_kv_vnode,Index,node()}, timeout),
    ?assertEqual(VPid, VPid1),

    true = is_process_alive(PPid),
    true = is_process_alive(VPid),
    #rt{id = Id, pl = {Id, node()}, ppid = PPid, vpid = VPid}.

prepare_next(S, RT, [ThresholdSeed]) ->
    {_RequestInterval, _Interval, Threshold} = get_params(ThresholdSeed),
    S#tstate{rt = RT, threshold = Threshold}.


%%
%% Suspend the vnode so it cannot process messages and builds
%% up the vnode message queue
%%
suspend_pre(#tstate{vnode_running = Running}) ->
    Running.

suspend_args(#tstate{rt = RT}) ->
    [RT].

suspend(#rt{vpid = VPid}) ->
    sys:suspend(VPid).

suspend_next(S, _V, _A) ->
    S#tstate{vnode_running = false}.

%%
%% Resume the vnode and wait until it has completed
%% processing all queued messages
%%
resume_pre(#tstate{vnode_running = Running}) ->
    not Running.

resume_args(#tstate{rt = RT}) ->
    [RT].

resume(#rt{ppid = PPid, vpid = VPid}) ->
    sys:resume(VPid),
    %% Use the sys:get_status call to force a synchronous call
    %% against the vnode proxy to ensure all messages sent by
    %% this process have been serviced and there are no pending
    %% 'ping's in the vnode before we continue.
    %% Then drain the vnode to make sure any pending pongs have
    %% been sent.
    ok = drain(VPid),
    _ = sys:get_status(VPid),
    _ = sys:get_status(PPid).

resume_next(S, _V, _A) ->
    S#tstate{vnode_running = true, proxy_msgs = 0, direct_msgs = 0}.

%%
%% Send messages through the proxy process, do not
%% wait for responses, use ignore so riak_core_vnode:reply
%% will not send any responses
%%
proxy_msg_args(#tstate{rt = RT}) ->
    [RT, pos_int()].

proxy_msg(#rt{pl = PL}, NumMsgs) ->
    [riak_kv_vnode:get(PL, {<<"b">>,<<"k">>}, ignore) ||
        _X <- lists:seq(1, NumMsgs)],
    ok.

proxy_msg_next(#tstate{vnode_running = false, proxy_msgs = ProxyMsgs} = S, _V,
               [_RT, NumMsgs]) ->
    S#tstate{proxy_msgs = ProxyMsgs + NumMsgs};
proxy_msg_next(S, _V, _A) ->
    S.

%%
%% Send message directly to the core vnode process, simulating
%% the vnode manager or other processes sending additional messages
%% not accounted for by the proxy.
%%
direct_msg_args(#tstate{rt = RT}) ->
    [RT, pos_int()].

direct_msg(#rt{vpid = VPid}, NumMsgs) ->
    [catch gen_fsm:sync_send_event(VPid, {ohai, X}, 0) ||
        X <- lists:seq(1, NumMsgs)],
    ok.

direct_msg_next(#tstate{vnode_running = false, direct_msgs = DirectMsgs} = S,
                _V, [_RT, NumMsgs]) ->
    S#tstate{direct_msgs = DirectMsgs + NumMsgs};
direct_msg_next(S, _V, _A) ->
    S.


%%
%% Check the current state of the vnode so we can test properties
%%
overloaded_args(#tstate{vnode_running = Running, rt = RT}) ->
    [Running, RT].

overloaded(Running, #rt{ppid = PPid, vpid = VPid}) ->
    case Running of
        true ->
            ok = drain([PPid, VPid]);
        _ ->
            ok
    end,
    {messages, PMsgs} = process_info(PPid, messages),
    {messages, VMsgs} = process_info(VPid, messages),
    Overloaded = riak_core_vnode_proxy:overloaded(PPid),
    {Overloaded, {VMsgs, PMsgs}, sys:get_status(PPid)}.

overloaded_post(#tstate{threshold = undefined}, _A,
                {R, _Messages, _ProxyStatus}) ->
    %% If there are no thresholds there should never be an overload
    eq(R, false);
overloaded_post(#tstate{vnode_running = true}, _A,
                {R, _Messages, _ProxyStatus}) ->
    %% If the vnode is running, we have cleared queues so
    %% should not be in overload.
    eq(R, false);
overloaded_post(#tstate{vnode_running = false,
                        proxy_msgs = ProxyMsgs,
                        threshold = Threshold}, _A,
                {ResultOverload, _Messages, _ProxyStatus}) ->
    %% Either
    %%   mailbox is completely an estimate based on proxy msgs
    %%   or mailbox is a check + estimate since
    %% The test cannot tell which as prior state may have left the counter
    %% at any value between 0 and Interval.
    %%
    if
        ProxyMsgs > Threshold ->  % definitely overloaded
            eq(ResultOverload, true);
        true ->
            true % otherwise, not possible to check - vnode_resume gets us back
    end.

%%
%% Test helpers
%%

%% Convert the test seed to request intervals, intervals and threshold
%% if unset, remove from the app env so the proxy init auto-configures
get_params(undefined) ->
    {unset, unset, undefined};
get_params({Interval, ThresholdDelta}) ->
    {unset, % let the proxy allocate it
     Interval,
     Interval + ThresholdDelta};
get_params({RequestInterval, IntervalDelta, ThresholdDelta}) ->
    {RequestInterval,
     RequestInterval + IntervalDelta,
     RequestInterval + IntervalDelta + ThresholdDelta};
get_params(Threshold) when Threshold > 5000 ->
    {unset, unset, Threshold};
get_params(Threshold) when Threshold < 5000 ->
    %% Avoid annoying message about safety
    {unset, Threshold div 2, Threshold}.


%% Set (or unset) params
prep_env(Var, unset) ->
    application:unset_env(riak_core, Var, infinity);
prep_env(Var, Val) ->
    application:set_env(riak_core, Var, Val).

%%
%% Wait until all messages are drained by the Pid. No guarantees
%% about future messages being sent, or that responses for the
%% last message consumed have been transmitted.
%%
drain(Pid) when is_pid(Pid) ->
    drain([Pid]);

drain(Pids) when is_list(Pids) ->
    _ = [sys:suspend(Pid) || Pid <- Pids],
    Len = lists:foldl(fun(Pid, Acc0) ->
            {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
            Acc0 + Len
            end, 0, Pids),
    _ = [sys:resume(Pid) || Pid <- Pids],
    case Len of
        0 ->
            ok;
        _ ->
            timer:sleep(1),
            drain(Pids)
    end.

%% Return the length of the message queue (or crash if proc dead)
msgq_len(Pid) ->
    {message_queue_len, L} = process_info(Pid, message_queue_len),
    L.

%% Kill the process and wait for it to exit
kill_and_wait(Pid) ->
    Mref = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', Mref, _, _, _} ->
            ok
    end.

%%
%% Check if EQC is available on the local node, then add to remotes if needed
%% This could be refactored out if many tests need it.
setup_eqc(Nodes) ->
    try
        _ = eqc:reserved_until(), % will throw exception if problems
        add_eqc_apps(Nodes)
    catch
        _:Err ->
            lager:info("EQC unavailable: ~p\n", [Err]),
            {error, unavailable}
    end.


%% Add the QuickCheck in the r_t runner to the list of Nodes
add_eqc_apps(Nodes) ->
    Apps = [eqc, pulse, pulse_otp, eqc_mcerlang],
    [case code:lib_dir(App, ebin) of
         {error, bad_name} -> % EQC component not installed locally
             ok;
         Path when is_list(Path) ->
             case rpc:call(Node, code, priv_dir, [App]) of
                 {error, bad_name} ->
                     true = rpc:call(Node, code, add_pathz, [Path], 60000);
                 _ ->
                     ok
             end
     end || App <- Apps, Node <- Nodes],
    ok.

-else.  %% no EQC

-export([confirm/0]).

confirm() ->
    lager:info("EQC not enabled, skipping test"),
    pass.

-endif.
