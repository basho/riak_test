-module(rt_local_host).

-behaviour(gen_server).
-behaviour(rt_host).

%% API
-export([connect/1,
         connect/2, 
         copy_dir/4, 
         consult/2,
         disconnect/1,
         exec/2, 
         hostname/1,
         ip_addr/1,
         killall/3,
         kill/3, 
         mkdirs/2,
         mvdir/3,
         rmdir/2,
         temp_dir/1,
         write_file/3]).

-define(TIMEOUT, infinity).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {options=[] :: proplists:proplist(),
                temp_dir :: filelib:dirname()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec connect(rt_host:hostname()) -> {ok, rt_host:host_id()} | rt_util:error().
connect(Hostname) ->
    connect(Hostname, []).

-spec connect(rt_host:hostname(), proplists:proplist()) -> {ok, rt_host:host_id()} | rt_util:error().
connect(_Hostname, Options) ->
    gen_server:start_link(?MODULE, [Options], []).

-spec consult(rt_host:host_id(), filelib:filename()) -> {ok, term()} | rt_util:error().
consult(Pid, Filename) ->
    gen_server:call(Pid, {consult, Filename}, ?TIMEOUT).

-spec copy_dir(rt_host:host_id(), filelib:dirname(), filelib:dirname(), boolean()) -> rt_host:command_result().
copy_dir(Pid, FromDir, ToDir, Recursive) ->
    %% TODO What is the best way to handle timeouts?
    gen_server:call(Pid, {copy_dir, FromDir, ToDir, Recursive}, ?TIMEOUT).

-spec disconnect(rt_host:host_id()) -> ok.
disconnect(Pid) ->
    gen_server:call(Pid, stop, ?TIMEOUT).

-spec exec(rt_host:host_id(), rt_host:command()) -> rt_host:command_result().
exec(Pid, Command) ->
    %% TODO What is the best way to handle timeouts? -> Not the OTP timeout, bot to erlexec
    gen_server:call(Pid, {exec, Command}, ?TIMEOUT).

-spec hostname(rt_host:host_id()) -> rt_host:hostname().
hostname(Pid) ->
    gen_server:call(Pid, hostname, ?TIMEOUT).

-spec ip_addr(rt_host:host_id()) -> string().
ip_addr(Pid) ->
    gen_server:call(Pid, ip_addr, ?TIMEOUT).

-spec kill(rt_host:host_id(), pos_integer(), pos_integer()) -> rt_util:result().
kill(Pid, Signal, OSPid) ->
    gen_server:call(Pid, {kill, Signal, OSPid}, ?TIMEOUT).

-spec killall(rt_host:host_id(), pos_integer(), string()) -> rt_util:result().
killall(Pid, Signal, Name) ->
    gen_server:call(Pid, {killall, Signal, Name}, ?TIMEOUT).

-spec mvdir(rt_host:host_id(), filelib:dirname(), filelib:dirname()) -> rt_util:result().
mvdir(Pid, FromDir, ToDir) ->
    gen_server:call(Pid, {mvdir, FromDir, ToDir}, ?TIMEOUT).

-spec mkdirs(rt_host:host_id(), filelib:dirname()) -> {ok, filelib:dirname()} | rt_result:error().
mkdirs(Pid, Path) ->
    gen_server:call(Pid, {mkdirs, Path}, ?TIMEOUT).

-spec rmdir(rt_host:host_id(), filelib:dirname()) -> rt_result:result().
rmdir(Pid, Dir) ->
    gen_server:call(Pid, {rmdir, Dir}, ?TIMEOUT).

-spec temp_dir(rt_host:host_id()) -> filelib:dirname().
temp_dir(Pid) ->
    gen_server:call(Pid, temp_dir, ?TIMEOUT).

-spec write_file(rt_host:host_id(), filelib:filename(), term()) -> rt_util:result().
write_file(Pid, Filename, Content) ->
    gen_server:call(Pid, {write_file, Filename, Content}, ?TIMEOUT).

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
init([Options]) ->
    maybe_init(make_temp_directory(), Options).

-spec maybe_init({ok, filelib:dirname()} | {error, term()}, proplists:proplist()) -> {ok, #state{}} | {stop, term()}.
maybe_init({ok, TempDir}, Options) ->
    State = #state{options=Options,
                    temp_dir=TempDir},
    lager:debug("Starting localhost gen_server with state ~p", [State]),
    {ok, State};
maybe_init({error, Reason}, _Options) ->
    lager:error("Failed to start localhost gen_server -- temp directory failed to be created due to ~p", [Reason]),
    {stop, Reason}.

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
handle_call({copy_dir, FromDir, ToDir, Recursive}, _From, State) ->
    {reply, do_copy_dir(FromDir, ToDir, Recursive), State};
handle_call({consult, Filename}, _From, State) ->
    {reply, file:consult(Filename), State};
handle_call({exec, Command}, _From, State) ->
    {reply, maybe_exec(Command), State};
handle_call(hostname, _From, State) ->
    {reply, localhost, State};
handle_call(ip_addr, _From, State) ->
    {reply, "127.0.0.1", State};
handle_call({kill, Signal, OSPid}, _From, State) ->
    lager:info("Killing process ~p with signal ~p on localhost", [OSPid, Signal]),
    {reply, exec:kill(OSPid, Signal), State};
handle_call({killall, Signal, Name}, _From, State) ->
    lager:info("Killing all processes named ~p with signal ~p on localhost", [Name, Signal]),
    SignalStr = lists:concat(["-", integer_to_list(Signal)]),
    {reply, maybe_exec({"/usr/bin/killall", [SignalStr, Name]}), State};
handle_call({mkdirs, Path}, _From, State) ->
    %% filelib:ensure_dir requires the path to end with a / in order to
    %% create a directory ...
    SanitizedPath = rt_util:maybe_append_when_not_endswith(Path, "/"),
    lager:debug("Creating directory ~p on localhost", [SanitizedPath]),
    {reply, filelib:ensure_dir(SanitizedPath), State};
handle_call({mvdir, FromDir, ToDir}, _From, State) ->
    {reply, maybe_exec(rt_host:mvdir_cmd(FromDir, ToDir)), State};
handle_call({rmdir, Dir}, _From, State) ->
    {reply, maybe_exec(rt_host:rmdir_cmd(Dir)), State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(temp_dir, _From, State=#state{temp_dir=TempDir}) ->
    {reply, TempDir, State};
handle_call({write_file, Filename, Content}, _From, State) ->
    lager:debug("Writing ~p to file ~p on localhost", [Content, Filename]),
    {reply, file:write_file(Filename, Content), State}.
                   
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
terminate(_Reason, #state{temp_dir=TempDir}) ->
    lager:debug("Shuttting down localhost gen_server."),
    case maybe_exec(rt_host:rmdir_cmd(TempDir)) of
        {ok, _} -> 
            lager:debug("Removed temporary directory ~p on localhost", [TempDir]),
            ok;
        {error, Reason} -> 
            lager:warning("Failed to remove temporary directory ~p due to ~p on localhost", 
                          [TempDir, Reason]),
            ok
    end.
                              
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
%% TODO Implement stdout and stderr redirection to lager ...
-spec maybe_exec(rt_host:command()) -> rt_host:command_result().
maybe_exec(Command) ->
    maybe_exec(Command, [sync, stdout, stderr]).

-spec maybe_exec({ok | error, proplists:proplists()} | rt_host:command(), string() | proplists:proplist()) -> rt_host:command_result().
maybe_exec(Result={ok, Reason}, CommandLine) ->
    lager:debug("Command ~s succeeded with result ~p", [CommandLine, Result]),
    Output = proplists:get_value(stdout, Reason, []),
    {ok, join_binaries_to_string(Output)};
maybe_exec(Result={error, Reason}, CommandLine) ->
    lager:error("Command ~s failed with result ~p", [CommandLine, Result]),
    Output = proplists:get_value(stderr, Reason, []),
    {error, join_binaries_to_string(Output)};
maybe_exec(Command, Options) ->
    CommandLine = rt_host:command_line_from_command(Command),
    lager:debug("Executing command ~p with options ~p", [CommandLine, Options]),
    maybe_exec(exec:run(CommandLine, Options), CommandLine).

%% TODO Consider implementing in pure Erlang to get more granular result info ...
-spec do_copy_dir(filelib:dirname(), filelib:dirname(), boolean()) -> rt_host:command_result().
do_copy_dir(FromDir, ToDir, true) ->
    lager:debug("Copying ~p to ~p recursively", [FromDir, ToDir]),
    maybe_exec("cp", ["-R", FromDir, ToDir]);
do_copy_dir(FromDir, ToDir, false) ->
    lager:debug("Copying ~p to ~p non-recursively", [FromDir, ToDir]),
    maybe_exec("cp", [FromDir, ToDir]).

-spec make_temp_directory() -> rt_host:command_result().
make_temp_directory() ->
    lager:debug("Creating a temporary directory on localhost"),
    maybe_exec(rt_host:make_temp_directory_cmd("riak_test")).

-spec join_binaries_to_string([binary()]) -> string().
join_binaries_to_string(Bins) ->
    Strings = lists:foldr(fun(Element, List) ->
                                  [binary:bin_to_list(Element)|List]
                          end, [], Bins),
    Result = string:join(Strings, "\n"),

    %% Only strip off the trailing newline ...
    string:strip(Result, right, $\n).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

setup() ->
    ok = application:ensure_started(exec),
    {Result, Pid} = connect(localhost),
    ?assertEqual(ok, Result),
    ?assertEqual(true, is_pid(Pid)),
    ?assertEqual(true, filelib:is_dir(temp_dir(Pid))),
    Pid.

teardown(Pid) ->
    TempDir = temp_dir(Pid),
    disconnect(Pid),
    ?assertEqual(false, filelib:is_dir(TempDir)).

verify_connect({Result, Pid}) ->
    ?_test(begin
               ?assertEqual(ok, Result),
               ?assertEqual(true, is_pid(Pid)),
               ok = disconnect(Pid)
           end).

connect_test_() ->
    {foreach,
     fun() -> ok = application:ensure_started(exec) end,
     [fun() -> verify_connect(connect(localhost)) end,
      fun() -> verify_connect(connect(localhost, [])) end]}.

check_exec_args(Pid) ->
    ?_test(begin
               verify_exec_results(exec(Pid, {"echo", ["test"]}), "test")
           end).

check_exec_no_args(Pid) ->
    ?_test(begin
               verify_exec_results(exec(Pid, {"echo", []}), "")
           end).
              
verify_exec_results({Result, Output}, Expected) ->
    ?assertEqual(ok, Result),
    ?assertEqual(Expected, Output).

exec_success_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [fun check_exec_no_args/1,
      fun check_exec_args/1]}.

exec_failure_test() ->
    Pid = setup(),
    {Result, _} = exec(Pid, {"asdfasdf", []}),
    ?assertEqual(error, Result),
    teardown(Pid).

check_kill(Pid, Signal) -> 
    ?_test(begin
                {ok, _, OSPid} = exec:run("while true; do sleep 1; done", 
                                        [{success_exit_code, Signal}]),
                Result = kill(Pid, Signal, OSPid),
                ?assertEqual(ok, Result)
           end).

kill_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [fun(Pid) -> check_kill(Pid, 9) end,
      fun(Pid) -> check_kill(Pid, 15) end]}.

mkdirs_test() ->
    Pid = setup(),
    TestDir = filename:join([temp_dir(Pid), "test123", "foo", "bar"]),
    Result = mkdirs(Pid, TestDir),
    ?assertEqual(ok, Result),
    ?assertEqual(true, filelib:is_dir(TestDir)),
    teardown(Pid).

hostname_test() ->
    Pid = setup(),
    ?assertEqual(localhost, hostname(Pid)),
    teardown(Pid).

ip_addr_test() ->
    Pid = setup(),
    ?assertEqual("127.0.0.1", ip_addr(Pid)),
    teardown(Pid).

join_binaries_to_string_test() ->
    Bins = [<<"foo">>, <<"bar">>, <<"zoo">>],
    Actual = join_binaries_to_string(Bins),
    ?assertEqual("foo\nbar\nzoo", Actual).

-endif.
