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
%% Module to manage the list of pending test plans and hand off work
%% to the appropriate test scheduler.
%% @end
%% Created : 30. Mar 2015 10:25 AM
%%-------------------------------------------------------------------
-module(rt_planner).
-author("Brett Hazen").

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0,
         load_from_giddyup/2,
         add_test_plan/5,
         fetch_test_plan/0,
         fetch_test_non_runnable_plan/0,
         number_of_plans/0,
         number_of_non_runable_plans/0,
         stop/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    %% Tests which are deemed to be runable
    runnable_test_plans :: queue(),
    %% Tests which are deemed not to be runable
    non_runnable_test_plans :: queue()
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
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Reads the list of test plans from GiddyUp and queues them up
%%
%% @end
%%--------------------------------------------------------------------
-spec(load_from_giddyup([string()] | undefined, list()) -> ok).
load_from_giddyup(Backends, CommandLineTests) ->
    gen_server:call(?MODULE, {load_from_giddyup, Backends, CommandLineTests}).

%%--------------------------------------------------------------------
%% @doc
%% Queue up a new test plan
%%
%% @end
%%--------------------------------------------------------------------
-spec(add_test_plan(string(), string(), [string()], rt_properties2:product_version(), rt_properties2:properties()) -> ok).
add_test_plan(Module, Platform, Backends, Version, Properties) ->
    gen_server:call(?MODULE, {add_test_plan, Module, Platform, Backends, Version, Properties}).

%%--------------------------------------------------------------------
%% @doc
%% Fetch a test plan off the queue
%%
%% @end
%%--------------------------------------------------------------------
-spec(fetch_test_plan() -> rt_test_plan:test_plan() | empty).
fetch_test_plan() ->
    gen_server:call(?MODULE, fetch_test_plan).

%%--------------------------------------------------------------------
%% @doc
%% Fetch a test plan off the queue
%%
%% @end
%%--------------------------------------------------------------------
-spec(fetch_test_non_runnable_plan() -> rt_test_plan:test_plan() | empty).
fetch_test_non_runnable_plan() ->
    gen_server:call(?MODULE, fetch_test_non_runnable_plan).

%%--------------------------------------------------------------------
%% @doc
%% Return the number of runable test plans in the queue
%%
%% @end
%%--------------------------------------------------------------------
-spec(number_of_plans() -> rt_test_plan:test_plan() | empty).
number_of_plans() ->
    gen_server:call(?MODULE, number_of_plans).

%%--------------------------------------------------------------------
%% @doc
%% Return the number of non-runable test plans in the queue
%%
%% @end
%%--------------------------------------------------------------------
-spec(number_of_non_runable_plans() -> rt_test_plan:test_plan() | empty).
number_of_non_runable_plans() ->
    gen_server:call(?MODULE, number_of_non_runable_plans).

%%--------------------------------------------------------------------
%% @doc
%% Stops the server
%%
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop, infinity).

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
init([]) ->
    {ok, #state{runnable_test_plans=queue:new(),
                non_runnable_test_plans=queue:new()}}.

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
%% Run only those GiddyUp tests which are specified on the command line
%% If none are specified, run everything
handle_call({load_from_giddyup, Backends, CommandLineTests}, _From, State) ->
    AllGiddyupTests = giddyup:get_test_plans(),
    {Included, Excluded} = case CommandLineTests of
        [] ->
            {AllGiddyupTests, []};
        _ ->
            Inc = [TestPlan || TestPlan <- AllGiddyupTests,
                               CName <- CommandLineTests,
                               rt_test_plan:get_module(TestPlan) =:= CName],
            {Inc, lists:filter(fun(Elem) -> not lists:member(Elem, Inc) end, AllGiddyupTests)}
        end,
    {Included1, Excluded1} = case Backends of
        undefined ->
            {Included, Excluded};
        _ ->
            Inc1 = [TestPlan || TestPlan <- Included,
                                            lists:member(rt_test_plan:get(backend, TestPlan), Backends)],
            {Inc1, lists:filter(fun(Elem) -> not lists:member(Elem, Inc1) end, AllGiddyupTests)}
        end,
    State1 = lists:foldl(fun sort_and_queue/2, State, Included1),
    State2 = lists:foldl(fun exclude_test_plan/2, State1, Excluded1),
    {reply, ok, State2};
%% Add a single test plan to the queue
handle_call({add_test_plan, Module, Platform, Backends, _Version, _Properties}, _From, State) ->
    State1 = lists:foldl(fun(Backend, AccState) ->
            TestPlan = rt_test_plan:new([{module, Module}, {platform, Platform}, {backend, Backend}]),
            sort_and_queue(TestPlan, AccState)
        end,
        State, Backends),
    {reply, ok, State1};
handle_call(fetch_test_plan, _From, State) ->
    Q = State#state.runnable_test_plans,
    {Item, Q1} = queue:out(Q),
    Result = case Item of
                 {value, Value} -> Value;
                 Empty -> Empty
             end,
    {reply, Result, State#state{runnable_test_plans=Q1}};
handle_call(fetch_test_non_runnable_plan, _From, State) ->
    Q = State#state.non_runnable_test_plans,
    {Item, Q1} = queue:out(Q),
    Result = case Item of
                 {value, Value} -> Value;
                 Empty -> Empty
             end,
    {reply, Result, State#state{non_runnable_test_plans=Q1}};
handle_call(number_of_plans, _From, State) ->
    Q = State#state.runnable_test_plans,
    {reply, queue:len(Q), State};
handle_call(number_of_non_runable_plans, _From, State) ->
    Q = State#state.non_runnable_test_plans,
    {reply, queue:len(Q), State};
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
%% Add a specific test into the proper list
%%
%% @end
%%--------------------------------------------------------------------
sort_and_queue(TestPlan, State) ->
    QR = State#state.runnable_test_plans,
    QNR = State#state.non_runnable_test_plans,
    {QR2, QNR2} = case is_runnable_test_plan(TestPlan) of
                      true ->
                          {queue:in(TestPlan, QR), QNR};
                      _ ->
                          {QR, queue:in(TestPlan, QNR)}
                  end,
    State#state{runnable_test_plans=QR2,
                non_runnable_test_plans=QNR2}.

%% Check for api compatibility
is_runnable_test_plan(TestPlan) ->
    TestModule = rt_test_plan:get_module(TestPlan),
    {Mod, Fun} = riak_test_runner:function_name(confirm, TestModule),

    code:ensure_loaded(Mod),
    erlang:function_exported(Mod, Fun, 0) orelse
        erlang:function_exported(Mod, Fun, 1).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Add a unused test to the list of non_runnable_test_plans
%% @end
%%--------------------------------------------------------------------
exclude_test_plan(TestPlan, State) ->
    QNR = queue:in(TestPlan, State#state.non_runnable_test_plans),
    State#state{non_runnable_test_plans=QNR}.