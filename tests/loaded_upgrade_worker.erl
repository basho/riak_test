%%%-------------------------------------------------------------------
%%% @author Joseph DeVivo <>
%%% @copyright (C) 2013, Joseph DeVivo
%%% @doc
%%%
%%% @end
%%% Created : 29 Jan 2013 by Joseph DeVivo <>
%%%-------------------------------------------------------------------
-module(loaded_upgrade_worker).

-include_lib("eunit/include/eunit.hrl").

-behaviour(gen_server).

%% API
-export([start_link/2, assert_equal/3, test_fun/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          name,
          runner,
          node=undefined,
          pbc,
          httpc,
          state=active,
          count=0
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name, Node) ->
    lager:debug("Spawning loaded_upgrade_worker for ~p", [Name]),
    gen_server:start_link({local, Name}, ?MODULE, [Name, Node], []).

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
init([Name, Node]) ->
    lager:debug("loaded_upgrade_worker:init([~p]) ~p", [Node, self()]),
    PBC = rt:pbc(Node),
    HTTPC = rt:httpc(Node),
    lager:debug("Created Clients ~p", [Name]),
    State = #state{
       node = Node,
       name = Name,
       pbc = PBC,
       httpc = HTTPC
      },
    Pid = spawn_link(?MODULE, test_fun, [State]),
    lager:debug("Spawned tester ~p", [Name]),
    {ok, State#state{runner=Pid}}.

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
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

test_fun(State=#state{count=Count}) ->
    [ F() || F <- generate_test_funs(State)],
    test_fun(State#state{count=Count+1}).

generate_test_funs(#state{name=Name, pbc=PBC, httpc=_HTTPC, count=Count}) ->
    
    [
     %% List Keys
     fun() ->
        io:format("< ~p > listkeys test #~p", [Name, Count]),
        {ok, Keys} = riakc_pb_socket:list_keys(PBC, <<"objects">>), 
        ActualKeys = lists:usort(Keys),
        ExpectedKeys = lists:usort([list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, 100)]),
        assert_equal(Name, ExpectedKeys, ActualKeys)    
     end
    ].

assert_equal(Name, Expected, Actual) ->
    case Expected -- Actual of 
        [] -> ok;
        Diff -> lager:info("<~p> Expected -- Actual: ~p", [Name, Diff])
    end,
    ?assertEqual(length(Actual), length(Expected)),
    ?assertEqual(Actual, Expected).

