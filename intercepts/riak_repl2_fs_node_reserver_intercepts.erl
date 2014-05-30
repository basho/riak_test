-module(riak_repl2_fs_node_reserver_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl2_fs_node_reserver_orig).

%% @doc Provide an intercept which forces the node reserver to fail when
%%      attempting to reserve a node with a location_down message.
down_reserve({reserve, _Partition}, _From, State) ->
    io:format("down_reserve~n"),
    ?I_INFO("down_reserve~n"),
    {reply, down, State};
down_reserve(Message, From, State) ->
    ?M:handle_call_orig(Message, From, State).
