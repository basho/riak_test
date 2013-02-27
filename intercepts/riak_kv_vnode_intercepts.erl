-module(riak_kv_vnode_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_vnode_orig).

%% @doc Simulate dropped puts by truncating the preflist for every kv
%%      vnode put.  This is useful for testing read-repair, AAE, etc.
dropped_put(Preflist, BKey, Obj, ReqId, StartTime, Options, Sender) ->
    NewPreflist = lists:sublist(Preflist, length(Preflist) - 1),
    %% Uncomment to see modification in logs, too spammy to have on by default
    %% ?I_INFO("Preflist modified from ~p to ~p", [Preflist, NewPreflist]),
    ?M:put_orig(NewPreflist, BKey, Obj, ReqId, StartTime, Options, Sender).

%% @doc Make all KV vnode commands take abnormally long.
slow_handle_command(Req, Sender, State) ->
    timer:sleep(500),
    ?M:handle_command_orig(Req, Sender, State).

%% @doc Simulate dropped gets/network partitions byresponding with
%%      noreply during get requests.
drop_do_get(_Sender, _BKey, _ReqId, State) ->
    _Partition = element(2,State),
    case ets:lookup(intercepts_tab, drop_do_get_partitions) of
        [] ->
            ?M:do_get_orig(_Sender, _BKey, _ReqId, State);
        [{drop_do_get_partitions, Partitions}] ->
            case lists:member(_Partition, Partitions) of
                true ->
                    %% ?I_INFO("Dropping get for ~p on ~p", [_BKey, _Partition]),
                    lager:log(info, self(), "dropping get request for ~p",
                        [_Partition]),
                    {noreply, State};
                false ->
                    ?M:do_get_orig(_Sender, _BKey, _ReqId, State)
            end
    end.

%% @doc Simulate dropped puts/network partitions byresponding with
%%      noreply during put requests.
drop_do_put(_Sender, _BKey, _RObj, _ReqId, _StartTime, _Options, State) ->
    _Partition = element(2,State),
    case ets:lookup(intercepts_tab, drop_do_put_partitions) of
        [] ->
            ?M:do_put_orig(_Sender, _BKey, _RObj, _ReqId, _StartTime, _Options, State);
        [{drop_do_put_partitions, Partitions}] ->
            case lists:member(_Partition, Partitions) of
                true ->
                    lager:log(info, self(), "dropping put request for ~p",
                        [_Partition]),
                    %% ?I_INFO("Dropping put for ~p on ~p", [_BKey, _Partition]),
                    State;
                false ->
                    ?M:do_put_orig(_Sender, _BKey, _RObj, _ReqId, _StartTime, _Options, State)
            end
    end.

%% @doc Simulate put failures by responding with failure messages.
error_do_put(_Sender, _BKey, _RObj, _ReqId, _StartTime, _Options, State) ->
    _Partition = element(2,State),
    case ets:lookup(intercepts_tab, drop_do_put_partitions) of
        [] ->
            ?M:do_put_orig(_Sender, _BKey, _RObj, _ReqId, _StartTime, _Options, State);
        [{drop_do_put_partitions, Partitions}] ->
            case lists:member(_Partition, Partitions) of
                true ->
                    lager:log(info, self(), "failing put request for ~p",
                        [_Partition]),
                    %% ?I_INFO("Failing put for ~p on ~p", [_BKey, _Partition]),

                    %% sleep for a while, so the vnode response order is
                    %% deterministic
                    timer:sleep(1000),
                    riak_core_vnode:reply(_Sender, {fail, _Partition, _ReqId}),
                    State;
                false ->
                    ?M:do_put_orig(_Sender, _BKey, _RObj, _ReqId, _StartTime, _Options, State)
            end
    end.
