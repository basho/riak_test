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

%% @doc Return wrong_node error because ownership transfer is happening
%%      when trying to get the hashtree pid for a partition.
wrong_node(_Partition) ->
    {error, wrong_node}.

%% @doc Make all KV vnode coverage commands take abnormally long.
slow_handle_coverage(Req, Filter, Sender, State) ->
    random:seed(erlang:now()),
    Rand = random:uniform(5000),
    error_logger:info_msg("coverage sleeping ~p", [Rand]),
    timer:sleep(Rand),
    ?M:handle_coverage_orig(Req, Filter, Sender, State).

%% @doc Simulate dropped gets/network partitions byresponding with
%%      noreply during get requests.
drop_do_get(Sender, BKey, ReqId, State) ->
    Partition = element(2,State),
    case ets:lookup(intercepts_tab, drop_do_get_partitions) of
        [] ->
            ?M:do_get_orig(Sender, BKey, ReqId, State);
        [{drop_do_get_partitions, Partitions}] ->
            case lists:member(Partition, Partitions) of
                true ->
                    %% ?I_INFO("Dropping get for ~p on ~p", [BKey, Partition]),
                    lager:log(info, self(), "dropping get request for ~p",
                        [Partition]),
                    {noreply, State};
                false ->
                    ?M:do_get_orig(Sender, BKey, ReqId, State)
            end
    end.

%% @doc Simulate dropped puts/network partitions byresponding with
%%      noreply during put requests.
drop_do_put(Sender, BKey, RObj, ReqId, StartTime, Options, State) ->
    Partition = element(2,State),
    case ets:lookup(intercepts_tab, drop_do_put_partitions) of
        [] ->
            ?M:do_put_orig(Sender, BKey, RObj, ReqId, StartTime, Options, State);
        [{drop_do_put_partitions, Partitions}] ->
            case lists:member(Partition, Partitions) of
                true ->
                    lager:log(info, self(), "dropping put request for ~p",
                        [Partition]),
                    %% ?I_INFO("Dropping put for ~p on ~p", [BKey, Partition]),
                    State;
                false ->
                    ?M:do_put_orig(Sender, BKey, RObj, ReqId, StartTime, Options, State)
            end
    end.

%% @doc Simulate put failures by responding with failure messages.
error_do_put(Sender, BKey, RObj, ReqId, StartTime, Options, State) ->
    Partition = element(2,State),
    case ets:lookup(intercepts_tab, drop_do_put_partitions) of
        [] ->
            ?M:do_put_orig(Sender, BKey, RObj, ReqId, StartTime, Options, State);
        [{drop_do_put_partitions, Partitions}] ->
            case lists:member(Partition, Partitions) of
                true ->
                    lager:log(info, self(), "failing put request for ~p",
                        [Partition]),
                    %% ?I_INFO("Failing put for ~p on ~p", [BKey, Partition]),

                    %% sleep for a while, so the vnode response order is
                    %% deterministic
                    timer:sleep(1000),
                    Reply = {fail, Partition, ReqId},
                    riak_core_vnode:reply(Sender, Reply),
                    {Reply, State};
                false ->
                    ?M:do_put_orig(Sender, BKey, RObj, ReqId, StartTime, Options, State)
            end
    end.

corrupting_handle_handoff_data(BinObj0, State) ->
    BinObj =
        case random:uniform(20) of
            10 ->
                corrupt_binary(BinObj0);
            _ -> BinObj0
        end,
    ?M:handle_handoff_data_orig(BinObj, State).

corrupt_binary(O) ->
    crypto:rand_bytes(byte_size(O)).
