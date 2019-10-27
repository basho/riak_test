%% -------------------------------------------------------------------
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
%%-------------------------------------------------------------------

-module(riak_kv_vnode_intercepts).
-compile([export_all, nowarn_export_all]).
-include("intercept.hrl").

%% shamelessly copied from riak_kv_vnode.hrl
-record(riak_kv_w1c_put_req_v1, {
    bkey :: {binary(),binary()},
    encoded_val :: binary(),
    type :: primary | fallback
    % start_time :: non_neg_integer(), Jon to add?
}).
-record(riak_kv_w1c_put_reply_v1, {
    reply :: ok | {error, term()},
    type :: primary | fallback
}).

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
    Rand = rand:uniform(5000),
    error_logger:info_msg("coverage sleeping ~p", [Rand]),
    timer:sleep(Rand),
    ?M:handle_coverage_orig(Req, Filter, Sender, State).

%% @doc Count how many times we call handle_handoff_command
count_handoff_w1c_puts(#riak_kv_w1c_put_req_v1{}=Req, Sender, State) ->
    Val = ?M:handle_handoff_command_orig(Req, Sender, State),
    ets:update_counter(intercepts_tab, w1c_put_counter, 1),
    Val;
count_handoff_w1c_puts(Req, Sender, State) ->
    ?M:handle_handoff_command_orig(Req, Sender, State).

%% @doc Count how many times we handle syncchronous and asynchronous replies
%% in handle_command when using w1c buckets
count_w1c_handle_command(#riak_kv_w1c_put_req_v1{}=Req, Sender, State) ->
    case ?M:handle_command_orig(Req, Sender, State) of
        {noreply, NewState} ->
            ets:update_counter(intercepts_tab, w1c_async_replies, 1),
            {noreply, NewState};
        {reply, #riak_kv_w1c_put_reply_v1{reply=ok, type=Type}, NewState} ->
            ets:update_counter(intercepts_tab, w1c_sync_replies, 1),
            {reply, #riak_kv_w1c_put_reply_v1{reply=ok, type=Type}, NewState};
        Any -> Any
    end;
count_w1c_handle_command(Req, Sender, State) ->
    ?M:handle_command_orig(Req, Sender, State).

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

%% @doc Simulate dropped heads/network partitions byresponding with
%%      noreply during head requests.
drop_do_head(Sender, BKey, ReqId, State) ->
    Partition = element(2,State),
    case ets:lookup(intercepts_tab, drop_do_head_partitions) of
        [] ->
            ?M:do_head_orig(Sender, BKey, ReqId, State);
        [{drop_do_head_partitions, Partitions}] ->
            case lists:member(Partition, Partitions) of
                true ->
                    %% ?I_INFO("Dropping get for ~p on ~p", [BKey, Partition]),
                    lager:log(info, self(), "dropping head request for ~p",
                        [Partition]),
                    {noreply, State};
                false ->
                    ?M:do_head_orig(Sender, BKey, ReqId, State)
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
                    %% NB: riak_kv#1046 - do_put returns a tuple now
                    {dropped_by_intercept, State};
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
                    riak_core_vnode:reply(Sender, {fail, Partition, ReqId}),
                    %% NB: riak_kv#1046 - do_put returns a tuple now
                    {error_by_intercept, State};
                false ->
                    ?M:do_put_orig(Sender, BKey, RObj, ReqId, StartTime, Options, State)
            end
    end.

corrupting_handle_handoff_data(BinObj0, State) ->
    BinObj =
        case rand:uniform(20) of
            10 ->
                corrupt_binary(BinObj0);
            _ -> BinObj0
        end,
    ?M:handle_handoff_data_orig(BinObj, State).

corrupt_binary(O) ->
    crypto:strong_rand_bytes(byte_size(O)).

put_as_readrepair(Preflist, BKey, Obj, ReqId, StartTime, Options) ->
    ?M:put_orig(Preflist, BKey, Obj, ReqId, StartTime, [rr | Options]).

coord_put_as_readrepair(Preflist, BKey, Obj, ReqId, StartTime, Options) ->
    ?M:coord_put_orig(Preflist, BKey, Obj, ReqId, StartTime, [rr | Options]).

never_ready_to_exit() -> false.