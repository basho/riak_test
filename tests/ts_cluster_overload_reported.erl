%% -------------------------------------------------------------------
%%
%% ts_cluster_overload_reported - test to ensure overload is handled correctly.
%% based on overload test, simplified to slam TS w/ queries until an overload
%% occurs to ensure {error, atom()} responses are handled correctly w/i TS.
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
-module(ts_cluster_overload_reported).
-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-define(NODE_COUNT, 3).
-define(READS_COUNT, 1000).
-define(WRITES_COUNT, 1).
-define(READ_RETRIES, 3).
-define(WRITE_RETRIES, 3).
-define(VALUE, "'overload_test_value'").

confirm() ->
    Table = atom_to_list(?MODULE),
    Nodes = setup(Table),
    {Pids, IsOverload} = try generate_mixed_rw_traffic(Nodes, Table) of
                             Ps -> {Ps, false}
                         catch throw:{ts_overload, Ps} ->
                                   {Ps, true}
                         end,

    kill_pids(Pids),
    ?assert(IsOverload),
    pass.

kill_pids(Pids) ->
    [exit(Pid, kill) || Pid <- Pids].

setup(Table) ->
    Nodes = rt:build_cluster(?NODE_COUNT, overload_config()),
    pb_create_table(hd(Nodes), Table),
    Nodes.

overload_config() ->
    VnodeOverloadThreshold = 2,
    VnodeCheckInterval = 1,
    VnodeCheckRequestInterval = 5,
    FsmLimit = 2,
    [{riak_core, [{ring_creation_size, 8},
                  {default_bucket_props,
                   [
                    {n_val, ?NODE_COUNT},
                    {allow_mult, true},
                    {dvv_enabled, true}
                   ]},
                  {vnode_management_timer, 1000},
                  {enable_health_checks, false},
                  {enable_consensus, true},
                  {vnode_overload_threshold, VnodeOverloadThreshold},
                  {vnode_check_interval, VnodeCheckInterval},
                  {vnode_check_request_interval, VnodeCheckRequestInterval}]},
     {riak_kv, [{fsm_limit, FsmLimit},
                {storage_backend, riak_kv_eleveldb_backend},
                {anti_entropy_build_limit, {100, 1000}},
                {anti_entropy_concurrency, 100},
                {anti_entropy_tick, 100},
                {anti_entropy, {on, []}},
                {anti_entropy_timeout, 5000}]}, 
     {riak_api, [{pb_backlog, 1024}]}].

generate_mixed_rw_traffic(Nodes, Table) ->
    Node = hd(Nodes),
    WritePids = spawn_writes(Node, Table, ?VALUE, ?WRITES_COUNT, ?WRITE_RETRIES),
    ReadPids = spawn_reads(Node, Table, ?READS_COUNT, ?READ_RETRIES),
    WritePids ++ ReadPids.

spawn_writes(Node, Table, Value, WriteCount, WriteRetries) ->
    PBInsertFun = fun(PBPid, I) ->
                          pb_insert(PBPid, Table, {I, Value})
                  end,
    spawn_op(PBInsertFun, Node, WriteCount, WriteRetries).

spawn_reads(Node, Table, ReadCount, ReadRetries) ->
    PBReadFun = fun(PBPid, _I) ->
                        pb_select(PBPid, Table)
                end,
    spawn_op(PBReadFun, Node, ReadCount, ReadRetries).

spawn_op(PBFun, Node, WriteCount, WriteRetries) ->
    TestPid = self(),
    Pids = [begin
                PBInsertFun = fun(PBPid) ->
                                      PBFun(PBPid, I)
                              end,
                Pid = spawn(fun() ->
                                    rt:wait_until(pb_fun_fun(TestPid, Node, PBInsertFun), WriteRetries, WriteRetries)
                            end),
                %% thunder on!, no sleep
                Pid
            end || I <- lists:seq(1, WriteCount)],
    Responses = [receive
         {Status, Pid} -> Status
     end || Pid <- Pids],
    [ throw({ts_overload, Pids}) || Response <- Responses,
                                    Response =:= sent_ts_overload ],
    Pids.

pb_create_table(Node, Table) ->
    PBPid = rt:pbc(Node),
    Sql = list_to_binary("CREATE TABLE " ++ Table ++
                         "(ts TIMESTAMP NOT NULL," ++
                         "v VARCHAR NOT NULL," ++
                         "PRIMARY KEY((QUANTUM(ts, 1, 'h')), ts))"),
    riakc_ts:query(PBPid, Sql).

pb_fun_fun(TestPid, Node, PBFun) ->
    fun() ->
            PBPid = rt:pbc(Node),
            Result = case catch PBFun(PBPid) of
                {error, {1001, <<"overload">>}} ->
                    lager:debug("ts overload detected, succeeded"),
                    TestPid ! {sent_ts_overload, self()},
                    true;
                {ok, _Res} ->
                    lager:debug("succeeded, continuing..."),
                    TestPid ! {sent_ok, self()},
                    true;
                {error, Reason} ->
                    lager:debug("error: ~p, continuing...", [Reason]),
                    false;
                {'EXIT', Type} ->
                    lager:debug("EXIT: ~p, continuing...", [Type]),
                    false
            end,
            riakc_pb_socket:stop(PBPid),
            Result
    end.

pb_insert(PBPid, Table, {I, Value}) ->
    Sql = list_to_binary("INSERT INTO " ++ Table ++
                         "(ts, v)VALUES(" ++
                            integer_to_list(I) ++
                            "," ++ Value ++
                         ")"),
    riakc_ts:query(PBPid, Sql).

pb_select(PBPid, Table) ->
    Sql = list_to_binary("SELECT * FROM " ++ Table ++
                         " WHERE ts >= 1 AND ts <= 10"),
    riakc_ts:query(PBPid, Sql).
