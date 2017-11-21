%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(loaded_upgrade_worker_sup).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-behavior(supervisor).

%% API
-export([assert_equal/2]).

-export([list_keys_tester/5, kv_tester/5, mapred_tester/5,
         twoi_tester/5, tester_start_link/4]).

-export([init/1]).
-export([start_link/5]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Name, FunName, Node, Vsn, ReportPid), {
    list_to_atom(atom_to_list(Name) ++ "_" ++ atom_to_list(FunName)),
    {   ?MODULE,
        tester_start_link,
        [FunName, Node, Vsn, ReportPid]},
        permanent, 5000, worker, [?MODULE]}).

start_link(Name, Node, Backend, Vsn, ReportPid) ->
    supervisor:start_link(?MODULE, [Name, Node, Backend, Vsn, ReportPid]).

init([Name, Node, Backend, Vsn, ReportPid]) ->
    rt:wait_for_service(Node, [riak_kv,riak_pipe]),

    ChildSpecs1 = [
        ?CHILD(Name, FunName, Node, Vsn, ReportPid)
        || FunName <- [list_keys_tester, kv_tester]],

    ChildSpecs = case Backend of
        eleveldb ->
            [?CHILD(Name, twoi_tester, Node, Vsn, ReportPid) | ChildSpecs1];
        _ -> ChildSpecs1
    end,
    {ok, {{one_for_one, 1000, 60}, ChildSpecs}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

tester_start_link(Function, Node, Vsn, ReportPid) ->
    {ok, spawn_link(?MODULE, Function, [Node, 0, undefined, Vsn, ReportPid])}.

list_keys_tester(Node, Count, Pid, Vsn, ReportPid) ->
    PBC = pb_pid_recycler(Pid, Node),
    case riakc_pb_socket:list_keys(PBC, <<"objects">>) of
        {ok, Keys} ->
            ActualKeys = lists:usort(Keys),
            ExpectedKeys = lists:usort([loaded_upgrade:int_to_key(K) || K <- lists:seq(0, 100)]),
            case assert_equal(ExpectedKeys, ActualKeys) of
                true -> cool;
                _ -> ReportPid ! {listkeys, Node, not_equal}
            end;
        {error, timeout} ->
            ReportPid ! {listkeys, Node, timeout};
        {error, {timeout, _}} ->
            ReportPid ! {listkeys, Node, timeout};
        Unexpected ->
            ReportPid ! {listkeys, Node, Unexpected}
    end,
    list_keys_tester(Node, Count + 1, PBC, Vsn, ReportPid).

kv_tester(Node, Count, Pid, Vsn, ReportPid) ->
    PBC = pb_pid_recycler(Pid, Node),
    Key = Count rem 8000,
    case riakc_pb_socket:get(PBC, loaded_upgrade:bucket(kv), loaded_upgrade:int_to_key(Key)) of
        {ok, Val} ->
            case loaded_upgrade:kv_valgen(Key) == riakc_obj:get_value(Val) of
                true -> cool;
                _ -> ReportPid ! {kv, Node, not_equal}
            end;
        {error, disconnected} ->
            ok;
        {error, notfound} ->
            ReportPid ! {kv, Node, {notfound, Key}};
        Unexpected ->
            ReportPid ! {kv, Node, Unexpected}
    end,
    kv_tester(Node, Count + 1, PBC, Vsn, ReportPid).

mapred_tester(Node, Count, Pid, Vsn, ReportPid) ->
    PBC = pb_pid_recycler(Pid, Node),
    case riakc_pb_socket:mapred(PBC, loaded_upgrade:bucket(mapred), loaded_upgrade:erlang_mr()) of
        {ok, [{1, [10000]}]} ->
            ok;
        {ok, R} ->
            lager:warning("Bad MR result: ~p", [R]),
            ReportPid ! {mapred, Node, bad_result};
        {error, disconnected} ->
            ok;
        %% Finkmaster Flex says timeouts are ok
        {error, timeout} ->
            ok;
        {error, {timeout, _}} ->
            ok;
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"{badmatch,{'EXIT',noproc}}", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"{badmatch,{'EXIT',timeout}}", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":0,\"error\":\"badarg", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":0,\"error\":\"[preflist_exhausted]", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":0,\"error\":\"{badmatch,{'EXIT',timeout}}", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"function_clause\",\"input\":\"{cover,", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":\"listkeys\",\"error\":\"badarg\",\"input\":\"{cover,", _/binary>>} ->
            ok;
        {error, <<"Error processing stream message: exit:{ucs,{bad_utf8_character_code}}:[{xmerl_ucs,", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":0,\"error\":\"[{vnode_down,{shutdown,{gen_fsm,sync_send_event,", _/binary>>} ->
            ok;
        {error, <<"{\"phase\":0,\"error\":\"[{vnode_down,noproc}]", _/binary>>} ->
            ok;
        Unexpected ->
            ReportPid ! {mapred, Node, Unexpected}
    end,
    mapred_tester(Node, Count + 1, PBC, Vsn, ReportPid).

twoi_tester(Node, 0, undefined, legacy, ReportPid) ->
    lager:warning("Legacy nodes do not have 2i load applied"),
    twoi_tester(Node, 1, undefined, legacy, ReportPid);
twoi_tester(Node, Count, Pid, legacy, ReportPid) ->
    twoi_tester(Node, Count + 1, Pid, legacy, ReportPid);
twoi_tester(Node, Count, Pid, Vsn, ReportPid) ->
    PBC = pb_pid_recycler(Pid, Node),
    Key = Count rem 8000,
    ExpectedKeys = [loaded_upgrade:int_to_key(Key)],
    case {
      riakc_pb_socket:get_index(
                              PBC,
                              loaded_upgrade:bucket(twoi),
                              {binary_index, "plustwo"},
                              loaded_upgrade:int_to_key(Key + 2)),
      riakc_pb_socket:get_index(
                              PBC,
                              loaded_upgrade:bucket(twoi),
                              {integer_index, "plusone"},
                              Key + 1)
     } of
        {{ok, ?INDEX_RESULTS{keys=BinKeys}}, {ok, ?INDEX_RESULTS{keys=IntKeys}}} ->
            case {assert_equal(ExpectedKeys, BinKeys), assert_equal(ExpectedKeys, IntKeys)} of
                {true, true} -> cool;
                {false, false} ->
                    ReportPid ! {twoi, Node, bolth_no_match};
                {false, true} ->
                    ReportPid ! {twoi, Node, bin_no_match};
                {true, false} ->
                    ReportPid ! {twoi, Node, int_no_match}
            end;
        {{error, Reason}, _} ->
            ReportPid ! {twoi, Node, {error, Reason}};
        {_, {error, Reason}} ->
            ReportPid ! {twoi, Node, {error, Reason}};
        Unexpected ->
            ReportPid ! {twoi, Node, Unexpected}
    end,
    twoi_tester(Node, Count + 1, PBC, Vsn, ReportPid).


assert_equal(Expected, Actual) ->
    case Expected -- Actual of
        [] -> ok;
        Diff -> lager:info("Expected -- Actual: ~p", [Diff])
    end,
    Actual == Expected.

pb_pid_recycler(undefined, Node) ->
    rt:pbc(Node);
pb_pid_recycler(Pid, Node) ->
    case riakc_pb_socket:is_connected(Pid) of
        true ->
            Pid;
        _ ->
            riakc_pb_socket:stop(Pid),
            rt:pbc(Node)
    end.
