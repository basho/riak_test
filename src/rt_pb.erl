%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(rt_pb).
-include("rt.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([pbc/1,
         pbc/2,
         stop/1,
         pbc_read/3,
         pbc_read/4,
         pbc_read_check/4,
         pbc_read_check/5,
         pbc_set_bucket_prop/3,
         pbc_write/4,
         pbc_write/5,
         pbc_put_dir/3,
         pbc_put_file/4,
         pbc_really_deleted/3,
         pbc_systest_write/2,
         pbc_systest_write/3,
         pbc_systest_write/5,
         pbc_systest_read/2,
         pbc_systest_read/3,
         pbc_systest_read/5,
         get_pb_conn_info/1]).

-define(HARNESS, (rt_config:get(rt_harness))).

%% @doc get me a protobuf client process and hold the mayo!
-spec pbc(node()) -> pid().
pbc(Node) ->
    pbc(Node, [{auto_reconnect, true}]).

-spec pbc(node(), proplists:proplist()) -> pid().
pbc(Node, Options) ->
    rt2:wait_for_service(Node, riak_kv),
    ConnInfo = proplists:get_value(Node, get_pb_conn_info([Node])),
    {IP, PBPort} = proplists:get_value(pb, ConnInfo),
    {ok, Pid} = riakc_pb_socket:start_link(IP, PBPort, Options),
    Pid.

stop(Pid) ->
    riakc_pb_socket:stop(Pid).

%% @doc does a read via the erlang protobuf client
-spec pbc_read(pid(), binary(), binary()) -> binary().
pbc_read(Pid, Bucket, Key) ->
    pbc_read(Pid, Bucket, Key, []).

-spec pbc_read(pid(), binary(), binary(), [any()]) -> binary().
pbc_read(Pid, Bucket, Key, Options) ->
    {ok, Value} = riakc_pb_socket:get(Pid, Bucket, Key, Options),
    Value.

-spec pbc_read_check(pid(), binary(), binary(), [any()]) -> boolean().
pbc_read_check(Pid, Bucket, Key, Allowed) ->
    pbc_read_check(Pid, Bucket, Key, Allowed, []).

-spec pbc_read_check(pid(), binary(), binary(), [any()], [any()]) -> boolean().
pbc_read_check(Pid, Bucket, Key, Allowed, Options) ->
    case riakc_pb_socket:get(Pid, Bucket, Key, Options) of
        {ok, _} ->
            true = lists:member(ok, Allowed);
        Other ->
            lists:member(Other, Allowed) orelse throw({failed, Other, Allowed})
    end.

%% @doc does a write via the erlang protobuf client
-spec pbc_write(pid(), binary(), binary(), binary()) -> atom().
pbc_write(Pid, Bucket, Key, Value) ->
    Object = riakc_obj:new(Bucket, Key, Value),
    riakc_pb_socket:put(Pid, Object).

%% @doc does a write via the erlang protobuf client plus content-type
-spec pbc_write(pid(), binary(), binary(), binary(), list()) -> atom().
pbc_write(Pid, Bucket, Key, Value, CT) ->
    Object = riakc_obj:new(Bucket, Key, Value, CT),
    riakc_pb_socket:put(Pid, Object).

%% @doc sets a bucket property/properties via the erlang protobuf client
-spec pbc_set_bucket_prop(pid(), binary(), [proplists:property()]) -> atom().
pbc_set_bucket_prop(Pid, Bucket, PropList) ->
    riakc_pb_socket:set_bucket(Pid, Bucket, PropList).

%% @doc Puts the contents of the given file into the given bucket using the
%% filename as a key and assuming a plain text content type.
pbc_put_file(Pid, Bucket, Key, Filename) ->
    {ok, Contents} = file:read_file(Filename),
    riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Contents, "text/plain")).

%% @doc Puts all files in the given directory into the given bucket using the
%% filename as a key and assuming a plain text content type.
pbc_put_dir(Pid, Bucket, Dir) ->
    lager:info("Putting files from dir ~p into bucket ~p", [Dir, Bucket]),
    {ok, Files} = file:list_dir(Dir),
    [pbc_put_file(Pid, Bucket, list_to_binary(F), filename:join([Dir, F]))
     || F <- Files].

%% @doc True if the given keys have been really, really deleted.
%% Useful when you care about the keys not being there. Delete simply writes
%% tombstones under the given keys, so those are still seen by key folding
%% operations.
pbc_really_deleted(Pid, Bucket, Keys) ->
    StillThere =
    fun(K) ->
            Res = riakc_pb_socket:get(Pid, Bucket, K,
                                      [{r, 1},
                                      {notfound_ok, false},
                                      {basic_quorum, false},
                                      deletedvclock]),
            case Res of
                {error, notfound} ->
                    false;
                _ ->
                    %% Tombstone still around
                    true
            end
    end,
    [] == lists:filter(StillThere, Keys).

%% @doc PBC-based version of {@link systest_write/1}
pbc_systest_write(Node, Size) ->
    pbc_systest_write(Node, Size, 2).

pbc_systest_write(Node, Size, W) ->
    pbc_systest_write(Node, 1, Size, <<"systest">>, W).

pbc_systest_write(Node, Start, End, Bucket, W) ->
    rt:wait_for_service(Node, riak_kv),
    Pid = pbc(Node),
    F = fun(N, Acc) ->
                Obj = riakc_obj:new(Bucket, <<N:32/integer>>, <<N:32/integer>>),
                try riakc_pb_socket:put(Pid, Obj, W) of
                    ok ->
                        Acc;
                    Other ->
                        [{N, Other} | Acc]
                catch
                    What:Why ->
                        [{N, {What, Why}} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

pbc_systest_read(Node, Size) ->
    pbc_systest_read(Node, Size, 2).

pbc_systest_read(Node, Size, R) ->
    pbc_systest_read(Node, 1, Size, <<"systest">>, R).

pbc_systest_read(Node, Start, End, Bucket, R) ->
    rt:wait_for_service(Node, riak_kv),
    Pid = pbc(Node),
    F = fun(N, Acc) ->
                case riakc_pb_socket:get(Pid, Bucket, <<N:32/integer>>, R) of
                    {ok, Obj} ->
                        case riakc_obj:get_value(Obj) of
                            <<N:32/integer>> ->
                                Acc;
                            WrongVal ->
                                [{N, {wrong_val, WrongVal}} | Acc]
                        end;
                    Other ->
                        [{N, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

-spec get_pb_conn_info(node()) -> [{inet:ip_address(), pos_integer()}].
get_pb_conn_info(Node) ->
    case rt:rpc_get_env(Node, [{riak_api, pb},
                            {riak_api, pb_ip},
                            {riak_kv, pb_ip}]) of
        {ok, [{NewIP, NewPort}|_]} ->
            {ok, [{NewIP, NewPort}]};
        {ok, PB_IP} ->
            {ok, PB_Port} = rt:rpc_get_env(Node, [{riak_api, pb_port},
                                               {riak_kv, pb_port}]),
            {ok, [{PB_IP, PB_Port}]};
        _ ->
            undefined
    end.
