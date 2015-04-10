%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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

-module(rt_bucket_types).
-include_lib("eunit/include/eunit.hrl").

-export([create_and_wait/3,
         create_and_activate_bucket_type/3,
         wait_until_bucket_type_visible/2,
         wait_until_bucket_type_status/3,
         wait_until_bucket_props/3]).

-include("rt.hrl").

%% Specify the bucket_types field for the properties record. The list
%% of bucket types may have two forms, a bucket_type or a pair
%% consisting of an integer and a bucket_type. The latter form
%% indicates that a bucket_type should only be applied to the cluster
%% with the given index. The former form is applied to all clusters.
-type bucket_type() :: {binary(), proplists:proplist()}.
-type bucket_types() :: [bucket_type() | {pos_integer(), bucket_type()}].

-export_type([bucket_types/0]).

-spec create_and_wait([node()], binary(), proplists:proplist()) -> ok.
create_and_wait(Nodes, Type, Properties) ->
    create_and_activate_bucket_type(hd(Nodes), Type, Properties),
    wait_until_bucket_type_status(Type, active, Nodes),
    wait_until_bucket_type_visible(Nodes, Type).

%% @doc create and immediately activate a bucket type
create_and_activate_bucket_type(Node, Type, Props) ->
    ok = rpc:call(Node, riak_core_bucket_type, create, [Type, Props]),
    wait_until_bucket_type_status(Type, ready, Node),
    ok = rpc:call(Node, riak_core_bucket_type, activate, [Type]),
    wait_until_bucket_type_status(Type, active, Node).

wait_until_bucket_type_status(Type, ExpectedStatus, Nodes) when is_list(Nodes) ->
    [wait_until_bucket_type_status(Type, ExpectedStatus, Node) || Node <- Nodes];
wait_until_bucket_type_status(Type, ExpectedStatus, Node) ->
    F = fun() ->
                ActualStatus = rpc:call(Node, riak_core_bucket_type, status, [Type]),
                ExpectedStatus =:= ActualStatus
        end,
    ?assertEqual(ok, rt:wait_until(F)).

-spec bucket_type_visible([atom()], binary()|{binary(), binary()}) -> boolean().
bucket_type_visible(Nodes, Type) ->
    MaxTime = rt_config:get(rt_max_receive_wait_time),
    IsVisible = fun erlang:is_list/1,
    {Res, NodesDown} = rpc:multicall(Nodes, riak_core_bucket_type, get, [Type], MaxTime),
    NodesDown == [] andalso lists:all(IsVisible, Res).

wait_until_bucket_type_visible(Nodes, Type) ->
    F = fun() -> bucket_type_visible(Nodes, Type) end,
    ?assertEqual(ok, rt:wait_until(F)).

-spec see_bucket_props([atom()], binary()|{binary(), binary()},
                       proplists:proplist()) -> boolean().
see_bucket_props(Nodes, Bucket, ExpectProps) ->
    MaxTime = rt_config:get(rt_max_receive_wait_time),
    IsBad = fun({badrpc, _}) -> true;
               ({error, _}) -> true;
               (Res) when is_list(Res) -> false
            end,
    HasProps = fun(ResProps) ->
                       lists:all(fun(P) -> lists:member(P, ResProps) end,
                                 ExpectProps)
               end,
    case rpc:multicall(Nodes, riak_core_bucket, get_bucket, [Bucket], MaxTime) of
        {Res, []} ->
            % No nodes down, check no errors
            case lists:any(IsBad, Res) of
                true  ->
                    false;
                false ->
                    lists:all(HasProps, Res)
            end;
        {_, _NodesDown} ->
            false
    end.

wait_until_bucket_props(Nodes, Bucket, Props) ->
    F = fun() ->
                see_bucket_props(Nodes, Bucket, Props)
        end,
    ?assertEqual(ok, rt:wait_until(F)).
