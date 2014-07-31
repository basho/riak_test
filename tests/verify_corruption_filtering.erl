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
-module(verify_corruption_filtering).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [build_cluster/1,
             leave/1,
             wait_until_unpingable/1,
             status_of_according_to/2,
             remove/2]).
%% Test plan:
%%   - build a 2-node cluster
%%   - load values
%%   - intercept the backend so that it sometimes returns 
%%     bad data
%%   - test puts, gets, folds, and handoff.




confirm() ->
    Nodes = build_cluster(2),
    [Node1, Node2] = Nodes,
    rt:wait_until_pingable(Node2),

    load_cluster(Node1),
   
    lager:info("Cluster loaded"),

    case rt_config:get(rt_backend, undefined) of 
        riak_kv_eleveldb_backend ->
            load_level_intercepts(Nodes);
        _ ->
            load_bitcask_intercepts(Nodes)
    end,
    
    get_put_mix(Node1),

    %% Have node2 leave
    lager:info("Have ~p leave", [Node2]),
    leave(Node2),
    ?assertEqual(ok, wait_until_unpingable(Node2)),
             
    %% we'll never get here or timeout if this issue 
    %% isn't fixed.
    pass.

get_put_mix(Node) ->
    PB = rt_pb:pbc(Node),
    [begin
         Key = random:uniform(1000),
         case random:uniform(2) of
             1 ->
                 X = crypto:rand_bytes(512),
                 riakc_pb_socket:put(PB, 
                                     riakc_obj:new(<<"foo">>, <<Key>>,
                                                   X));
             2 ->
                 case riakc_pb_socket:get(PB, <<"foo">>, <<Key>>) of
                     {error, notfound} ->
                         ok;
                     {error, Reason} ->
                         lager:error("got unexpected return: ~p",
                                     [Reason]),
                         throw(Reason);
                     {ok, _O} -> ok;
                     Else -> throw(Else)
                 end
         end
     end 
    || _ <- lists:seq(1, 2000)].

load_cluster(Node) -> 
    PB = rt_pb:pbc(Node),
    [riakc_pb_socket:put(PB, 
                         riakc_obj:new(<<"foo">>, <<X>>,
                                       <<X:4096>>))
     || X <- lists:seq(1,1000)].

load_level_intercepts(Nodes) ->
    [begin
         rt_intercept:add(Node, {riak_kv_eleveldb_backend,
                                 [{{get, 3}, corrupting_get}]}),
         rt_intercept:add(Node, {riak_kv_eleveldb_backend,
                                 [{{put, 5}, corrupting_put}]}),
         rt_intercept:add(Node, {riak_kv_vnode,
                                 [{{handle_handoff_data, 2}, 
                                 corrupting_handle_handoff_data}]})
     end 
     || Node <- Nodes].

load_bitcask_intercepts(Nodes) ->
    [begin
         rt_intercept:add(Node, {riak_kv_bitcask_backend,
                                 [{{get, 3}, corrupting_get}]}),
         rt_intercept:add(Node, {riak_kv_bitcask_backend,
                                 [{{put, 5}, corrupting_put}]}),
         rt_intercept:add(Node, {riak_kv_vnode,
                                 [{{handle_handoff_data, 2}, 
                                   corrupting_handle_handoff_data}]})
     end 
     || Node <- Nodes].
