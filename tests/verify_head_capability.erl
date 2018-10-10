%% -------------------------------------------------------------------
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
%%% @doc
%%% riak_test for vnode capability controlling HEAD request use in get
%%% fsm
%%% @end

-module(verify_head_capability).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"test-bucket">>).

confirm() ->
    %% Create a mixed cluster of current and previous
    %% Create a PB client
    %% Do some puts so we have some data
    %% do GETs, they should all be _gets_ at the vnode
    %% Upgrade nodes to current
    %% Do some GETS
    %% check that HEADs happen after cap is negotiated
    [Prev1, Prev2, _Curr1, _Curr2] = rt:build_cluster([previous, previous, current, current]),

    PrevPB = rt:pbc(Prev1),

    Res = rt:systest_write(Prev1, 1, 50, ?BUCKET, 2),

    ?assertEqual([], Res),

    ReadRes = rt:systest_read(Prev1, 1, 50, ?BUCKET, 2),

    ?assertEqual([], ReadRes),

    riakc_pb_socket:stop(PrevPB),

    rt:upgrade(Prev1, current),
    rt:upgrade(Prev2, current),

    PrevPB2 = rt:pbc(Prev1),

    ?assertEqual(ok, rt:wait_until_capability(Prev1, {riak_kv, get_request_type}, head)),

    riakc_pb_socket:stop(PrevPB2),

    pass.
