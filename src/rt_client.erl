%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
-module(rt_client).

-export([
         client_vsn/0,
         set_up_slave_for_previous_client/1
        ]).

-spec client_vsn() -> string().
client_vsn() ->
    application:load(riakc),
    {ok, Vsn} = application:get_key(riakc, vsn),
    Vsn.

-spec set_up_slave_for_previous_client(node()) -> node().
set_up_slave_for_previous_client(SlaveNode) ->
    {ok, SlaveNode} = rt_slave:start(SlaveNode, "-setcookie riak"),
    PrevRiakcPath =
        hd(filelib:wildcard(
             filename:join(
               [rtdev:relpath(previous),
                "dev/dev1/lib/riakc-*/ebin"]))),
    true = rpc:call(
             SlaveNode, code, replace_path, ["riakc", PrevRiakcPath]),
    SlaveNode.
