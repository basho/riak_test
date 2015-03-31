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
%% -------------------------------------------------------------------

-module(api_entrypoints).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-behavior(riak_test).
-export([confirm/0]).

confirm() ->

    lager:info("Set up a 1-node cluster to test discovery of API entry points"),
    PbcPort = 10017,
    rt:set_conf(all, [{"listener.protobuf.internal",
                       lists:flatten(io_lib:format("0.0.0.0:~b", [PbcPort]))}]),
    [Node1] = Nodes =
        rt:build_cluster(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    Pid = rt:pbc(Node1),

    lager:info("Do getifaddrs ourself"),
    {_Ifaces, LocalAddrs} = lists:unzip(get_routed_interfaces()),
    lager:info("Local routed addresses: ~p", [LocalAddrs]),

    lager:info("riakc_pb_socket:get_api_entry_points/3"),
    {ok, EPList} =
        riakc_pb_socket:get_api_entry_points(Pid, pbc, [{force_update, true}]),

    {Addr1_, Port1, LastChecked1} = hd(EPList),
    Addr1 = case Addr1_ of
               <<"not_routed">> -> not_routed;
               Addr_ ->
                   {ok, Parsed} = inet:parse_address(binary_to_list(Addr_)),
                   Parsed
           end,
    lager:info("Discovered pbc listener address: ~p:~p, last checked ~p", [Addr1, Port1, LastChecked1]),
    ?assert(lists:member(Addr1, LocalAddrs) orelse LocalAddrs =:= []),
    lager:info("Looks good"),

    %% lager:info("Stop the nodes"),
    %% [rt:stop(Node) || Node <- Nodes],
    pass.


%% ===================================================================
%% Local functions
%% ===================================================================

-spec get_routed_interfaces() -> [{Iface::string(), inet:ip_address()}].
%% @private
%% @doc Returns the {iface, address} pairs of all non-loopback, bound
%%      interfaces in the underlying OS.
%% (copied from riak_api/riak_api_lib.erl)
get_routed_interfaces() ->
    case inet:getifaddrs() of
        {ok, Ifaces} ->
            lists:filtermap(
              fun({Iface, Details}) ->
                      case is_routed_addr(Details) of
                          undefined ->
                              false;
                          Addr ->
                              {true, {Iface, Addr}}
                      end
              end,
              Ifaces);
        {error, PosixCode} ->
            _ = lager:log(error, self(), "Failed to enumerate network ifaces: ~p", [PosixCode]),
            []
    end.

-spec is_routed_addr([{Ifname::string(), Ifopt::[{atom(), any()}]}]) ->
    inet:ip_address() | undefined.
%% @private
%% (copied from riak_api/riak_api_lib.erl)
is_routed_addr(Details) ->
    Flags = proplists:get_value(flags, Details),
    case {(is_list(Flags) andalso
           not lists:member(loopback, Flags)),
          proplists:get_all_values(addr, Details)} of
        {true, [_|_] = Ipv4AndPossibly6} ->
            hd(lists:sort(Ipv4AndPossibly6));
        _ ->
            undefined
    end.
