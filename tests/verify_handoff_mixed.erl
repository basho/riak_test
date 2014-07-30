%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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

%% @doc Test basic handoff in mixed-version clusters. This was born
%% out of a bug found in the upgrade of vnode fold requests:
%% https://github.com/basho/riak/issues/407
%%
%% Basic test:
%%  - load data into a new node
%%  - join an old node to it
%%  - wait for handoff to finish
%%
%% Node versions used are `current' and whatever the test runner has
%% set the `upgrade_version' metadata to (`previous' by default).
%%
%% Handoff uses riak_core_fold_req_v* commands, and riak issue #407
%% tracked a problem with upgrading that command from 1.4.2 format to
%% 2.0.0pre3 format.
-module(verify_handoff_mixed).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include("rt_pipe.hrl").

-define(KV_BUCKET, <<"vhm_kv">>).
-define(KV_COUNT, 1000).

-define(SEARCH_BUCKET, <<"vhm_search">>).
-define(SEARCH_COUNT, 1000).

-define(PIPE_COUNT, 100).

-define(FOLD_CAPABILITY, {riak_core,fold_req_version}).

confirm() ->
    %% this `upgrade_version' lookup was copied from loaded_upgrade
    UpgradeVsn = proplists:get_value(upgrade_version,
                                     riak_test_runner:metadata(),
                                     previous),
    SearchEnabled = [{riak_search, [{enabled, true}]}],
    Versions = [{current, SearchEnabled},
                {UpgradeVsn, SearchEnabled}],
    Services = [riak_kv, riak_search, riak_pipe],
    [Current, Old] = Nodes = rt_cluster:deploy_nodes(Versions, Services),

    prepare_vnodes(Current),

    %% before joining, learn what fold req the old version used,
    %% so we can know when the cluster has negotiated to it
    OldFold = rt:capability(Old, ?FOLD_CAPABILITY, v1),

    %% now link the nodes together and wait for handoff to complete
    ok = rt:join(Old, Current),
    ok = rt:wait_until_all_members(Nodes),
    ok = rt:wait_until_ring_converged(Nodes),

    %% the calls to ..._no_pending_changes and ..._transfers_complete
    %% speed up the timing of handoff such that it will happen before
    %% capability renegotiation if we don't wait here - this is still
    %% technically race-prone, but negotiation usually happens *much*
    %% sooner than handoff at normal timing
    lager:info("Wait for fold_req_version == ~p", [OldFold]),
    ok = rt:wait_until_capability(Current, ?FOLD_CAPABILITY, OldFold),

    %% this will timeout if wrong fix is in place
    %% (riak_kv_vnode would infinite-loop v1 fold requests)
    %% or if no fix is in place
    %% (riak_pipe_vnode would drop v1 fold requests on the floor)
    ok = rt:wait_until_no_pending_changes(Nodes),
    ok = rt:wait_until_transfers_complete(Nodes),

    %% this will error if wrong fix is in place
    %% (riak_search forward v1 fold requests)
    ok = check_logs(),
    pass.

%% @doc get vnodes running on Node, such that they'll be ready to
%% handoff when we join the other node
prepare_vnodes(Node) ->
    prepare_kv_vnodes(Node),
    prepare_search_vnodes(Node),
    prepare_pipe_vnodes(Node).

prepare_kv_vnodes(Node) ->
    lager:info("Preparing KV vnodes with keys 1-~b in bucket ~s",
               [?KV_COUNT, ?KV_BUCKET]),
    C = rt_pb:pbc(Node),
    lists:foreach(
      fun(KV) ->
              ok = riakc_pb_socket:put(C, riakc_obj:new(?KV_BUCKET, KV, KV))
      end,
      [ list_to_binary(integer_to_list(N)) || N <- lists:seq(1, ?KV_COUNT) ]),
    riakc_pb_socket:stop(C).

prepare_search_vnodes(Node) ->
    lager:info("Peparing Search vnodes with keys 1000-~b in bucket ~s",
               [1000+?SEARCH_COUNT, ?SEARCH_BUCKET]),
    rt:enable_search_hook(Node, ?SEARCH_BUCKET),
    C = rt_pb:pbc(Node),
    lists:foreach(
      fun(KV) ->
              O = riakc_obj:new(?SEARCH_BUCKET, KV, KV, "text/plain"),
              ok = riakc_pb_socket:put(C, O)
      end,
      [ list_to_binary(integer_to_list(N))
        || N <- lists:seq(1000, 1000+?SEARCH_COUNT) ]),
    riakc_pb_socket:stop(C).

prepare_pipe_vnodes(Node) ->
    %% the riak_pipe_w_pass worker produces no archive, but the vnode
    %% still sends its queue (even if empty) through handoff
    Spec = [#fitting_spec{name=vhm, module=riak_pipe_w_pass}],
    %% keep outputs out of our mailbox
    DummySink = spawn_link(fun() -> receive never -> ok end end),
    Options = [{sink, #fitting{pid=DummySink}}],

    lager:info("Filling a pipe with ~b inputs", [?PIPE_COUNT]),
    {ok, Pipe} = rpc:call(Node, riak_pipe, exec, [Spec, Options]),
    lists:foreach(
      fun(I) -> ok = rpc:call(Node, riak_pipe, queue_work, [Pipe, I]) end,
      lists:seq(1, ?PIPE_COUNT)).

check_logs() ->
    AppCounts = sum_app_handoff(),
    lager:info("Found handoff counts in logs: ~p", [AppCounts]),

    %% make sure all of our apps completed some handoff
    ExpectedApps = lists:sort([riak_kv_vnode,
                               riak_search_vnode,
                               riak_pipe_vnode]),
    FoundApps = lists:sort([ A || {A, _} <- AppCounts ]),
    ?assertEqual(ExpectedApps, FoundApps),

    ZeroHandoff = [ A || {_, Count}=A <- AppCounts,
                         Count == 0 ],
    %% none of these apps should be reporting zero objects handed off
    ?assertEqual([], ZeroHandoff),
    ok.

sum_app_handoff() ->
    lager:info("Combing logs for handoff notes"),
    lists:foldl(
      fun({App, Count}, Acc) ->
              orddict:update_counter(App, Count, Acc)
      end,
      [],
      lists:append([ find_app_handoff(Log) || Log <- rt:get_node_logs() ])).

find_app_handoff({Path, Port}) ->
    case re:run(Path, "console\.log$") of
        {match, _} ->
            find_line(Port, file:read_line(Port));
        nomatch ->
            %% save time not looking through other logs
            []
    end.

find_line(Port, {ok, Data}) ->
    Re = "ownership transfer of ([a-z_]+).*"
        "completed.*([0-9]+) objects",
    case re:run(Data, Re, [{capture, all_but_first, list}]) of
        {match, [App, Count]} ->
            [{list_to_atom(App), list_to_integer(Count)}
             |find_line(Port, file:read_line(Port))];
        nomatch ->
            find_line(Port, file:read_line(Port))
    end;
find_line(_, _) ->
    [].
