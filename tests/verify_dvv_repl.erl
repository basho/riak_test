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

%%% @copyright (C) 2013, Basho Technologies
%%% @doc
%%% riak_test repl caused sibling explosion
%%% Encodes scenario as described to me in hipchat.
%%% @end

-module(verify_dvv_repl).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"dvv-repl-bucket">>).
-define(KEY, <<"dvv-repl-key">>).
-define(KEY2, <<"dvv-repl-key2">>).

confirm() ->
    inets:start(),

    {{ClientA, ClusterA}, {ClientB, ClusterB}} = make_clusters(),

    %% Write data to B
    write_object(ClientB),

    %% Connect for real time repl A->B
    connect_realtime(ClusterA, ClusterB),

    IsReplicating = make_replicate_test_fun(ClientA, ClientB),

    rt:wait_until(IsReplicating),

    %% Update ClusterA 100 times
    [write_object(ClientA) || _ <- lists:seq(1, 100)],

    %% Get the object, and see if it has 100 siblings (not the two it should have)
    AObj = get_object(ClientA),
    BObj = get_object(ClientB),

    ?assertEqual(1, riakc_obj:value_count(AObj)),
    ?assertEqual(2, riakc_obj:value_count(BObj)),

    pass.


make_replicate_test_fun(From, To) ->
    fun() ->
            Obj = riakc_obj:new(?BUCKET, ?KEY2, <<"am I replicated yet?">>),
            ok = riakc_pb_socket:put(From, Obj),
            case riakc_pb_socket:get(To, ?BUCKET, ?KEY2) of
                {ok, _} ->
                    true;
                {error, notfound} ->
                    false
            end
    end.

make_clusters() ->
    Conf = [{riak_repl, [{fullsync_on_connect, false},
                         {fullsync_interval, disabled}]},
           {riak_core, [{default_bucket_props,
                         [{dvv_enabled, true},
                          {allow_mult, true}]}]}],
    Nodes = rt:deploy_nodes(6, Conf),
    {ClusterA, ClusterB} = lists:split(3, Nodes),
    A = make_cluster(ClusterA, "A"),
    B = make_cluster(ClusterB, "B"),
    {A, B}.

make_cluster(Nodes, Name) ->
    repl_util:make_cluster(Nodes),
    repl_util:name_cluster(hd(Nodes), Name),
    repl_util:wait_until_leader_converge(Nodes),
    C = rt:pbc(hd(Nodes)),
    riakc_pb_socket:set_options(C, [queue_if_disconnected]),
    {C, Nodes}.

write_object([]) ->
    ok;
write_object([Client | Rest]) ->
    ok = write_object(Client),
    write_object(Rest);
write_object(Client) ->
    fetch_resolve_write(Client).

get_object(Client) ->
    case riakc_pb_socket:get(Client, ?BUCKET, ?KEY) of
        {ok, Obj} ->
            Obj;
        _ ->
            riakc_obj:new(?BUCKET, ?KEY)
    end.

fetch_resolve_write(Client) ->
    Obj = get_object(Client),
    Value = resolve_update(riakc_obj:get_values(Obj)),
    Obj3 = riakc_obj:update_metadata(riakc_obj:update_value(Obj, Value), dict:new()),
    ok = riakc_pb_socket:put(Client, Obj3).

resolve_update([]) ->
    sets:add_element(1, sets:new());
resolve_update(Values) ->
    Resolved = lists:foldl(fun(V0, Acc) ->
                        V = binary_to_term(V0),
                        sets:union(V, Acc)
                end,
                sets:new(),
                Values),
    NewValue = lists:max(sets:to_list(Resolved)),
    sets:add_element(NewValue, Resolved).

%% Set up one way RT repl
connect_realtime(ClusterA, ClusterB) ->
    lager:info("repl power...ACTIVATE!"),
    LeaderA = get_leader(hd(ClusterA)),
    MgrPortB = get_mgr_port(hd(ClusterB)),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", MgrPortB),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    repl_util:enable_realtime(LeaderA, "B"),
    repl_util:start_realtime(LeaderA, "B").

get_leader(Node) ->
    rpc:call(Node, riak_core_cluster_mgr, get_leader, []).

get_mgr_port(Node) ->
    {ok, {_IP, Port}} = rpc:call(Node, application, get_env,
                                 [riak_core, cluster_mgr]),
    Port.
