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
%%% @copyright (C) 2014, Basho Technologies
%%% @doc
%%% riak_test for kv679 lost clock flavour.
%%%
%%% issue kv679 is a possible dataloss issue, it's basically caused by
%%% the fact that per key logical clocks can go backwards in time in
%%% certain situations. The situation under test here is as follows:
%%% Create value, write N times
%%% Fail to locally read value on coordinate (corruption, error, solar flare)
%%% write new value (new value, new clock!)
%%% replicate new value
%%% replicas drop write as clock is dominated
%%% read repair removes value. Data loss.
%%% @end

-module(kv679_dataloss).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"kv679">>).
-define(KEY, <<"test">>).

confirm() ->
    Conf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [{allow_mult, true},
                                                 {dvv_enabled, true}]}]},
            {bitcask, [{sync_strategy, o_sync}, {io_mode, nif}]}],

    [Node] = rt:deploy_nodes(1, Conf),
    Client = rt:pbc(Node),
    riakc_pb_socket:set_options(Client, [queue_if_disconnected]),

    %% Get preflist for key
    %% assuming that the head of the preflist on a single node cluster
    %% will always coordinate the writes
    PL = kv679_tombstone:get_preflist(Node),

    %% Write key some times
    write_key(Client, <<"phil">>, []),

    {ok, Bod} =  write_key(Client, <<"bob">>, [return_body]),

    lager:info("wrote value <<bob>>"),

    VCE0 = riakc_obj:vclock(Bod),
    VC0 = rpc:call(Node, riak_object, decode_vclock, [VCE0]),
    lager:info("VC ~p~n", [VC0]),


    %% delete the local data for Key
    %% ERM why not just stop the node, then delete the data dir?
    delete_datadir(hd(PL)),

    timer:sleep(10000),

    {ok, Bod2} = write_key(Client, <<"jon">>, [return_body]),

    VCE1 = riakc_obj:vclock(Bod2),
    VC1 = rpc:call(Node, riak_object, decode_vclock, [VCE1]),
    lager:info("VC ~p~n", [VC1]),


    lager:info("wrote value <<jon>>"),

    %% At this point, two puts with empty contexts should be siblings
    %% due to the data loss at the coordinator we lose the second
    %% write

    Res = riakc_pb_socket:get(Client, ?BUCKET, ?KEY, []),

    ?assertMatch({ok, _}, Res),
    {ok, O} = Res,

    VCE = riakc_obj:vclock(O),
    VC = rpc:call(Node, riak_object, decode_vclock, [VCE]),
    lager:info("VC ~p~n", [VC]),

    ?assertEqual([<<"bob">>, <<"jon">>, <<"phil">>], lists:sort(riakc_obj:get_values(O))),

    pass.

write_key(Client, Val, Opts) ->
    write_object(Client, riakc_obj:new(?BUCKET, ?KEY, Val), Opts).

write_object(Client, Object, Opts) ->
    riakc_pb_socket:put(Client, Object, Opts).

delete_datadir({{Idx, Node}, Type}) ->
    %% stop node
    lager:info("deleting backend data dir for ~p ~p on ~p",
               [Idx, Node, Type]),
    %% Get default backend
    Backend = rpc:call(Node, app_helper, get_env, [riak_kv, storage_backend]),

    %% get name from mod
    BackendName = backend_name_from_mod(Backend),
    %% get data root for type
    DataRoot = rpc:call(Node, app_helper, get_env, [BackendName, data_root]),
    %% get datadir from Idx
    Path = filename:join([rtdev:relpath(current),
                          "dev",
                          "dev"++ integer_to_list(rtdev:node_id(Node)),
                          DataRoot,
                          integer_to_list(Idx)]),
    lager:info("Path ~p~n", [Path]),

    rt:stop_and_wait(Node),

%%    vnode_util:kill_vnode({Idx, Node}),
    del_dir(Path),
    rt:start_and_wait(Node).

backend_name_from_mod(riak_kv_bitcask_backend) ->
    bitcask;
backend_name_from_mod(riak_kv_eleveldb_backend) ->
    eleveldb.

del_dir(Dir) ->
   lists:foreach(fun(D) ->
                    ok = file:del_dir(D)
                 end, del_all_files([Dir], [])).

del_all_files([], EmptyDirs) ->
   EmptyDirs;
del_all_files([Dir | T], EmptyDirs) ->
   {ok, FilesInDir} = file:list_dir(Dir),
   {Files, Dirs} = lists:foldl(fun(F, {Fs, Ds}) ->
                                  Path = Dir ++ "/" ++ F,
                                  case filelib:is_dir(Path) of
                                     true ->
                                          {Fs, [Path | Ds]};
                                     false ->
                                          {[Path | Fs], Ds}
                                  end
                               end, {[],[]}, FilesInDir),
   lists:foreach(fun(F) ->
                         ok = file:delete(F)
                 end, Files),
   del_all_files(T ++ Dirs, [Dir | EmptyDirs]).
