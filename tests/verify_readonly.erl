%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Erlang Solutions Limited.
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
-module(verify_readonly).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"B0">>).
-define(POST_MULT, 100).
-define(ITEMS, 1000).

%% There has been a problem where a persisted backend (eleveldb) would not
%% prompt a vnode crash if it could no longer write to the file.  Without
%% the crash bubbling up to bring the node down, fallbacks are not elected.
%%
%% The test starts a cluster, loads some data, changes the file permissions to
%% read only, writes a lot more data ... and then confirms:
%% - The vast majority of PUTs work (we might still see o(1) failures during
%% cycle of crashes)
%% - At the end of the test the node we expect to fail is down
%%
%% This test will fail with an eleveldb backend as of Riak 2.9.0, but should
%% pass with leveled and bitcask backends.  It is expected to fail with a
%% memory backend

confirm() ->
    NTestItems = ?ITEMS,   %% How many test items to write/verify?
    NTestNodes = 4,      %% How many nodes to spin up for tests?
    lager:info("Spinning up test nodes"),
    Config = [{riak_core, [{ring_creation_size, 8}]},
                {riak_kv, [{anti_entropy, {off, []}}]},
                {bitcask, [{max_file_size, 1000000},
                {leveled, [{journal_objectcount, 5000}]}]}],
    
    [RootNode | TestNodes] = rt:build_cluster(NTestNodes, Config),
    [FailNode | _RestNodes] = TestNodes,
    rt:wait_for_service(RootNode, riak_kv),
    rt:wait_for_service(FailNode, riak_kv),
    Path = filename:join([rtdev:relpath(current),
                          "dev",
                          "dev"++ integer_to_list(rtdev:node_id(FailNode)),
                          "data"]),

    % When running the test we cannot let it crash on failure - as then the
    % write permissions will not be restored (and any subsequent test in a test
    % run will fail)
    R = 
        try run_test(NTestItems, RootNode, FailNode) of
            pass -> pass
        catch
            _:Error -> 
                lager:error("Test failure as caught error ~w",  [Error]),
                lager:error("Failure has trace ~p", [erlang:get_stacktrace()]),
                error
        end,
    
    S = io_lib:format("chmod -R u+w ~p",[Path]),
    lager:info("Restoring write permission to path via ~s", [S]),
    os:cmd(S),
    
    R.


run_test(NTestItems, RootNode, FailNode) ->
    lager:info("Populating cluster with writes."),
    [] = rt:systest_write(RootNode, 1, NTestItems, ?BUCKET, 2),
    lager:info("Write complete - removing write permisions on data path"),
    %% write one object with a bucket type

    Path = filename:join([rtdev:relpath(current),
                          "dev",
                          "dev"++ integer_to_list(rtdev:node_id(FailNode)),
                          "data"]),
    S = io_lib:format("chmod -R u-w ~p",[Path]),
    lager:info("Running command ~s with output ~w", [S, os:cmd(S)]),
    
    pong = net_adm:ping(FailNode),

    WriteAttempts = NTestItems * (?POST_MULT - 1),
    lager:info("Beginning large set of new writes ~w", [WriteAttempts]),
    PostErrors =
        rt:systest_write(RootNode,
                            NTestItems, NTestItems * ?POST_MULT, ?BUCKET,
                            2),
    lager:info("Write complete - validating"),
    
    lager:info("Errors on write count of ~w out of ~w",
                [length(PostErrors), WriteAttempts]),
    true = length(PostErrors) < (WriteAttempts div 10000),
    lager:info("Less than 0.01% of writes errored due to failure"),
    
    lager:info("Confirm the node did crash as it couldn't write"),
    pang = net_adm:ping(FailNode),

    pass.
