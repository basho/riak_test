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

%% @doc Verifies Riak's warnings and caps for number of siblings
%%  and object size. Warnings end up in the logs, and hard caps can
%%  make requests fail.
-module(verify_object_limits).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"b">>).
-define(WARN_SIZE, 1000).
-define(MAX_SIZE, 10000).
-define(WARN_SIBLINGS,2).
-define(MAX_SIBLINGS,5).


confirm() ->
    [Node1] = rt:build_cluster(1, [{riak_kv, [
                        {ring_creation_size, 8},
                        {max_object_size, ?MAX_SIZE},
                        {warn_object_size, ?WARN_SIZE},
                        {max_siblings, ?MAX_SIBLINGS},
                        {warn_siblings, ?WARN_SIBLINGS}]}]),
    C = rt:pbc(Node1),

    %% Set up to grep logs to verify messages
    rt:setup_log_capture(Node1),

    % For the sibling test, we need the bucket to allow siblings
    lager:info("Configuring bucket to allow siblings"),
    ?assertMatch(ok, riakc_pb_socket:set_bucket(C, ?BUCKET,
                                                [{allow_mult, true}])),
    verify_size_limits(C, Node1),
    verify_sibling_limits(C, Node1),
    pass.

verify_size_limits(C, Node1) ->
    lager:info("Verifying size limits"),
    Puts = [{1, ok},
            {10, ok},
            {50, ok},
            {?WARN_SIZE, warning},
            {?MAX_SIZE, error},
            {?MAX_SIZE*2, error}],
    [begin
            lager:info("Checking put of size ~p, expected ~p", [N, X]),
            K = <<N:32/big-integer>>,
            V = <<0:(N)/integer-unit:8>>, % N zeroes bin
            O = riakc_obj:new(?BUCKET, K, V),
            % Verify behavior on write
            Res = riakc_pb_socket:put(C, O),
            lager:info("Result : ~p", [Res]),
            case X of
                ok ->
                    ?assertMatch({N, ok}, {N, Res});
                error ->
                    ?assertMatch({N, {error, _}}, {N, Res}),
                    verify_size_write_error(Node1, K, N);
                warning ->
                    verify_size_write_warning(Node1, K, N)
            end,
            % Now verify on read
            lager:info("Now checking read of size ~p, expected ~p", [N, X]),
            ReadRes = riakc_pb_socket:get(C, ?BUCKET, K),
            case X of
                ok ->
                    ?assertMatch({{ok, _}, N}, {ReadRes, N});
                warning ->
                    ?assertMatch({{ok, _}, N}, {ReadRes, N}),
                    verify_size_read_warning(Node1, K, N);
                error ->
                    ?assertMatch({{error, _}, N}, {ReadRes, N})
            end
        end || {N, X} <- Puts],
    ok.

verify_size_write_warning(Node, K, N) ->
    lager:info("Looking for write warning for size ~p", [N]),
    Pattern = io_lib:format("warning.*Writ.*~p.*~p",[?BUCKET, K]),
    Res = rt:expect_in_log(Node, Pattern),
    ?assertEqual({warning, N, true}, {warning, N, Res}).

verify_size_read_warning(Node, K, N) ->
    lager:info("Looking for read warning for size ~p", [N]),
    Pattern = io_lib:format("warning.*Read.*~p.*~p",[?BUCKET, K]),
    Res = rt:expect_in_log(Node, Pattern),
    ?assertEqual({warning, N, true}, {warning, N, Res}).

verify_size_write_error(Node, K, N) ->
    lager:info("Looking for write error for size ~p", [N]),
    Pattern = io_lib:format("error.*~p.*~p",[?BUCKET, K]),
    Res = rt:expect_in_log(Node, Pattern),
    ?assertEqual({warning, N, true}, {warning, N, Res}).

verify_sibling_limits(C, Node1) ->
    K = <<"sibtest">>,
    O = riakc_obj:new(?BUCKET, K, <<"val">>),
    [?assertMatch(ok, riakc_pb_socket:put(C, O)) 
     || _ <- lists:seq(1, ?WARN_SIBLINGS+1)],
    P = io_lib:format("warning.*siblings.*~p.*~p.*(~p)",
                      [?BUCKET, K, ?WARN_SIBLINGS+1]),
    Found = rt:expect_in_log(Node1, P),
    lager:info("Looking for sibling warning: ~p", [Found]),
    ?assertEqual(true, Found),
    % Generate error now
    [?assertMatch(ok, riakc_pb_socket:put(C, O)) 
     || _ <- lists:seq(?WARN_SIBLINGS+2, ?MAX_SIBLINGS)],
    Res = riakc_pb_socket:put(C, O),
    lager:info("Result when too many siblings : ~p", [Res]),
    ?assertMatch({error,_},  Res),
    ok.
