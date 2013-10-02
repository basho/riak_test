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

    % Set up special logs capturing module
    rt:load_modules_on_nodes([riak_test_lager_backend], [Node1]),
    ok = rpc:call(Node1, gen_event, add_handler, [lager_event,
                                                  riak_test_lager_backend,
                                                  [info, false]]),
    ok = rpc:call(Node1, lager, set_loglevel, [riak_test_lager_backend, info]),

    % For the sibling test, we need the bucket to allow siblings
    ?assertMatch(ok, riakc_pb_socket:set_bucket(C, ?BUCKET,
                                                [{allow_mult, true}])),
    verify_size_thresholds(C, Node1),
    verify_sibling_thresholds(C, Node1),
    pass.

verify_size_thresholds(C, Node1) ->
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
    Res = verify_log(Node, Pattern),
    ?assertEqual({warning, N, true}, {warning, N, Res}).

verify_size_read_warning(Node, K, N) ->
    lager:info("Looking for read warning for size ~p", [N]),
    Pattern = io_lib:format("warning.*Read.*~p.*~p",[?BUCKET, K]),
    Res = verify_log(Node, Pattern),
    ?assertEqual({warning, N, true}, {warning, N, Res}).

verify_size_write_error(Node, K, N) ->
    lager:info("Looking for write error for size ~p", [N]),
    Pattern = io_lib:format("error.*~p.*~p",[?BUCKET, K]),
    Res = verify_log(Node, Pattern),
    ?assertEqual({warning, N, true}, {warning, N, Res}).

verify_sibling_thresholds(C, Node1) ->
    K = <<"sibtest">>,
    O = riakc_obj:new(?BUCKET, K, <<"val">>),
    [?assertMatch(ok, riakc_pb_socket:put(C, O)) 
     || _ <- lists:seq(1, ?WARN_SIBLINGS+1)],
    P = io_lib:format("warning.*siblings.*~p.*~p.*(~p)",
                      [?BUCKET, K, ?WARN_SIBLINGS+1]),
    Found = verify_log(Node1, P),
    lager:info("Looking for sibling warning: ~p", [Found]),
    ?assertEqual(true, Found),
    % Generate error now
    [?assertMatch(ok, riakc_pb_socket:put(C, O)) 
     || _ <- lists:seq(?WARN_SIBLINGS+2, ?MAX_SIBLINGS)],
    Res = riakc_pb_socket:put(C, O),
    lager:info("Result when too many siblings : ~p", [Res]),
    ?assertMatch({error,_},  Res),
    ok.

verify_log(Node, Pattern) ->
    CheckLogFun = fun() ->
            Logs = rpc:call(Node, riak_test_lager_backend, get_logs, []),
            lager:info("Logs ~s", [Logs]),
            lager:info("looking for pattern ~s", [Pattern]),
            case re:run(Logs, Pattern, []) of
                {match, _} ->
                    lager:info("Found match"),
                    true;
                nomatch    ->
                    lager:info("No match"),
                    false
            end
    end,
    case rt:wait_until(CheckLogFun) of
        ok ->
            true;
        _ ->
            false
    end.
