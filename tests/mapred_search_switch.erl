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
%% @doc Verify that "search" MapReduce inputs come from Yokozuna or
%% Riak Search, as the Riak node is configured.
%%
%% Without the switch code in place in Riak, this test's failure is
%% confusing: it will fail saying that riak_search was enabled, and it
%% got riak_search mapred results, but it expected not to. This is
%% because the first test also enables yokozuna without explicitly
%% selecting one or the other. The test expects Riak to return an
%% error, but without the switch code in place, Riak happily returns
%% what it has always returned: riak_search mapred results.
-module(mapred_search_switch).
-behavior(riak_test).
-export([
         %% riak_test api
         confirm/0
        ]).
-compile([export_all]). %% because we run tests as ?MODULE:T(Nodes)
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

%% name of the riak_kv appenv specifying which search provider to use
-define(PROVIDER_KEY, mapred_search).

-record(env, {
          nodes,     % nodes running
          rs_bucket, % bucket configured for riak_search
          rs_keyuqs, % keys and their unique data for riak_search
          rs_common, % common data for all keys for riak_search
          yz_bucket, % \
          yz_keyuqs, % +- ditto, yokozuna bucket 1
          yz_common, % /
          yz_index   % YZ index separate from bucket name
         }).

confirm() ->
    Env = setup_test_env(),

    [ confirm_config(Env,
                     [{riak_search, [{enabled, RS}]},
                      {yokozuna, [{enabled, YZ}]},
                      {riak_kv, [{?PROVIDER_KEY, P}]}])
      || RS <- [true, false],
         YZ <- [true, false],
         P <- [riak_search, yokozuna, undefined] ],
    pass.

setup_test_env() ->
    %% must enable both RS and YZ at startup to get test data indexed;
    %% nothing extra would be tested by using multiple nodes, so just
    %% deploy one to make the test run faster
    Nodes = rt:deploy_nodes(1, [{riak_search, [{enabled, true}]},
                                {yokozuna, [{enabled, true}]}]),
    ok = rt:wait_until_nodes_ready(Nodes),
    ok = rt:wait_for_cluster_service(Nodes, riak_search),
    ok = rt:wait_for_cluster_service(Nodes, yokozuna),

    {RSBucket, RSKeyAndUniques, RSCommon} = generate_test_data(<<"rs">>),
    setup_rs_bucket(Nodes, RSBucket),
    load_test_data(Nodes, RSBucket, RSKeyAndUniques, RSCommon),

    {YZBucket, YZKeyAndUniques, YZCommon} = generate_test_data(<<"yz">>),
    YZIndex = generate_string(),
    lager:info("yz index: ~s", [YZIndex]),

    setup_yz_index(Nodes, YZIndex),
    setup_yz_bucket(Nodes, YZBucket, YZIndex),
    load_test_data(Nodes, YZBucket, YZKeyAndUniques, YZCommon),

    %% give yokozuna time to auto-commit
    YZSleep_ms = 1000,
    lager:info("Giving Yokozuna ~bms to auto-commit", [YZSleep_ms]),
    timer:sleep(YZSleep_ms),

    #env{ nodes=Nodes,
          rs_bucket=RSBucket,
          rs_keyuqs=RSKeyAndUniques,
          rs_common=RSCommon,
          yz_bucket=YZBucket,
          yz_keyuqs=YZKeyAndUniques,
          yz_common=YZCommon,
          yz_index=YZIndex }.

set_config(#env{nodes=Nodes}, Config) ->
    [ [ set_config(Nodes, App, K, V)
        || {K, V} <- Props ]
      || {App, Props} <- Config ].

set_config(Nodes, App, K, V) ->
    ?assertEqual(
       {lists:duplicate(length(Nodes), ok), []},
       case V of
           undefined ->
               rpc:multicall(Nodes, application, unset_env, [App, K]);
           _ ->
               rpc:multicall(Nodes, application, set_env, [App, K, V])
        end).

confirm_config(#env{nodes=Nodes,
                    rs_bucket=RSBucket,
                    rs_keyuqs=RSKeyAndUniques,
                    rs_common=RSCommon,
                    yz_bucket=YZBucket,
                    yz_keyuqs=YZKeyAndUniques,
                    yz_index=YZIndex}=Env,
                    Config) ->
    lager:info("Running Config: ~p", [Config]),
    set_config(Env, Config),

    RSBResults = run_bucket_mr(Nodes, RSBucket, RSCommon),
    YZBResults = run_bucket_mr(Nodes, YZIndex, <<"*:*">>),

    lager:info("RS Bucket Results: ~p", [RSBResults]),
    lager:info("YZ Bucket Results: ~p", [YZBResults]),

    ?assertEqual(expected_riak_search(Config),
                 got_riak_search(RSBResults, RSBucket,
                                 RSKeyAndUniques)),
    ?assertEqual(expected_yokozuna(Config),
                 got_yokozuna(YZBResults, YZBucket, YZKeyAndUniques)),
    %% asking YZ to MR a bucket it hasn't indexed results in error
    ?assertEqual(expected_yokozuna(Config) or expected_error(Config),
                 got_error(RSBResults)),
    ?assertEqual(expected_error(Config),
                 got_error(YZBResults)).

%% make up random test data, to fight against accidental re-runs, and
%% put it in the test log so we know where to poke when things fail
generate_test_data(System) ->
    Bucket = generate_bucket_name(System),
    lager:info("~s bucket: ~s", [System, Bucket]),

    Common = generate_string(),
    lager:info("~s common: ~s", [System, Common]),

    KeyAndUniques = [ {generate_string(), generate_string()},
                      {generate_string(), generate_string()},
                      {generate_string(), generate_string()} ],
    [ lager:info("~s key/uq: ~s / ~s", [System, Key, Unique])
      || {Key, Unique} <- KeyAndUniques ],

    {Bucket, KeyAndUniques, Common}.

%% setup riak_search hook
setup_rs_bucket([Node|_], Bucket) ->
    lager:info("Setting up riak_search hook"),
    C = rt:httpc(Node),
    ok = rhc:set_bucket(C, Bucket, [{search, true}]).

%% setup yokozuna hook/index - bucket name == index name
setup_yz_bucket([Node|_], Bucket, Index) ->
    %% attach bucket to index
    %% TODO: teach rhc_bucket:httpify_prop/2 `search_index'
    BUrl = iburl(Node, ["/buckets/",Bucket,"/props"]),
    BHeaders = [{"content-type", "application/json"}],
    BProps = mochijson2:encode([{props, {struct, [{search_index, Index}]}}]),
    {ok, "204", _, _} = ibrowse:send_req(BUrl, BHeaders, put, BProps).

setup_yz_index([Node|_]=Cluster, Index) ->
    %% create index
    IUrl = iburl(Node, index_path(Index)),
    {ok, "204", _, _} = ibrowse:send_req(IUrl, [], put),
    wait_for_index(Cluster, Index).

index_path(Index) ->
    ["/search/index/",Index].

%% if we start writing data too soon, it won't be indexed, so wait
%% until solr has created the index
wait_for_index(Cluster, Index) ->
    IsIndexUp =
        fun(Node) ->
                lager:info("Waiting for index ~s on node ~p", [Index, Node]),
                IUrl = iburl(Node, index_path(Index)),
                case ibrowse:send_req(IUrl, [], get) of
                    {ok, "200", _, _} -> true;
                    _ -> false
                end
        end,
    [?assertEqual(ok, rt:wait_until(Node, IsIndexUp)) || Node <- Cluster].

%% ibrowse really wants a list of characters, not a binary, not an iolist
iburl(Node, Path) ->
    binary_to_list(list_to_binary([rt:http_url(Node), Path])).

%% Create a set of keys, all of which have a common term in their
%% value, and each of which has a unique term in its value
load_test_data([Node|_], Bucket, KeyAndUniques, Common) ->
    lager:info("Loading test data"),
    C = rt:httpc(Node),
    [ begin
          Value = list_to_binary([Common, " ", Unique]),
          ok = rhc:put(C, riakc_obj:new(Bucket, Key, Value, "text/plain"))
      end
      || {Key, Unique} <- KeyAndUniques ].

expected_riak_search(Config) ->
    is_enabled(Config, riak_search)
        %% if yokozuna is also enabled, riak_search is the default
        and ( (not is_enabled(Config, yokozuna))
              or (yokozuna =/= provider(Config)) ).

expected_yokozuna(Config) ->
    is_enabled(Config, yokozuna)
        %% if riak_search is also enabled, must explicitly pick yokozuna
        and ( (not is_enabled(Config, riak_search))
              or (yokozuna == provider(Config)) ).

expected_error(Config) ->
    %% must have at least one system on to get results
    not ( is_enabled(Config, yokozuna)
          or is_enabled(Config, riak_search) ).

is_enabled(Config, App) ->
    true == kvc:path([App, enabled], Config).

provider(Config) ->
    case kvc:path([riak_kv, ?PROVIDER_KEY], Config) of
        [] -> undefined;
        Provider -> Provider
    end.

%% We only check that we got at least one match, and that any matches
%% we did get are expected, and of the RS format. We don't check that
%% all known keys are present in the result because we're not
%% interested in whether RS is working with full correctness.
got_riak_search(Results, Bucket, KeyAndUniques) ->
    case Results of
        {ok, [{0, Matches}]} when Matches /= [] ->
            IsRS = fun({{B, K},{struct,Props}}) when B == Bucket ->
                           lists:keymember(K, 1, KeyAndUniques) and
                               lists:keymember(p, 1, Props) and
                               lists:keymember(score, 1, Props);
                      (_) ->
                           false
                   end,
            lists:all(IsRS, Matches);
        _ ->
            false
    end.

%% similar to got_riak_search - just check that we got at least one
%% result, and that all results are in the expected YZ format - this
%% test doesn't care if YZ is fulfilling its harvest/yield promises
got_yokozuna(Results, Bucket, KeyAndUniques) ->
    case Results of
        {ok, [{0, Matches}]} when Matches /= [] ->
            IsYZ = fun({{{<<"default">>, B}, K}, {struct, []}}) when B == Bucket ->
                           lists:keymember(K, 1, KeyAndUniques);
                      (_) ->
                           false
                   end,
            lager:info("got_yokozuna: ~p ... ~p", [Matches, KeyAndUniques]),
            lists:all(IsYZ, Matches);
        _ ->
            false
    end.

%% we don't care what the error is right now, just that it is an error
%% TODO: actually get a good error message bubbled in these cases
got_error({error, _}) ->
    true;
got_error(_) ->
    false.

run_bucket_mr([Node|_], Bucket, Common) ->
    C = rt:pbc(Node),
    riakc_pb_socket:mapred(
      C,
      %% TODO: check {search, Bucket, Common, Filter}
      %% independently
      {search, Bucket, Common},
      []).

%% Prefix is included to make it easy to tell what was set up for
%% riak_search vs. yokozuna
generate_bucket_name(Prefix) ->
    list_to_binary([Prefix,generate_string()]).

generate_string() ->
    %% stolen from riak_core_util:unique_id_62/0, but using 36 instead
    %% so as not to have to copy riak_core_util:integer_to_list
    Rand = crypto:hash(sha, term_to_binary({make_ref(), os:timestamp()})),
    <<I:160/integer>> = Rand,
    list_to_binary(integer_to_list(I, 36)).
