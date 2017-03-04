%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.
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

-module(s3_lifecycle_hooks).
-include_lib("eunit/include/eunit.hrl").

%%
%% This test uses some standard Riak APIs to create what the S3 web facade
%% would otherwise create via its REST APIs, specifically, a Riak
%%

-export([confirm/0]).

-define(BUCKET_TYPE, <<"s3_lifecycle_hooks">>).

-define(WEBHOOK_PATH, "/lifecycle_hook").
-define(WEBHOOK_PORT, 8765).
-define(WEBHOOK_URL, "http://localhost:" ++ integer_to_list(?WEBHOOK_PORT) ++ ?WEBHOOK_PATH).

-define(CONFIG, [
    {riak_core, [
        {ring_creation_size, 8},
        {handoff_concurrency, 10},
        {vnode_management_timer, 1000}
    ]},
    {riak_kv, [
        {sweep_tick, 1000}
    ]},
    {riak_s3_api, [
        {lifecycle_hook_url, ?WEBHOOK_URL},
        {lifecycle_sweep_interval, 1},
        %% Make objects "expire" instantly, as soon as a sweep happens:
        {debug_lifecycle_expiration_bypass, true}
    ]}
]).
-define(NUM_NODES, 1).


confirm() ->
    %%
    %% Start our webhook
    %%
    lager:info("Starting mock lifecycle webhook server..."),
    ok = webhook_server:start_link(?WEBHOOK_PORT),
    %%
    %% Build the cluster
    %%
    Cluster = rt:build_cluster(?NUM_NODES, ?CONFIG),
    Node = lists:nth(random:uniform(length((Cluster))), Cluster),
    rt:wait_for_service(Node, [riak_kv]),
    %%
    %% Create a mock S3 "bucket" (aka Riak bucket type) with
    %%
    rt:create_and_activate_bucket_type(Node, ?BUCKET_TYPE, [{riak_s3_lifecycle, [{'standard_ia.days', 1}]}]),
    rt:wait_until_bucket_type_visible(Cluster, ?BUCKET_TYPE),

    Bucket = rpc:call(Node, riak_s3_bucket, to_riak_bucket, [?BUCKET_TYPE]),
    lager:info("Bucket: ~p", [Bucket]),


    %% Populate an S3 "object" TODO refactor as needed -- maybe lots of puts?
    %%
    lager:info("Writing an object to the S3 bucket..."),
    Client = rt:pbc(Node),
    _Ret = riakc_pb_socket:put(
        Client, riakc_obj:new(
            Bucket, <<"test_key">>, <<"test_value">>
        )
    ),

    %% TODO Force a sweep of our object's partition:

    %%
    %% confirm webhook has been called
    %%
    lager:info("Waiting for lifecycle webhook to be executed..."),
    ?assertEqual(ok, rt:wait_until(fun check_for_webhook_request/0)),

    pass.

check_for_webhook_request() ->
    case webhook_server:get_next() of
        empty ->
            lager:info("No entry received yet."),
            false;
        {got_http_req, Req, Body} ->
            verify_webhook_request_parameters(Req),
            verify_webhook_request_body(Body),
            true
    end.

verify_webhook_request_parameters(Req) ->
    ?assertEqual('PUT', Req:get(method)),
    ?assertEqual(?WEBHOOK_PATH, Req:get(path)).

verify_webhook_request_body(BodyBin) ->
    Body = mochijson2:decode(BodyBin),
    lager:info("Got body ~p", [Body]),
    true.
