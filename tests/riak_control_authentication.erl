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
%%
%% @doc Verify authentication works for riak_control.

-module(riak_control_authentication).

-behaviour(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(RC_AUTH_NONE_CONFIG, 
        [{riak_control, [{enabled, true}, 
                         {auth, none}]}]).

-define(RC_AUTH_NONE_CONFIG_FORCE_SSL, 
        [{riak_api, [{https, [{"127.0.0.1", 8069}]}]},
         {riak_core,
          [{https, [{"127.0.0.1", 8069}]},
           {ssl,
            [{certfile, "./etc/cert.pem"},
             {keyfile, "./etc/key.pem"}
            ]}]},
         {riak_control, [{enabled, true}, 
                         {auth, none}, 
                         {force_ssl, true}]}]).

-define(RC_AUTH_USERLIST_CONFIG, 
        [{riak_api, [{https, [{"127.0.0.1", 8069}]}]},
         {riak_core,
          [{https, [{"127.0.0.1", 8069}]},
           {ssl,
            [{certfile, "./etc/cert.pem"},
             {keyfile, "./etc/key.pem"}
            ]}]},
         {riak_control, [{enabled, true}, 
                         {auth, userlist},
                         {userlist, [{"user", "pass"}]}]}]).

-define(RC_AUTH_USERLIST_CONFIG_FORCE_SSL, 
        [{riak_api, [{https, [{"127.0.0.1", 8069}]}]},
         {riak_core,
          [{https, [{"127.0.0.1", 8069}]},
           {ssl,
            [{certfile, "./etc/cert.pem"},
             {keyfile, "./etc/key.pem"}
            ]}]},
         {riak_control, [{enabled, true}, 
                         {force_ssl, true},
                         {auth, userlist},
                         {userlist, [{"user", "pass"}]}]}]).

-define(RC_AUTH_USERLIST_CONFIG_NO_FORCE_SSL, 
        [{riak_api, [{https, [{"127.0.0.1", 8069}]}]},
         {riak_core,
          [{https, [{"127.0.0.1", 8069}]},
           {ssl,
            [{certfile, "./etc/cert.pem"},
             {keyfile, "./etc/key.pem"}
            ]}]},
         {riak_control, [{enabled, true}, 
                         {force_ssl, false},
                         {auth, userlist},
                         {userlist, [{"user", "pass"}]}]}]).

%% @doc Confirm all authentication methods work for the three supported
%%      releases.
confirm() ->
    %% Verify authentication method 'none'.
    verify_authentication(legacy,   ?RC_AUTH_NONE_CONFIG),
    verify_authentication(previous, ?RC_AUTH_NONE_CONFIG),

    %% Verify authentication method 'userlist'.
    verify_authentication(legacy,   ?RC_AUTH_USERLIST_CONFIG),
    verify_authentication(previous, ?RC_AUTH_USERLIST_CONFIG),

    %% Verify authentication none, and then with forced SSL.
    verify_authentication(current,  ?RC_AUTH_NONE_CONFIG),
    verify_authentication(current,  ?RC_AUTH_NONE_CONFIG_FORCE_SSL),

    %% Verify authentication userlist, without SSL and then with SSL.
    verify_authentication(current,  ?RC_AUTH_USERLIST_CONFIG_FORCE_SSL),
    verify_authentication(current,  ?RC_AUTH_USERLIST_CONFIG_NO_FORCE_SSL).

%% @doc Verify the disabled authentication method works.
verify_authentication(Vsn, ?RC_AUTH_NONE_CONFIG) ->
    lager:info("Verifying auth 'none', ~p.", [Vsn]),
    Nodes =   build_singleton_cluster(Vsn, ?RC_AUTH_NONE_CONFIG),
    Node =    lists:nth(1, Nodes),

    %% Assert that we can load the main page.
    lager:info("Verifying Control loads."),
    Command = io_lib:format("curl -sL -w %{http_code} ~s~p -o /dev/null", 
                            [rt:http_url(Node), "/admin"]),
    ?assertEqual("200", os:cmd(Command)),

    pass;
%% @doc Verify the disabled authentication method works with force SSL.
verify_authentication(current, ?RC_AUTH_NONE_CONFIG_FORCE_SSL) ->
    lager:info("Verifying auth 'none', 'force_ssl' 'true', current."),
    Nodes =   build_singleton_cluster(current, 
                                      ?RC_AUTH_NONE_CONFIG_FORCE_SSL),
    Node =    lists:nth(1, Nodes),

    %% Assert that we get redirected if we hit the HTTP port.
    lager:info("Verifying redirect to SSL."),
    RedirectCommand = io_lib:format("curl -sL -w %{http_code} ~s~p -o /dev/null", 
                                    [rt:http_url(Node), "/admin"]),
    ?assertEqual("303", os:cmd(RedirectCommand)),

    %% TODO: Temporarily disabled because of OTP R16B02 SSL bug.
    %% Assert that we can access resource over the SSL port.
    % lager:info("Verifying Control loads over SSL."),
    % AccessCommand = io_lib:format("curl --insecure -sL -w %{http_code} ~s~p", 
    %                               [rt:https_url(Node), "/admin"]),
    % ?assertEqual("200", os:cmd(AccessCommand)),

    pass;
%% @doc Verify the userlist authentication method works.
verify_authentication(Vsn, ?RC_AUTH_USERLIST_CONFIG) ->
    lager:info("Verifying auth 'userlist', ~p.", [Vsn]),
    Nodes =   build_singleton_cluster(Vsn, ?RC_AUTH_USERLIST_CONFIG),
    Node =    lists:nth(1, Nodes),

    %% Assert that we get redirected if we hit the HTTP port.
    lager:info("Verifying redirect to SSL."),
    RedirectCommand = io_lib:format("curl -sL -w %{http_code} ~s~p -o /dev/null", 
                                    [rt:http_url(Node), "/admin"]),
    ?assertEqual("303", os:cmd(RedirectCommand)),

    %% Assert that we can access resource over the SSL port.
    lager:info("Verifying Control loads over SSL."),
    AccessCommand = io_lib:format("curl --insecure -sL -w %{http_code} ~s~p -o /dev/null", 
                                  [rt:https_url(Node), "/admin"]),
    ?assertEqual("401", os:cmd(AccessCommand)),

    %% Assert that we can access resource over the SSL port.
    lager:info("Verifying Control loads with credentials."),
    AuthCommand = io_lib:format("curl -u user:pass --insecure -sL -w %{http_code} ~s~p -o /dev/null", 
                                [rt:https_url(Node), "/admin"]),
    ?assertEqual("200", os:cmd(AuthCommand)),

    pass;
%% @doc Verify the userlist authentication method works.
verify_authentication(current, ?RC_AUTH_USERLIST_CONFIG_FORCE_SSL) ->
    lager:info("Verifying auth 'userlist', 'force_ssl' 'true', current."),
    Nodes =   build_singleton_cluster(current, ?RC_AUTH_USERLIST_CONFIG_FORCE_SSL),
    Node =    lists:nth(1, Nodes),

    %% Assert that we get redirected if we hit the HTTP port.
    lager:info("Verifying redirect to SSL."),
    RedirectCommand = io_lib:format("curl -sL -w %{http_code} ~s~p -o /dev/null", 
                                    [rt:http_url(Node), "/admin"]),
    ?assertEqual("303", os:cmd(RedirectCommand)),

    %% TODO: Temporarily disabled because of OTP R16B02 SSL bug.
    %% Assert that we can access resource over the SSL port.
    % lager:info("Verifying Control loads over SSL."),
    % AccessCommand = io_lib:format("curl --insecure -sL -w %{http_code} ~s~p -o /dev/null", 
    %                               [rt:https_url(Node), "/admin"]),
    % ?assertEqual("401", os:cmd(AccessCommand)),

    %% TODO: Temporarily disabled because of OTP R16B02 SSL bug.
    %% Assert that we can access resource over the SSL port.
    % lager:info("Verifying Control loads with credentials."),
    % AuthCommand = io_lib:format("curl -u user:pass --insecure -sL -w %{http_code} ~s~p -o /dev/null", 
    %                             [rt:https_url(Node), "/admin"]),
    % ?assertEqual("200", os:cmd(AuthCommand)),

    pass;
%% @doc Verify the userlist authentication method works.
verify_authentication(current, ?RC_AUTH_USERLIST_CONFIG_NO_FORCE_SSL) ->
    lager:info("Verifying auth 'userlist', 'force_ssl' 'false', current."),
    Nodes =   build_singleton_cluster(current, ?RC_AUTH_USERLIST_CONFIG_NO_FORCE_SSL),
    Node =    lists:nth(1, Nodes),

    %% Assert that we can access resource over the SSL port.
    lager:info("Verifying Control loads over SSL."),
    AccessCommand = io_lib:format("curl --insecure -sL -w %{http_code} ~s~p -o /dev/null", 
                                  [rt:http_url(Node), "/admin"]),
    ?assertEqual("401", os:cmd(AccessCommand)),

    %% Assert that we can access resource over the SSL port.
    lager:info("Verifying Control loads with credentials."),
    AuthCommand = io_lib:format("curl -u user:pass --insecure -sL -w %{http_code} ~s~p -o /dev/null",
                                [rt:http_url(Node), "/admin"]),
    ?assertEqual("200", os:cmd(AuthCommand)),

    pass.

%% @doc Build a one node cluster.
build_singleton_cluster(Vsn, Config) ->
    [Nodes] = rt_cluster:build_clusters([{1, Vsn, Config}]),

    %% Start and stop, wait for riak_kv.
    %%
    %% Since many of the Riak Control configuration options change how
    %% the supervisor starts, we need to restart to ensure settings
    %% take effect.
    Node = lists:nth(1, Nodes),
    rt_node:stop_and_wait(Node),
    rt_node:start_and_wait(Node),
    rt:wait_for_service(Node, riak_kv),

    %% Wait for control to start.
    VersionedNodes = [{Vsn, N} || N <- Nodes],
    rt:wait_for_control(VersionedNodes),

    lager:info("Build ~p, nodes: ~p.", [Vsn, Nodes]),
    Nodes.
