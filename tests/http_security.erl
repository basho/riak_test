-module(http_security).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    ok = application:start(crypto),
    ok = application:start(asn1),
    ok = application:start(public_key),
    ok = application:start(ssl),
    lager:info("Deploy some nodes"),
    PrivDir = rt:priv_dir(),
    Conf = [
            {riak_core, [
                    {ssl, [
                            {certfile, filename:join([PrivDir,
                                                      "certs/selfsigned/site3-cert.pem"])},
                            {keyfile, filename:join([PrivDir,
                                                     "certs/selfsigned/site3-key.pem"])}
                            ]},
                    {security, true}
                    ]}
    ],
    Nodes = rt:build_cluster(4, Conf),
    Node = hd(Nodes),
    [enable_ssl(N) || N <- Nodes],
    {ok, [{"127.0.0.1", Port0}]} = rpc:call(Node, application, get_env,
                                 [riak_core, http]),
    {ok, [{"127.0.0.1", Port}]} = rpc:call(Node, application, get_env,
                                 [riak_core, https]),

    %% connections over regular HTTP get told to go elsewhere
    C0 = rhc:create("127.0.0.1", Port0, "riak", []),
    ?assertMatch({error, {ok, "426", _, _}}, rhc:ping(C0)),

    C1 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C1)),

    C2 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "pass"}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C2)),

    %% grant the user credentials
    ok = rpc:call(Node, riak_core_console, add_user, [["user", "password=password"]]),

    %% trust anyone on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user",
                                                         "127.0.0.1/32",
                                                         "trust"]]),

    %% invalid credentials should be ignored in trust mode
    C3 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "pass"}]),
    ?assertEqual(ok, rhc:ping(C3)),

    %% require password on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user",
                                                         "127.0.0.1/32",
                                                         "password"]]),

    %% invalid credentials should be rejected in password mode
    C4 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "pass"}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C4)),

    %% valid credentials should be accepted in password mode
    C5 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "password"}]),
    ?assertEqual(ok, rhc:ping(C5)),

    ok.

enable_ssl(Node) ->
    {ok, [{"127.0.0.1", Port}]} = rpc:call(Node, application, get_env,
                                 [riak_core, http]),
    rt:update_app_config(Node, [{riak_core, [{https, [{"127.0.0.1",
                                                     Port+1000}]}]}]),
    rt:wait_until_pingable(Node),
    rt:wait_for_service(Node, riak_kv).



