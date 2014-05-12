-module(client_java_verify).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-prereq("java").
-prereq("mvn").
-prereq("curl").

confirm() ->

    lager:info("+P ~p", [erlang:system_info(process_limit)]),
    prereqs(),
    Nodes = rt:deploy_nodes(1),
    [Node1] = Nodes,
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),

    rpc:call(Node1, application, set_env, [erlang_js, script_timeout, 10000]),
    [{Node1, ConnectionInfo}] = rt:connection_info([Node1]),
    {HTTP_Host, HTTP_Port} = orddict:fetch(http, ConnectionInfo),
    {PB_Host, PB_Port} = orddict:fetch(pb, ConnectionInfo),

    lager:info("Connection Info: http: ~p:~p pb: ~p:~p", [HTTP_Host, HTTP_Port, PB_Host, PB_Port]),

    java_unit_tests(HTTP_Host, HTTP_Port, PB_Host, PB_Port),
    pass.

prereqs() ->
    ok.

java_unit_tests(HTTP_Host, HTTP_Port, _PB_Host, PB_Port) ->
    lager:info("Run the Java unit tests from somewhere on the local machine."),
    %% run the following:
    Cmd = io_lib:format(
            "mvn -f deps/riak_java_client/pom.xml -Pitest clean compile verify "
            "-DargLine=\"-Dcom.basho.riak.host=~s -Dcom.basho.riak.http.port=~p"
            " -Dcom.basho.riak.pbc.port=~p\"",
            [HTTP_Host, HTTP_Port, PB_Port]),
    lager:info("Cmd: ~s", [Cmd]),

    {ExitCode, JavaLog} = rt_local:stream_cmd(Cmd, []),
    ?assertEqual(0, ExitCode),
    lager:info(JavaLog),
    ?assertNot(rt:str(JavaLog, "FAILURES!!!")),
    ok.
