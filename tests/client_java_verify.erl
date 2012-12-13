-module(client_java_verify).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% Change when a new release comes out.
-define(JAVA_FAT_BE_URL, rt:config(java.fat_be_url)).
-define(JAVA_FAT_FILENAME, lists:last(string:tokens(?JAVA_FAT_BE_URL, "/"))).
-define(JAVA_TESTS_URL, rt:config(java.tests_url)).
-define(JAVA_TESTS_FILENAME, lists:last(string:tokens(?JAVA_TESTS_URL, "/"))).

-prereq("java").
-prereq("curl").

confirm() ->
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
    %% Does you have the java client available?
    you_got_jars(?JAVA_FAT_BE_URL, ?JAVA_FAT_FILENAME),
    you_got_jars(?JAVA_TESTS_URL, ?JAVA_TESTS_FILENAME),
    ok.

java_unit_tests(HTTP_Host, HTTP_Port, _PB_Host, PB_Port) ->
    lager:info("Run the Java unit tests from somewhere on the local machine."),

    %% run the following:
    Cmd = io_lib:format("java -Dcom.basho.riak.host=~s -Dcom.basho.riak.http.port=~p -Dcom.basho.riak.pbc.port=~p -cp ~s:~s org.junit.runner.JUnitCore com.basho.riak.client.AllTests",
        [HTTP_Host, HTTP_Port, PB_Port, rt:config(rt_scratch_dir) ++ "/" ++ ?JAVA_FAT_FILENAME, rt:config(rt_scratch_dir) ++ "/" ++ ?JAVA_TESTS_FILENAME]),
    lager:info("Cmd: ~s", [Cmd]),

    {ExitCode, JavaLog} = rt:stream_cmd(Cmd, [{cd, rt:config(rt_scratch_dir)}]),
    ?assertEqual(0, ExitCode),
    lager:info(JavaLog),
    ?assertNot(rt:str(JavaLog, "FAILURES!!!")),
    ok.
    

you_got_jars(Url, Filename) ->
    case file:read_file_info(Filename) of
        {ok, _} ->
            lager:info("Got it ~p", [Filename]),
            ok;
        {error, _} ->
            lager:info("Getting it ~p", [Filename]),
            rt:stream_cmd("curl  -O -L " ++ Url, [{cd, rt:config(rt_scratch_dir)}]);
        _ -> meh
    end.
