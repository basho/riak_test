-module(client_java_verify).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% Change when a new release comes out.
-define(JAVA_FAT_BE_URL, "http://s3.amazonaws.com/builds.basho.com/riak-java-client/CURRENT/riak-client-1.0.6-SNAPSHOT-jar-with-dependencies-and-tests.jar").
-define(JAVA_FAT_FILENAME, "riak-client-1.0.6-SNAPSHOT-jar-with-dependencies-and-tests.jar").
-define(JAVA_TESTS_URL, "http://s3.amazonaws.com/builds.basho.com/riak-java-client/CURRENT/riak-client-1.0.6-SNAPSHOT-tests.jar").
-define(JAVA_TESTS_FILENAME, "riak-client-1.0.6-SNAPSHOT-tests.jar").

confirm() ->
    prereqs(),
    Nodes = rt:deploy_nodes(1),
    [Node1] = Nodes,
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),
    
    [{Node1, ConnectionInfo}] = rt:connection_info([Node1]),
    {HTTP_Host, HTTP_Port} = orddict:fetch(http, ConnectionInfo),
    {PB_Host, PB_Port} = orddict:fetch(pb, ConnectionInfo),

    lager:info("Connection Info: http: ~p:~p pb: ~p:~p", [HTTP_Host, HTTP_Port, PB_Host, PB_Port]),

    %% {Node, [{http, {HTTP_IP, HTTP_Port}}, {pb, {PB_IP, PB_Port}}]},
    java_unit_tests(HTTP_Host, HTTP_Port, PB_Host, PB_Port),
    pass.

prereqs() ->
    %% Does you have java available?
    ?assertNot(length(os:cmd("which java")) =:= 0),
    %% Does you have the java client available?
    
    you_got_jars(?JAVA_FAT_BE_URL, ?JAVA_FAT_FILENAME),
    you_got_jars(?JAVA_TESTS_URL, ?JAVA_TESTS_FILENAME),
    %% http://s3.amazonaws.com/builds.basho.com/riak-java-client/CURRENT/riak-client-1.0.6-SNAPSHOT-jar-with-dependencies-and-tests.jar
    %% http://s3.amazonaws.com/builds.basho.com/riak-java-client/CURRENT/riak-client-1.0.6-SNAPSHOT-tests.jar
    
    ok.

java_unit_tests(HTTP_Host, HTTP_Port, _PB_Host, PB_Port) ->
   
    
     lager:info("Run the Java unit tests from somewhere on the local machine."),
    %% go to folder
    
    %% run the following:
    

    Cmd = io_lib:format("java -Dcom.basho.riak.host=~s -Dcom.basho.riak.http.port=~p -Dcom.basho.riak.pbc.port=~p -cp ~s:~s org.junit.runner.JUnitCore com.basho.riak.client.AllTests",
        [HTTP_Host, HTTP_Port, PB_Port, ?JAVA_FAT_FILENAME, ?JAVA_TESTS_FILENAME]),
    lager:info("Cmd: ~s", [Cmd]),
    JavaLog = os:cmd(Cmd),
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
            Log = os:cmd("curl  -O -L " ++ Url),
            lager:info("curl log: ~p", [Log]);
        _ -> meh
    end.
