-module(riaknostic_rt).
-export([riaknostic_rt/0]).
-include_lib("eunit/include/eunit.hrl").

%% Change when a new release comes out.
-define(RIAKNOSTIC_URL, "https://github.com/basho/riaknostic/downloads/riaknostic-1.0.2.tar.gz").

riaknostic_rt() ->
    %% Build a small cluster
    [Node1, _Node2] = rt:build_cluster(2, []),

    %% Since we don't have command output matching yet, we'll just
    %% print it to the console and hope for the best.
    lager:info("1. Check download message"),
    {ok, RiaknosticOut1} = rt:admin(Node1, ["diag"]),
    ?assert(riak_test_util:contains_substring(RiaknosticOut1, "is not present!")),
    %%dut.cmd_output_match('Riak diagnostics utility is not present')

    %% Install
    lager:info("2a. Install Riaknostic"),
    {ok, LibDir} = rpc:call(Node1, application, get_env, [riak_core, platform_lib_dir]),
    Cmd = io_lib:format("sh -c \"cd ~s && curl -O -L ~s && tar xzf ~s\"", [LibDir, ?RIAKNOSTIC_URL, filename:basename(?RIAKNOSTIC_URL)]),
    lager:info("Running command: ~s", [Cmd]),
    lager:debug("~p~n", [rpc:call(Node1, os, cmd, [Cmd])]),
    
    %% Execute
    lager:info("2b. Check Riaknostic executes"),
    {ok, RiaknosticOut2b} = rt:admin(Node1, ["diag"]),
    ?assertNot(riak_test_util:contains_substring(RiaknosticOut2b, "is not present!")),
    ?assertNot(riak_test_util:contains_substring(RiaknosticOut2b, "[debug]")),
    
    %% Check usage message
    lager:info("3. Run Riaknostic usage message"),
    {ok, RiaknosticOut3} = rt:admin(Node1, ["diag", "--help"]),
    ?assert(riak_test_util:contains_substring(RiaknosticOut3, "Usage: riak-admin")),
    
    %% Check commands list
    lager:info("4. Run Riaknostic commands list message"),
    {ok, RiaknosticOut4} = rt:admin(Node1, ["diag", "--list"]),
    ?assert(riak_test_util:contains_substring(RiaknosticOut4, "Available diagnostic checks")),
    ?assert(riak_test_util:contains_substring(RiaknosticOut4, "  disk           ")),
    ?assert(riak_test_util:contains_substring(RiaknosticOut4, "  dumps          ")),
    ?assert(riak_test_util:contains_substring(RiaknosticOut4, "  memory_use     ")),
    ?assert(riak_test_util:contains_substring(RiaknosticOut4, "  nodes_connected")),
    ?assert(riak_test_util:contains_substring(RiaknosticOut4, "  ring_membership")),
    ?assert(riak_test_util:contains_substring(RiaknosticOut4, "  ring_preflists ")),
    ?assert(riak_test_util:contains_substring(RiaknosticOut4, "  ring_size      ")),
    ?assert(riak_test_util:contains_substring(RiaknosticOut4, "  search         ")),
    
    %% Check log levels
    lager:info("5. Run Riaknostic with a different log level"),
    {ok, RiaknosticOut5} = rt:admin(Node1, ["diag", "--level", "debug"]),
    ?assert(riak_test_util:contains_substring(RiaknosticOut5, "[debug]")),

    %% Check node conn failure when stopped
    lager:info("6. Riaknostic warns of node connection failure when stopped"),
    rt:stop(Node1),
    {ok, RiaknosticOut6} = rt:admin(Node1, ["diag"]),
    ?assert(riak_test_util:contains_substring(RiaknosticOut6, "[warning] Could not connect")),
    
    %% Check node connection when started
    lager:info("7. Riaknostic connects to node when running"),
    rt:start(Node1),
    {ok, RiaknosticOut7} = rt:admin(Node1, ["diag", "--level", "debug"]),
    ?assert(riak_test_util:contains_substring(RiaknosticOut7, "[debug] Not connected")),
    ?assert(riak_test_util:contains_substring(RiaknosticOut7, "[debug] Connected to local Riak node")),
    
    %% Done!
    lager:info("Test riaknostic: PASS"),
    pass.
