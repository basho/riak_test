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
    ?assert(rt:assertRegex("is not present!", RiaknosticOut1)),
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
    ?assert(not(rt:assertRegex("is not present!", RiaknosticOut2b))),
    ?assert(not(rt:assertRegex("\\[debug\\]", RiaknosticOut2b))),
    
    %% Check usage message
    lager:info("3. Run Riaknostic usage message"),
    {ok, RiaknosticOut3} = rt:admin(Node1, ["diag", "--help"]),
    ?assert(rt:assertRegex("Usage: riak-admin", RiaknosticOut3)),
    
    %% Check commands list
    lager:info("4. Run Riaknostic commands list message"),
    {ok, RiaknosticOut4} = rt:admin(Node1, ["diag", "--list"]),
    ?assert(rt:assertRegex("Available diagnostic checks", RiaknosticOut4)),
    ?assert(rt:assertRegex("  disk           ", RiaknosticOut4)),
    ?assert(rt:assertRegex("  dumps          ", RiaknosticOut4)),
    ?assert(rt:assertRegex("  memory_use     ", RiaknosticOut4)),
    ?assert(rt:assertRegex("  nodes_connected", RiaknosticOut4)),
    ?assert(rt:assertRegex("  ring_membership", RiaknosticOut4)),
    ?assert(rt:assertRegex("  ring_preflists ", RiaknosticOut4)),
    ?assert(rt:assertRegex("  ring_size      ", RiaknosticOut4)),
    ?assert(rt:assertRegex("  search         ", RiaknosticOut4)),
    
    %% Check log levels
    lager:info("5. Run Riaknostic with a different log level"),
    {ok, RiaknosticOut5} = rt:admin(Node1, ["diag", "--level", "debug"]),
    ?assert(rt:assertRegex("\\[debug\\]", RiaknosticOut5)),

    %% Check node conn failure when stopped
    lager:info("6. Riaknostic warns of node connection failure when stopped"),
    rt:stop(Node1),
    {ok, RiaknosticOut6} = rt:admin(Node1, ["diag"]),
    ?assert(rt:assertRegex("\\[warning\\] Could not connect", RiaknosticOut6)),
    
    %% Check node connection when started
    lager:info("7. Riaknostic connects to node when running"),
    rt:start(Node1),
    {ok, RiaknosticOut7} = rt:admin(Node1, ["diag", "--level", "debug"]),
    ?assert(rt:assertRegex("\\[debug\\] Not connected", RiaknosticOut7)),
    ?assert(rt:assertRegex("\\[debug\\] Connected to local Riak node", RiaknosticOut7)),
    
    %% Done!
    lager:info("Test riaknostic: PASS"),
    pass.
