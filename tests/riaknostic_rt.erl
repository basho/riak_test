-module(riaknostic_rt).
-export([riaknostic_rt/0]).

%% Change when a new release comes out.
-define(RIAKNOSTIC_URL, "https://github.com/basho/riaknostic/downloads/riaknostic-1.0.2.tar.gz").

riaknostic_rt() ->
    %% Build a small cluster
    [Node1, _Node2] = rt:build_cluster(2, []),

    %% Since we don't have command output matching yet, we'll just
    %% print it to the console and hope for the best.
    lager:info("1. Check download message"),
    rt:admin(Node1, ["diag"]),
    
    %% Install
    lager:info("2a. Install Riaknostic"),
    {ok, LibDir} = rpc:call(Node1, application, get_env, [riak_core, platform_lib_dir]),
    Cmd = io_lib:format("sh -c \"cd ~s && curl -O -L ~s && tar xzf ~s\"", [LibDir, ?RIAKNOSTIC_URL, filename:basename(?RIAKNOSTIC_URL)]),
    lager:info("Running command: ~s", [Cmd]),
    lager:debug("~p~n", [rpc:call(Node1, os, cmd, [Cmd])]),
    
    %% Execute
    lager:info("2b. Check Riaknostic executes"),
    rt:admin(Node1, ["diag"]),
    
    %% Check usage message
    lager:info("3. Run Riaknostic usage message"),
    rt:admin(Node1, ["diag", "--help"]),
    
    %% Check commands list
    lager:info("4. Run Riaknostic commands list message"),
    rt:admin(Node1, ["diag", "--list"]),
    
    %% Check log levels
    lager:info("5. Run Riaknostic with a different log level"),
    rt:admin(Node1, ["diag", "--level", "debug"]),

    %% Check node conn failure when stopped
    lager:info("6. Riaknostic warns of node connection failure when stopped"),
    rt:stop(Node1),
    rt:admin(Node1, ["diag"]),
    
    %% Check node connection when started
    lager:info("7. Riaknostic connects to node when running"),
    rt:start(Node1),
    rt:admin(Node1, ["diag", "--level", "debug"]),
    
    %% Done!
    lager:info("Test riaknostic: PASS"),
    ok.
