-module(client_python_verify).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(PYTHON_CLIENT_TAG, "1.5.1").
-define(PYTHON_CHECKOUT, filename:join([rt:config(rt_scratch_dir), "riak-python-client"])).
-define(PYTHON_GIT_URL, "git://github.com/basho/riak-python-client.git").

confirm() ->
    prereqs(),
    Config = [{riak_search, [{enabled, true}]}],
    [Node] = rt:deploy_nodes(1, Config),
    rt:wait_for_service(Node, riak_search),

    [{Node, ConnectionInfo}] = rt:connection_info([Node]),
    {HTTP_Host, HTTP_Port} = orddict:fetch(http, ConnectionInfo),
    {PB_Host, PB_Port} = orddict:fetch(pb, ConnectionInfo),

    lager:info("Connection Info: http: ~p:~p pb: ~p:~p", [HTTP_Host, HTTP_Port, PB_Host, PB_Port]),

    lager:info("Enabling search hook on 'searchbucket'"),
    rt:enable_search_hook(Node, <<"searchbucket">>),

    {ExitCode, PythonLog} = rt:stream_cmd("bin/python setup.py develop test",
                                          [{cd, ?PYTHON_CHECKOUT},
                                           {env,[{"RIAK_TEST_PB_HOST", PB_Host},
                                                 {"RIAK_TEST_PB_PORT", integer_to_list(PB_Port)},
                                                 {"RIAK_TEST_HTTP_HOST", HTTP_Host},
                                                 {"RIAK_TEST_HTTP_PORT", integer_to_list(HTTP_Port)},
                                                 {"SKIP_LUWAK", "1"}]}]),
    ?assertEqual(0, ExitCode),
    lager:info(PythonLog),
    ?assertNot(rt:str(PythonLog, "FAIL")),
    pass.

prereqs() ->
    %% Cleanup from previous test runs as it causes a
    %% virtualenv caremad explosion if it already exists
    lager:info("[PREREQ] Cleaning up python scratch directory"),
    CleanupCMD = io_lib:format("rm -rf ~s", [?PYTHON_CHECKOUT]),
    os:cmd(CleanupCMD),

    %% Need python, yo
    lager:info("[PREREQ] Checking for presence of python"),
    ?assertNot(length(os:cmd("which python")) =:= 0),

    %% Python should be at least 2.6, but not 3.x
    lager:info("[PREREQ] Checking for python version >= 2.6, < 3.0"),
    "Python 2." ++ [Minor|_] = os:cmd("python -V"),
    ?assert(Minor =:= $6 orelse Minor =:= $7),

    %% Need setuptools too
    lager:info("[PREREQ] Checking for presence of setuptools"),
    ?assertCmd("python -c 'import setuptools'"),

    %% Virtualenv will isolate this so we don't have permissions issues.
    lager:info("[PREREQ] Checking for presence of virtualenv"),
    ?assertCmd("virtualenv --help"),

    %% Checkout the project and a specific tag.
    lager:info("[PREREQ] Cloning riak-python-client from ~s", [?PYTHON_GIT_URL]),
    Cmd = io_lib:format("git clone ~s ~s", [?PYTHON_GIT_URL, ?PYTHON_CHECKOUT]),
    rt:stream_cmd(Cmd),

    lager:info("[PREREQ] Resetting python client to tag '~s'", [?PYTHON_CLIENT_TAG]),
    TagCmd = io_lib:format("git reset --hard ~s", [?PYTHON_CLIENT_TAG]),
    rt:stream_cmd(TagCmd, [{cd, ?PYTHON_CHECKOUT}]),

    lager:info("[PREREQ] Installing an isolated environment with virtualenv in ~s", [?PYTHON_CHECKOUT]),
    rt:stream_cmd("virtualenv --clear --no-site-packages .", [{cd, ?PYTHON_CHECKOUT}]),
    ok.
