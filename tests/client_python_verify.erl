-module(client_python_verify).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(PYTHON_CLIENT_TAG, "1.5.2").
-define(PYTHON_CHECKOUT, filename:join([rt_config:get(rt_scratch_dir), "riak-python-client"])).
-define(PYTHON_GIT_URL, "git://github.com/basho/riak-python-client.git").

%% Need python, yo
-prereq("python").
%% Virtualenv will isolate this so we don't have permissions issues.
-prereq("virtualenv").

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),
    {ok, TestCommand} = prereqs(),
    Config = [{riak_kv, [{secondary_index_sort_default, true}]},
              {riak_search, [{enabled, true}]}],
    [Node] = rt:deploy_nodes(1, Config),
    rt:wait_for_service(Node, riak_search),

    [{Node, ConnectionInfo}] = rt:connection_info([Node]),
    {HTTP_Host, HTTP_Port} = orddict:fetch(http, ConnectionInfo),
    {PB_Host, PB_Port} = orddict:fetch(pb, ConnectionInfo),

    lager:info("Connection Info: http: ~p:~p pb: ~p:~p", [HTTP_Host, HTTP_Port, PB_Host, PB_Port]),

    lager:info("Enabling search hook on 'searchbucket'"),
    rt:enable_search_hook(Node, <<"searchbucket">>),

    {ExitCode, PythonLog} = rt_local:stream_cmd(TestCommand,
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

    %% Python should be at least 2.6, but not 3.x
    lager:info("[PREREQ] Checking for python version >= 2.6, < 3.0"),
    "Python 2." ++ [Minor|_] = Version = os:cmd("python -V"),
    lager:info("[PREREQ] Got python version: ~s", [Version]),
    ?assert(Minor =:= $6 orelse Minor =:= $7),

    %% Need setuptools too
    lager:info("[PREREQ] Checking for presence of setuptools"),
    ?assertCmd("python -c 'import setuptools'"),


    %% Checkout the project and a specific tag.
    lager:info("[PREREQ] Cloning riak-python-client from ~s", [?PYTHON_GIT_URL]),
    Cmd = io_lib:format("git clone ~s ~s", [?PYTHON_GIT_URL, ?PYTHON_CHECKOUT]),
    rt_local:stream_cmd(Cmd),

    lager:info("[PREREQ] Resetting python client to tag '~s'", [?PYTHON_CLIENT_TAG]),
    TagCmd = io_lib:format("git checkout ~s", [?PYTHON_CLIENT_TAG]),
    rt_local:stream_cmd(TagCmd, [{cd, ?PYTHON_CHECKOUT}]),

    lager:info("[PREREQ] Installing an isolated environment with virtualenv in ~s", [?PYTHON_CHECKOUT]),
    rt_local:stream_cmd("virtualenv --clear --no-site-packages .", [{cd, ?PYTHON_CHECKOUT}]),

    lager:info("[PREREQ] Installing dependencies"),
    rt_local:stream_cmd("bin/python setup.py develop", [{cd, ?PYTHON_CHECKOUT}]),
    case Minor of
        $6 ->
            lager:info("[PREREQ] Installing unittest2 for python 2.6"),
            rt_local:stream_cmd("bin/easy_install unittest2", [{cd, ?PYTHON_CHECKOUT}]),
            {ok, "bin/unit2 riak.tests.test_all"};
        _ ->
            {ok, "bin/python setup.py test"}
    end.
