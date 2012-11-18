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

    {ExitCode, PythonLog} = rt:stream_cmd("python setup.py develop test",
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
    %% Need python, yo
    ?assertNot(length(os:cmd("which python")) =:= 0),

    %% Python should be at least 2.6, but not 3.x
    "Python 2." ++ [Minor|_] = os:cmd("python -V"),
    ?assert(Minor =:= $6 orelse Minor =:= $7),

    %% Need setuptools too
    ?assertCmd("python -c 'import setuptools'"),

    %% Checkout the project and a specific tag.
    case file:read_file_info(?PYTHON_CHECKOUT) of
        {error, _} ->
            lager:info("Cloning riak-python-client from ~s", [?PYTHON_GIT_URL]),
            Cmd = io_lib:format("git clone ~s ~s", [?PYTHON_GIT_URL, ?PYTHON_CHECKOUT]),
            rt:stream_cmd(Cmd);
        _ -> ok
    end,
    TagCmd = io_lib:format("git reset --hard ~s", [?PYTHON_CLIENT_TAG]),
    rt:stream_cmd(TagCmd, [{cd, ?PYTHON_CHECKOUT}]),
    ok.
