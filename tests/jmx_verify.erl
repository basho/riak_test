%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(jmx_verify).
-behavior(riak_test).
-export([confirm/0, test_supervision/0]).
-include_lib("eunit/include/eunit.hrl").

-prereq("java").

%% You should have curl installed locally to do this.
confirm() ->
    test_supervision(),

    test_application_stop(),

    JMXPort = 41111,
    Config = [{riak_jmx, [{enabled, true}, {port, JMXPort}]}],
    Nodes = rt_cluster:deploy_nodes(1, Config),
    [Node1] = Nodes,
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),

    [{http, {IP, _Port}}|_] = rt:connection_info(Node1),

    JMXDumpCmd = jmx_dump_cmd(IP, JMXPort),

    JMX1 = jmx_dump(JMXDumpCmd),

    %% make sure a set of stats have valid values
    verify_nz(JMX1, [<<"cpu_nprocs">>,
                     <<"mem_total">>,
                     <<"mem_allocated">>,
                     <<"ring_creation_size">>]),

    lager:info("perform 5 x  PUT and a GET to increment the stats"),
    lager:info("as the stat system only does calcs for > 5 readings"),

    C = rt_http:httpc(Node1),
    [rt_http:httpc_write(C, <<"systest">>, <<X>>, <<"12345">>) || X <- lists:seq(1, 5)],
    [rt_http:httpc_read(C, <<"systest">>, <<X>>) || X <- lists:seq(1, 5)],

    JMX2 = jmx_dump(JMXDumpCmd),
    %% make sure the stats that were supposed to increment did
    verify_inc(JMX1, JMX2, [{<<"node_gets">>, 10},
                            {<<"node_puts">>, 5},
                            {<<"node_gets_total">>, 10},
                            {<<"node_puts_total">>, 5},
                            {<<"vnode_gets">>, 30},
                            {<<"vnode_puts">>, 15},
                            {<<"vnode_gets_total">>, 30},
                            {<<"vnode_puts_total">>, 15}]),

    %% verify that fsm times were tallied
    verify_nz(JMX2, [<<"node_get_fsm_time_mean">>,
                     <<"node_get_fsm_time_median">>,
                     <<"node_get_fsm_time_95">>,
                     <<"node_get_fsm_time_99">>,
                     <<"node_get_fsm_time_100">>,
                     <<"node_put_fsm_time_mean">>,
                     <<"node_put_fsm_time_median">>,
                     <<"node_put_fsm_time_95">>,
                     <<"node_put_fsm_time_99">>,
                     <<"node_put_fsm_time_100">>]),

    lager:info("Make PBC Connection"),
    Pid = rt_pb:pbc(Node1),

    JMX3 = jmx_dump(JMXDumpCmd),
    rt:systest_write(Node1, 1),
    %% make sure the stats that were supposed to increment did
    verify_inc(JMX2, JMX3, [{<<"pbc_connects_total">>, 1},
                            {<<"pbc_connects">>, 1},
                            {<<"pbc_active">>, 1}]),

    lager:info("Force Read Repair"),
    rt_pb:pbc_write(Pid, <<"testbucket">>, <<"1">>, <<"blah!">>),
    rt:pbc_set_bucket_prop(Pid, <<"testbucket">>, [{n_val, 4}]),

    JMX4 = jmx_dump(JMXDumpCmd),

    verify_inc(JMX3, JMX4, [{<<"read_repairs_total">>, 0},
                            {<<"read_repairs">>, 0}]),

    _Value = rt_pb:pbc_read(Pid, <<"testbucket">>, <<"1">>),

    %%Stats5 = get_stats(Node1),
    JMX5 = jmx_dump(JMXDumpCmd),
    verify_inc(JMX3, JMX5, [{<<"read_repairs_total">>, 1},
                            {<<"read_repairs">>, 1}]),
    pass.

test_supervision() ->
    JMXPort = 41111,
    Config = [{riak_jmx, [{enabled, true}, {port, JMXPort}]}],
    [Node|[]] = rt_cluster:deploy_nodes(1, Config),
    timer:sleep(20000),
    case net_adm:ping(Node) of
        pang ->
            lager:error("riak_jmx crash able to crash riak node"),
            ?assertEqual("riak_jmx crash able to crash riak node", true);
        _ ->
            yay
    end,

    %% Let's make sure the thing's restarting as planned
    lager:info("calling riak_jmx:stop() to reset retry counters"),
    rpc:call(Node, riak_jmx, stop, ["stopping for test purposes"]),

    lager:info("loading lager backend on node"),
    rt:load_modules_on_nodes([riak_test_lager_backend], [Node]),
    ok = rpc:call(Node, gen_event, add_handler, [lager_event, riak_test_lager_backend, [info, false]]),
    ok = rpc:call(Node, lager, set_loglevel, [riak_test_lager_backend, info]),

    lager:info("Now we're capturing logs on the node, let's start jmx"),
    lager:info("calling riak_jmx:start() to get these retries started"),
    rpc:call(Node, riak_jmx, start, []),

    lager:info("It can fail, it can fail 10 times"),

    rt:wait_until(retry_check_fun(Node)),
    rt_node:stop(Node),
    ok_ok.

retry_check_fun(Node) ->
    fun() ->
            Logs = rpc:call(Node, riak_test_lager_backend, get_logs, []),
             10 =:= lists:foldl(log_fold_fun(), 0, Logs)
    end.

log_fold_fun() ->
    fun(Log, Sum) ->
            try case re:run(Log, "JMX server monitor .* exited with code .*\. Retry #.*", []) of
                    {match, _} -> 1 + Sum;
                    _ -> Sum
                end
            catch
                Err:Reason ->
                    lager:error("jmx supervision re:run failed w/ ~p: ~p", [Err, Reason]),
                    Sum
            end
    end.

test_application_stop() ->
    lager:info("Testing application:stop()"),
    JMXPort = 41111,
    Config = [{riak_jmx, [{enabled, true}, {port, JMXPort}]}],
    Nodes = rt_cluster:deploy_nodes(1, Config),
    [Node] = Nodes,
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node])),

    %% Let's make sure the java process is alive!
    lager:info("checking for riak_jmx.jar running."),
    rt:wait_until(Node, fun(_N) ->
        try case re:run(rpc:call(Node, os, cmd, ["ps -Af"]), "riak_jmx.jar", []) of
            nomatch -> false;
            _ -> true
            end
        catch
        Err:Reason ->
            lager:error("jmx stop re:run failed w/ ~p: ~p", [Err, Reason]),
            false
        end
    end),

    rpc:call(Node, riak_jmx, stop, ["Stopping riak_jmx"]),
    timer:sleep(20000),
    case net_adm:ping(Node) of
        pang ->
            lager:error("riak_jmx stop takes down riak node"),
            ?assertEqual("riak_jmx stop takes down riak node", true);
        _ ->
            yay
    end,

    %% Let's make sure the java process is dead!
    lager:info("checking for riak_jmx.jar not running."),

    ?assertEqual(nomatch, re:run(rpc:call(Node, os, cmd, ["ps -Af"]), "riak_jmx.jar", [])),

    rt_node:stop(Node).

verify_inc(Prev, Props, Keys) ->
    [begin
         Old = proplists:get_value(Key, Prev, 0),
         New = proplists:get_value(Key, Props, 0),
         lager:info("~s: ~p -> ~p (expected ~p)", [Key, Old, New, Old + Inc]),
         ?assertEqual({Key, New}, {Key, (Old + Inc)})
     end || {Key, Inc} <- Keys].

verify_nz(Props, Keys) ->
    [?assertNotEqual({Key, proplists:get_value(Key,Props,0)}, {Key, 0}) || Key <- Keys].

jmx_jar_path() ->
    %% Find riak_jmx.jar
    DepsPath = rt:get_deps(),
    Deps = string:tokens(os:cmd("ls " ++ DepsPath), "\n"),
    [RiakJMX] = lists:filter(fun(X) -> string:str(X, "riak_jmx") == 1 end, Deps),
    filename:join([DepsPath, RiakJMX, "priv", "riak_jmx.jar"]).

jmx_dump_cmd(IP, Port) ->
    io_lib:format("java -cp ~s com.basho.riak.jmx.Dump ~s ~p",
        [jmx_jar_path(), IP, Port]).

jmx_dump(Cmd) ->
    timer:sleep(40000), %% JMX only updates every 30seconds
    lager:info("Dumping JMX stats using command ~s", [Cmd]),
    Output = string:strip(os:cmd(Cmd), both, $\n),
    JSONOutput = mochijson2:decode(Output),
    [ {Key, Value} || {struct, [{Key, Value}]} <- JSONOutput].
