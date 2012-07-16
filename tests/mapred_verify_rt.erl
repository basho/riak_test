%% @doc Runs the mapred_verify tests from
%% http://github.com/basho/mapred_verify

-module(mapred_verify_rt).

-export([mapred_verify_rt/0]).

-define(NODE_COUNT, 3).

mapred_verify_rt() ->
    lager:info("Deploy ~b nodes", [?NODE_COUNT]),
    Nodes = rt:deploy_nodes(?NODE_COUNT),
    
    lager:info("Join nodes"),
    lists:foreach(fun(N) -> rt:join(N, hd(Nodes)) end, tl(Nodes)),
    
    lager:info("Wait for cluster to be ready"),
    rt:wait_until_all_members(Nodes),
    rt:wait_until_nodes_ready(Nodes),
    rt:wait_until_no_pending_changes(Nodes),
    
    PrivDir = code:priv_dir(mapred_verify),
    MRVProps = [{node, hd(Nodes)},
                %% don't need 'path' because riak_test does that for us
                {keycount, 1000},
                {bodysize, 1},
                {populate, true},
                {runjobs, true},
                {testdef, filename:join(PrivDir, "tests.def")}],
    
    lager:info("Run mapred_verify"),
    0 = mapred_verify:do_verification(MRVProps),
    lager:info("~s: PASS", [atom_to_list(?MODULE)]),
    ok.
