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
%% @doc Runs the mapred_verify tests from
%% http://github.com/basho/mapred_verify

-module(mapred_verify_rt).

-behavior(riak_test).
-export([confirm/0]).

-define(NODE_COUNT, 3).

confirm() ->
    lager:info("Build ~b node cluster", [?NODE_COUNT]),
    Nodes = rt:build_cluster(?NODE_COUNT),
    
    MRVProps = [{node, hd(Nodes)},
                %% don't need 'path' because riak_test does that for us
                {keycount, 1000},
                {bodysize, 1},
                {populate, true},
                {runjobs, true},
                {testdef, filename:join("./_build/default/lib/mapred_verify/priv", "tests.def")}],
    
    lager:info("Run mapred_verify"),
    0 = mapred_verify:do_verification(MRVProps),
    lager:info("~s: PASS", [atom_to_list(?MODULE)]),
    pass.
