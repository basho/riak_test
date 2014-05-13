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


%%
%{riak_test, [
%    {src_dirs, ["test/src", "deps/riak_test_"]}
%]}.

-module(rebar_riak_test_plugin).

-export([
    clean/2,
    compile/2,
    rt_run/2
]).

%% ===================================================================
%% Public API
%% ===================================================================
clean(Config, AppFile) ->
    case should_i_run(Config) of
        false -> ok;
        _ -> riak_test_clean(Config, AppFile)
    end.

compile(Config, AppFile) ->
    case should_i_run(Config) of
        false -> ok;
        _ -> riak_test_compile(Config, AppFile)
    end.
    
rt_run(Config, AppFile) ->
    case should_i_run(Config) of
        false -> ok;
        _ -> riak_test_run(Config, AppFile)
    end.

%% ===================================================================
%% Private Functions - pronounced Funk-tee-owns, not funk-ee-towns
%% ===================================================================
should_i_run(Config) ->
    %% Only run on the base dir
    hd(lists:reverse(element(3, Config))) =:= local andalso proplists:is_defined(riak_test, element(3, Config)).

option(Key, Config) ->
    case proplists:get_value(riak_test, element(3, Config), not_configured) of
        not_configured -> {error, not_configured};
        RTConfig ->
            proplists:get_value(Key, RTConfig, {error, not_set})
    end.

riak_test_clean(Config, _AppFile) ->
    case option(test_output, Config) of
        {error, not_set} -> 
            io:format("No test_output directory set, check your rebar.config");
        TestOutputDir ->
            io:format("Removing test_output dir ~s~n", [TestOutputDir]),
            rebar_file_utils:rm_rf(TestOutputDir)
    end,
    ok.

riak_test_compile(Config, AppFile) ->    
    CompilationConfig = compilation_config(Config),
    rebar_erlc_compiler:compile(CompilationConfig, AppFile),
    ok.
    
riak_test_run(Config, _AppFile) ->
    RiakTestConfig = rebar_config:get_global(Config, config, "rtdev"),
    Test = rebar_config:get_global(Config, test, ""),
    code:add_pathsz([rebar_utils:ebin_dir(), option(test_output, Config)]),
    riak_test:main(["-c", RiakTestConfig, "-t", Test]),
    ok.
    
compilation_config(Conf) ->
    C1 = rebar_config:set(Conf, riak_test, undefined),
    C2 = rebar_config:set(C1, plugins, undefined),
    ErlOpts = proplists:get_value(erl_opts, element(3, Conf)),
    ErlOpts1 = proplists:delete(src_dirs, ErlOpts),
    ErlOpts2 = [{parse_transform,lager_transform},{outdir, option(test_output, Conf)}, {src_dirs, option(test_paths, Conf)} | ErlOpts1],
    case eqc_present() of
       true ->
           ErlOpts3 = [{d, 'EQC'},{d, 'TEST'} | ErlOpts2];
       false ->
           ErlOpts3 = [{d, 'TEST'} | ErlOpts2]
    end,
    io:format("erl_opts: ~p~n", [ErlOpts3]),
    rebar_config:set(C2, erl_opts, ErlOpts3).

eqc_present() ->
    case catch eqc:version() of
        {'EXIT', {undef, _}} ->
            false;
        _ ->
            true
    end.
