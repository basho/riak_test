%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
%% Enable and disable tracing from the riak_test command line,
%% add `--tracing` to the test run to execute any calls to
%% `rt_redbug:trace/2` that are in the test suites. The traces
%% are printed to the test.log
%%
%% -------------------------------------------------------------------

-module(rt_redbug).

-export([is_tracing_applied/0]).
-export([new/0]).
-export([set_tracing_applied/1]).
-export([stop/1]).
-export([trace/2, trace/3]).

%% Create a new ets to store globally whether we're tracing or not.
new() -> 
    rt_trace_tab = ets:new(rt_trace_tab, [named_table]).

set_tracing_applied(TracingApplied) when is_boolean(TracingApplied) ->
	lager:info("Setting apply tracing to ~p", [TracingApplied]),
    ets:insert(rt_trace_tab, {apply_traces,TracingApplied}).

is_tracing_applied() ->
	(catch ets:lookup_element(rt_trace_tab, apply_traces, 2)) == true.

%% eper documentation for the redbug trace string and options is here:
%%
%% https://github.com/massemanet/eper/blob/master/doc/redbug.txt
%%
%% docs on profiling using redbug (for test timeouts) is here:
%%
%% http://roberto-aloi.com/erlang/profiling-erlang-applications-using-redbug
%%
%% Set a trace on a function
%%     "riak_kv_qry_compiler:compile"
%%
trace(Nodes, TraceStrings) ->
	trace(Nodes, TraceStrings, []).

trace(Nodes, TraceStrings, Options1) when (is_atom(Nodes) orelse is_list(Nodes)) ->
	case is_tracing_applied() of
		true ->
			Options2 = apply_options_to_defaults(Options1),
			[apply_trace(N, TraceStrings, Options2) || N <- ensure_list(Nodes)],
			ok;
		false ->
			ok
	end.

%%
apply_trace(Node, TraceString, Options) ->
	lager:info("APPLY TRACE ~p", [TraceString]),
	rpc:call(Node, redbug, start, [TraceString, Options]).

%%
apply_options_to_defaults(Options) ->
	lists:foldl(
		fun({K,_} = E,Acc) ->
			lists:keystore(K, 1, Acc, E)
		end, default_trace_options(), Options).

%%
default_trace_options() ->
	[
	 %% default ct timeout of 30 minutes
	 %% http://erlang.org/doc/apps/common_test/write_test_chapter.html#id77922
	 {time,(30*60*1000)}
	].

%% Stop redbug tracing on a node or list of nodes
stop(Nodes) ->
	case is_tracing_applied() of
		true ->
			[rpc:call(N, redbug, stop, []) || N <- ensure_list(Nodes)],
			ok;
		false ->
			ok
	end.

%% Doesn't work on lists of strings!
ensure_list(V) ->
	lists:flatten([V]).

