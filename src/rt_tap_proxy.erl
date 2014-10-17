%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(rt_tap_proxy).

-behavior(gen_server).

-export([init/1,
         get_metrics_for_node/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         increment_counter_by_one/2,
         start_link/0, 
         stop/0,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, { metrics }).

%%%===================================================================
%%% API
%%%===================================================================
get_metrics_for_node(Node) ->
    gen_server:call({global, ?SERVER}, {get_metrics_for_node, Node}, infinity).

increment_counter_by_one(Node, Metric) ->
    gen_server:cast({global, ?SERVER}, {increment_counter_by_one, Node, Metric}).

start_link() ->
    gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:cast({global, ?SERVER}, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    {ok, #state{metrics = dict:new()}}.

handle_call({get_metrics_for_node, Node}, _From, State=#state{metrics=Metrics}) ->
    {reply, maybe_create_node_metrics(Node, Metrics), State}.

handle_cast({increment_counter_by_one, Node, Metric}, _State=#state{metrics=Metrics}) ->
    NodeMetrics = maybe_create_metric(Metric, maybe_create_node_metrics(Node, Metrics)),
    
    UpdatedNodeMetrics = dict:update_counter(Metric, 1, NodeMetrics),
    UpdatedMetrics = dict:store(Node, UpdatedNodeMetrics, Metrics),
    
    {noreply, #state{metrics = UpdatedMetrics}}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
maybe_create_metric(Metric, NodeMetrics) ->
    maybe_create_metric(dict:is_key(Metric, NodeMetrics), Metric, NodeMetrics).

maybe_create_metric(true, _Metric, NodeMetrics) ->
    NodeMetrics;
maybe_create_metric(false, Metric, NodeMetrics) ->
    dict:store(Metric, 0, NodeMetrics).

maybe_create_node_metrics(Node, Metrics) ->
    maybe_create_node_metrics(dict:is_key(Node, Metrics), Node, Metrics).

maybe_create_node_metrics(true, Node, Metrics) ->
    dict:fetch(Node, Metrics);
maybe_create_node_metrics(false, _Node, _Metrics) ->
    dict:new().

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_NODE, ?SERVER).
-define(TEST_METRIC, node_gets).

missing_metric_test() ->
    NodeMetrics = maybe_create_metric(?TEST_METRIC, dict:new()),
    ?assertEqual(0, dict:fetch(?TEST_METRIC, NodeMetrics)).

existing_metric_test() ->
    MetricValue = 100,
    NodeMetrics = maybe_create_metric(?TEST_METRIC, 
                                      dict:store(?TEST_METRIC, MetricValue, dict:new())),
    ?assertEqual(MetricValue, dict:fetch(?TEST_METRIC, NodeMetrics)).

missing_node_metrics_test() ->
    NodeMetrics = maybe_create_node_metrics(?TEST_NODE, dict:new()), 
    ?assertEqual(0, dict:size(NodeMetrics)).

existing_node_metrics_test() ->
    ExpectedNodeMetrics = dict:store(?TEST_METRIC, 100, dict:new()),
    Metrics = dict:store(?TEST_NODE, ExpectedNodeMetrics, dict:new()),
    ActualNodeMetrics = maybe_create_node_metrics(?TEST_NODE, Metrics),
    ?assertEqual(ActualNodeMetrics, ExpectedNodeMetrics).

increment_node_metric_test() ->
    application:load(rt_tap_proxy),
    start_link(),
    increment_counter_by_one(?TEST_NODE, ?TEST_METRIC),
    NodeMetrics = get_metrics_for_node(?TEST_NODE),
    ?assertEqual(1, dict:fetch(?TEST_METRIC, NodeMetrics)).
    
-endif.
