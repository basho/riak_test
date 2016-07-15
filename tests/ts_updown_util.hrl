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
%% -------------------------------------------------------------------

-ifndef(TS_UPDOWN_UTIL_HRL).
-define(TS_UPDOWN_UTIL_HRL, included).

-define(CFG(K, C), proplists:get_value(K, C)).

-type cell() :: integer() | float() | binary() | boolean().
-type row() :: [cell()].

-type config() :: proplists:proplist().
-type version() :: current | previous.

-record(scenario, {
          table_node_vsn :: version(),
          query_node_vsn :: version(),
          need_table_node_transition :: boolean(),
          need_query_node_transition :: boolean(),
          need_pre_cluster_mixed :: boolean(),
          need_post_cluster_mixed :: boolean(),
          table :: binary(),
          data :: [row()],
          ddl :: binary(),
          select_vs_expected :: [{binary(), term()}]
         }).

-record(failure_report, {
          cluster :: [node()],
          table_node :: node(),
          query_node :: node(),
          did_transition_table_node :: boolean(),
          did_transition_query_node :: boolean(),
          failing_query :: binary(),
          expected :: term(),
          error :: term()
         }).

-endif.
