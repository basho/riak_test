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

%% Scenario description and requirements
-record(scenario, {
          %% riak version on the node where CREATE TABLE query will be issued
          table_node_vsn :: version(),

          %% riak version on the node where SELECT queries will be issued
          query_node_vsn :: version(),

          %% whether to up- or downgrade the interesting nodes after
          %% table creation (before running SELECT queries)
          need_table_node_transition :: boolean(),
          need_query_node_transition :: boolean(),

          %% hints to try to ensure cluster homogeneity after any node
          %% transitions (may not be honoured depending on whether the
          %% interesting nodes emerge in different or same version
          %% after transitions)
          need_pre_cluster_mixed :: boolean(),
          need_post_cluster_mixed :: boolean(),

          %% Table DDL (an io:format template containing a single
          %% "~s", because we will be generating a unique name for
          %% each scenario)
          ddl :: binary(),

          %% Data to write to the table
          data :: [row()],

          %% a list of {SelectQueryFmt, Expected} (`SelectQueryFmt'
          %% must contain a single "~s" placeholder for the table name
          select_vs_expected :: [{binary(), term()}]
         }).


%% Error report
-record(failure_report, {
          %% node composition, with versions at each node
          cluster :: [{node(), version()}],

          %% node where table was created
          table_node :: node(),

          %% node where the failing SELECT was issued
          query_node :: node(),

          %% whether the table and querying nodes were upgraded or
          %% downgraded prior to SELECT query
          did_transition_table_node :: boolean(),
          did_transition_query_node :: boolean(),

          %% the failing query proper
          failing_query :: binary(),

          %% Expected and Got
          expected :: term(),
          error :: term()
         }).

-endif.
