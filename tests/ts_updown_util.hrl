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
-type row()  :: [cell()].

-type config()  :: proplists:proplist().
-type version() :: current | previous.

-type cap_with_ver() :: {{atom(), atom()}, term()}.

-record(create, {
          should_skip = false :: boolean(),
          ddl      :: binary,
          expected :: term()
         }).

-record(insert, {
          should_skip = false :: boolean(),
          %% A list of data to write to the table
          data     :: [row()],
          expected :: term()
         }).

-record(select, {
          should_skip = false :: boolean(),
          %% the select query is an io_lib:format containing a single "~s" placeholder
          %% for the table name
          qry        :: binary(),
          expected   :: term(),
          assert_mod :: atom(),
          assert_fun :: atom()
         }).

-record(test_set, {
          testname     :: string(),
          create       :: #create{},
          insert       :: #insert{},
          selects = [] :: [#select{}],
          timestamp    :: string()
         }).

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

          tests = [] :: [#test_set{}],

          %% in case riak.conf generated for one version needs tweaked
          %% to be understood by the other, use these functions
          convert_config_to_previous = fun(_) -> ok end :: function(),
          convert_config_to_current  = fun(_) -> ok end :: function(),

          %% list of capability-version pairs, to wait until
          %% propagation thereof, before issuing a query
          %% - when the cluster is fully upgraded:
          ensure_full_caps = [] :: [cap_with_ver()],
          %% - when the cluster is mixed or fully downgraded:
          ensure_degraded_caps = [] :: [cap_with_ver()]
         }).

%% Error report
-record(failure_report, {
          cluster  :: [{node(), version()}],
          %% node where table was created
          table_node :: node(),
          %% node where the failing SELECT was issued
          query_node :: node(),

          %% the failing test proper
          failing_test :: binary(),

          %% Expected and Got
          message  :: binary(),
          expected :: term(),
          got      :: term()
         }).

-endif.
