%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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

-record(rt_webhook, {
        name :: string(),
        url :: string(),
        headers=[] :: [{atom(), string()}]
        }).

-record(rt_properties, {
          nodes :: [node()],
          node_count=6 :: non_neg_integer(),
          metadata=[] :: proplists:proplist(),
          properties=[] :: proplists:proplist(),
          rolling_upgrade=false :: boolean(),
          start_version=current :: atom(),
          upgrade_version=current :: atom(),
          wait_for_transfers=false :: boolean(),
          valid_backends=all :: all | [atom()],
          make_cluster=true :: boolean(),
          config :: term()
         }).
-type rt_properties() :: #rt_properties{}.
