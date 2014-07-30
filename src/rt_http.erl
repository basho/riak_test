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
-module(rt_http).
-include("rt.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([http_url/1,
         https_url/1,
         httpc/1,
         httpc_read/3,
         httpc_write/4,
         get_http_conn_info/1,
         get_https_conn_info/1]).

%% @doc Returns HTTPS URL information for a list of Nodes
https_url(Nodes) when is_list(Nodes) ->
    [begin
         {Host, Port} = orddict:fetch(https, Connections),
         lists:flatten(io_lib:format("https://~s:~b", [Host, Port]))
     end || {_Node, Connections} <- rt:connection_info(Nodes)];
https_url(Node) ->
    hd(https_url([Node])).

%% @doc Returns HTTP URL information for a list of Nodes
http_url(Nodes) when is_list(Nodes) ->
    [begin
         {Host, Port} = orddict:fetch(http, Connections),
         lists:flatten(io_lib:format("http://~s:~b", [Host, Port]))
     end || {_Node, Connections} <- rt:connection_info(Nodes)];
http_url(Node) ->
    hd(http_url([Node])).

%% @doc get me an http client.
-spec httpc(node()) -> term().
httpc(Node) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, [{IP, Port}]} = get_http_conn_info(Node),
    rhc:create(IP, Port, "riak", []).

%% @doc does a read via the http erlang client.
-spec httpc_read(term(), binary(), binary()) -> binary().
httpc_read(C, Bucket, Key) ->
    {_, Value} = rhc:get(C, Bucket, Key),
    Value.

%% @doc does a write via the http erlang client.
-spec httpc_write(term(), binary(), binary(), binary()) -> atom().
httpc_write(C, Bucket, Key, Value) ->
    Object = riakc_obj:new(Bucket, Key, Value),
    rhc:put(C, Object).

-spec get_http_conn_info(node()) -> [{inet:ip_address(), pos_integer()}].
get_http_conn_info(Node) ->
    case rt:rpc_get_env(Node, [{riak_api, http},
                            {riak_core, http}]) of
        {ok, [{IP, Port}|_]} ->
            {ok, [{IP, Port}]};
        _ ->
            undefined
    end.


-spec get_https_conn_info(node()) -> [{inet:ip_address(), pos_integer()}].
get_https_conn_info(Node) ->
    case rt:rpc_get_env(Node, [{riak_api, https},
                            {riak_core, https}]) of
        {ok, [{IP, Port}|_]} ->
            {ok, [{IP, Port}]};
        _ ->
            undefined
    end.
