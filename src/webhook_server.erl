%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.
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
-module(webhook_server).

-behaviour(gen_server).

%% API
-export([start_link/1, get_next/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% mochiweb callbacks
-export([receive_callback/1]).

-define(SERVER, ?MODULE).

-record(state, {
    receipts = queue:new()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(WebhookPort) ->
    {ok, _Pid} = gen_server:start_link({local, ?SERVER}, ?MODULE, [WebhookPort], []),
    ok.

get_next() ->
    gen_server:call(?MODULE, get_next).

receive_callback(Req) ->
    %% Beware, recv_body will fail if we call it outside this request
    %% handler process, because mochiweb secretly stores stuff
    %% in the process dictionary. The failure is also silent and
    %% mysterious, due to mochiweb_request calling `exit(normal)`
    %% in several places instead of crashing or properly handling
    %% the error... o_O
    Body = Req:recv_body(),
    Req:respond({200, [], []}),
    lager:info("Received call to webhook."),
    gen_server:cast(?MODULE, {got_http_req, Req, Body}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([WebhookPort]) ->
    {ok, _Pid} = mochiweb_http:start_link([{name, webhook_server_internal}, {loop, fun receive_callback/1}, {port, WebhookPort}]),
    {ok, #state{}}.

handle_call(get_next, _From, #state{receipts=Receipts} = State) ->
    case queue:out(Receipts) of
        {{value, Receipt}, Receipts2} ->
            {reply, Receipt, State#state{receipts=Receipts2}};
        {empty, Receipts} ->
            {reply, empty, State}
    end.

handle_cast({got_http_req, _Req, _Body} = Receipt, #state{receipts=Receipts} = State) ->
    {noreply, State#state{receipts = queue:in(Receipt, Receipts)}}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    mochiweb_http:stop(webhook_server_internal),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

