%%-------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%% @author Brett Hazen
%% @copyright (C) 2015, Basho Technologies
%% @doc
%% Communicate with the GiddyUp web service.  Pulls jobs to do and
%% report results back up to GiddyUp.
%% @end
%% Created : 20. Apr 2015 10:39 AM
%%-------------------------------------------------------------------
-module(giddyup).
-author("Brett Hazen").

-behaviour(gen_server).

%% API
-export([start_link/7,
    get_test_plans/0,
    post_result/2,
    post_artifact/3]).

-define(STREAM_CHUNK_SIZE, 8192).

-record(rt_webhook, {
    name :: string(),
    url :: string(),
    headers=[] :: [{atom(), string()}]
}).

%% gen_server callbacks
-export([init/1,
    stop/0,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    platform :: string(),
    default_product :: string(),
    default_version_number :: string(),
    default_version :: string(),
    giddyup_host :: string(),
    giddyup_user :: string(),
    giddyup_password :: string(),
    %% A dictionary of Base URLs to store artifacts
    artifact_base :: dict()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(string(), string(), string(), string(), string(), string(), string()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Platform, Product, VersionNumber, Version, GiddyUpHost, GiddyUpUser, GiddyUpPassword) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Platform, Product, VersionNumber, Version, GiddyUpHost, GiddyUpUser, GiddyUpPassword], []).

%%--------------------------------------------------------------------
%% @doc
%% Stops the server
%%
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Get the entire test suite for the specified platform
%%
%% @end
%%--------------------------------------------------------------------
get_test_plans() ->
    %% TODO: Is this a good timeout?
    gen_server:call(?MODULE, get_test_plans, 60000).

%%--------------------------------------------------------------------
%% @doc
%% Send a test result back up to GiddyUp
%% @end
%%--------------------------------------------------------------------

-spec post_result(rt_test_plan:test_plan(), pass | {fail, string()}) -> ok.
post_result(TestPlan, TestResult) ->
    gen_server:cast(?MODULE, {post_result, TestPlan, TestResult}).

%%--------------------------------------------------------------------
%% @doc
%% Send a test log file back up to GiddyUp
%% @end
%%--------------------------------------------------------------------

-spec(post_artifact(string(), string(), string()) -> ok | error).
post_artifact(TestPlan, Label, Filename) ->
    gen_server:cast(?MODULE, {post_artifact, TestPlan, Label, Filename}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the GiddyUp server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([Platform, Product, VersionNumber, Version, GiddyUpHost, GiddyUpUser, GiddyUpPassword]) ->
    load_and_start(ibrowse),
    {ok, #state{platform=Platform,
                default_version=Version,
                default_version_number=VersionNumber,
                default_product=Product,
                giddyup_host=GiddyUpHost,
                giddyup_user=GiddyUpUser,
                giddyup_password=GiddyUpPassword,
                artifact_base = dict:new()}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(get_test_plans, _From, State) ->
    TestPlans = fetch_all_test_plans(State#state.platform, State#state.default_product, State#state.default_version_number, State#state.default_version, State#state.giddyup_host),
    {reply, TestPlans, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({post_result, TestPlan, TestResult}, State) ->
    {ok, Location} = post_result(TestPlan, TestResult, State#state.giddyup_host, State#state.giddyup_user, State#state.giddyup_password),
    Dictionary = State#state.artifact_base,
    %% Store the Base URL in a dictionary keyed off the test name
    {noreply, State#state{artifact_base=dict:store(rt_test_plan:get_name(TestPlan), Location, Dictionary)}};
handle_cast({post_artifact, TestPlan, Label, Filename}, State) ->
    BaseURL = dict:fetch(rt_test_plan:get_name(TestPlan), State#state.artifact_base),
    post_artifact(BaseURL, Label, Filename, State#state.giddyup_user, State#state.giddyup_password),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get the entire test suite from Giddyup in an `rt_test_plan' list
%% @end
%%--------------------------------------------------------------------

-spec fetch_all_test_plans(string(), string(), string(), string(), string()) -> [rt_test_plan:test_plan()].
fetch_all_test_plans(Platform, Product, VersionNumber, DefaultVersion, Host) ->
    %% Make sure ibrowse is up and running
    rt:check_ibrowse(),
    Schema = get_schema(Platform, Product, VersionNumber, Host),
    Project = kvc:path('project.name', Schema),
    lager:info("Retrieved Project: ~s", [Project]),
    Tests = kvc:path('project.tests', Schema),
    TestProps  =
        fun(Test) ->
            Id = kvc:path(id, Test),
            Module = binary_to_atom(kvc:path(name, Test), utf8),
            Plan0 = rt_test_plan:new([{id, Id}, {module, Module}, {project, Project}, {platform, Platform}, {version, VersionNumber}]),
            {ok, Plan1} = case kvc:path('tags.backend', Test) of
                          [] -> {ok, Plan0};
                          Backend -> rt_test_plan:set(backend, binary_to_atom(Backend, utf8), Plan0)
                      end,
            {ok, Plan2} = case kvc:path('tags.upgrade_version', Test) of
                [] -> {ok, Plan1};
                UpgradeVsn ->
                    UpgradeVersion = case UpgradeVsn of
                                         <<"legacy">> -> rt_config:get_legacy_version();
                                         <<"previous">> -> rt_config:get_previous_version();
                                         _ -> rt_config:get_version(binary_to_list(UpgradeVsn))
                                     end,
                    rt_test_plan:set(upgrade_path, [UpgradeVersion, DefaultVersion], Plan1)
            end,
            %% TODO: Remove? No tests currently use this multi_config setting
            %% Plan3 = case kvc:path('tags.multi_config', Test) of
            %%    [] -> Plan1;
            %%    MultiConfig -> rt_test_plan:set(multi_config, binary_to_atom(MultiConfig, utf8), Plan2)
            %%end,
            lager:debug("Giddyup Module ~p using TestPlan ~p", [Module, Plan2]),
            Plan2
        end,
    [ TestProps(Test) || Test <- Tests].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get the GiddyUp Schema in JSON format (decoded to Erlang terms)
%% Retry up to 3 times.
%% @end
%%--------------------------------------------------------------------

-spec(get_schema(atom(), string(), string(), string()) -> term()).
get_schema(Platform, Product, VersionNumber, Host) ->
    get_schema(Platform, Product, VersionNumber, Host, 3).

get_schema(Platform, Product, VersionNumber, Host, Retries) ->
    URL = lists:flatten(io_lib:format("http://~s/projects/~s?platform=~s&version=~s", [Host, Product, Platform, VersionNumber])),
    lager:info("giddyup url: ~s", [URL]),

    rt:check_ibrowse(),
    case {Retries, ibrowse:send_req(URL, [], get, [], [])} of
        {_, {ok, "200", _Headers, JSON}} -> mochijson2:decode(JSON);
        {0, Error} ->
            lager:error("GiddyUp GET failed: ~p", [Error]),
            exit(1);
        {_, Error} ->
            lager:warning("GiddyUp GET failed: ~p", [Error]),
            lager:warning("GiddyUp trying ~p more times", [Retries]),
            timer:sleep(60000),
            get_schema(Platform, Product, VersionNumber, Host, Retries - 1)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Send a test result back up to GiddyUp
%% @end
%%--------------------------------------------------------------------

-spec post_result(rt_test_plan:test_plan(), pass | {fail, string()}, string(), string(), string()) -> {ok, string()} | error.
post_result(TestPlan, TestResult, Host, User, Password) ->
    URL = "http://" ++ Host ++ "/test_results",
    lager:info("giddyup url: ~s", [URL]),
    rt:check_ibrowse(),
    BasicAuth = {basic_auth, {User, Password}},
    case post_result(TestPlan, TestResult, #rt_webhook{name="GiddyUp", url=URL, headers=[BasicAuth]}) of
        {ok, RC, Headers} ->
            {_, Location} = lists:keyfind("Location", 1, Headers),
            lager:info("Test Result successfully POSTed to GiddyUp! ResponseCode: ~s, URL: ~s", [RC, Location]),
            {ok, Location};
        error ->
            error
    end.

-spec(post_result(rt_test_plan:test_plan(), pass | {fail, string()}, term()) -> {ok, integer(), [string()]} | error).
post_result(TestPlan, TestResult, #rt_webhook{url=URL, headers=HookHeaders, name=Name}) ->
    Status = case TestResult of
                 pass -> pass;
                 _ -> fail
             end,
    GiddyupResult = [
        {test, rt_test_plan:get_module(TestPlan)},
        {status, Status},
        {backend, rt_test_plan:get(backend, TestPlan)},
        {id, rt_test_plan:get(id, TestPlan)},
        {platform, rt_test_plan:get(platform, TestPlan)},
        {version, rt_test_plan:get(version, TestPlan)},
        {project, rt_test_plan:get(project, TestPlan)}
    ],
    try ibrowse:send_req(URL,
                        [{"Content-Type", "application/json"}],
                        post,
                        mochijson2:encode(GiddyupResult),
                        [{content_type, "application/json"}] ++ HookHeaders,
                        300000) of  %% 5 minute timeout

        {ok, RC=[$2|_], Headers, _Body} ->
            {ok, RC, Headers};
        {ok, ResponseCode, Headers, Body} ->
            lager:info("Test Result did not generate the expected 2XX HTTP response code."),
            lager:debug("Post"),
            lager:debug("Response Code: ~p", [ResponseCode]),
            lager:debug("Headers: ~p", [Headers]),
            lager:debug("Body: ~p", [Body]),
            error;
        X ->
            lager:warning("Some error POSTing test result: ~p", [X]),
            error
    catch
        Class:Reason ->
            lager:error("Error reporting to ~s. ~p:~p", [Name, Class, Reason]),
            lager:error("Payload: ~p", [GiddyupResult]),
            error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Send a test log file back up to GiddyUp
%% @end
%%--------------------------------------------------------------------

-spec(post_artifact(string(), string(), string(), string(), string()) -> ok | error).
post_artifact(BaseURL, Label, Filename, User, Password) ->
    %% First compute the path of where to post the artifact
    URL = artifact_url(BaseURL, Label),
    {ok, Body} = file:open(Filename, [read, binary]),
    ReqBody = make_req_body(Body),
    CType = guess_ctype(Label),
    BasicAuth = {basic_auth, {User, Password}},

    %% Send request
    try ibrowse:send_req(URL, [{"Content-Type", CType}],
        post,
        ReqBody,
        [{content_type, CType}, BasicAuth],
        300000) of
        {ok, [$2|_], Headers, _Body} ->
            {_, Location} = lists:keyfind("Location", 1, Headers),
            lager:info("Successfully uploaded test artifact ~s to GiddyUp! URL: ~s", [Label, Location]),
            ok;
        {ok, RC, Headers, Body} ->
            lager:info("Test artifact ~s failed to upload!", [Label]),
            lager:debug("Status: ~p~nHeaders: ~p~nBody: ~s~n", [RC, Headers, Body]),
            error;
        X ->
            lager:error("Error uploading ~s to giddyup. ~p~n"
            "URL: ~p~nRequest Body: ~p~nContent Type: ~p~n",
                [Label, X, URL, ReqBody, CType]),
            error
    catch
        Throws ->
            lager:error("Error uploading ~s to giddyup. ~p~n"
            "URL: ~p~nRequest Body: ~p~nContent Type: ~p~n",
                [Label, Throws, URL, ReqBody, CType])
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Given a URI parsed by http_uri, reconstitute it.
%% @end
%%--------------------------------------------------------------------

generate_uri({_Scheme, _UserInfo, _Host, _Port, _Path, _Query}=URI) ->
    generate_uri(URI, http_uri:scheme_defaults()).

generate_uri({Scheme, UserInfo, Host, Port, Path, Query}, SchemeDefaults) ->
    {Scheme, DefaultPort} = lists:keyfind(Scheme, 1, SchemeDefaults),
    lists:flatten([
                   [ atom_to_list(Scheme), "://" ],
                   [ [UserInfo, "@"] || UserInfo /= [] ],
                   Host,
                   [ [$:, integer_to_list(Port)] || Port /= DefaultPort ],
                   Path, Query
                  ]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Given the test result URL, constructs the appropriate URL for the artifact.
%% @end
%%--------------------------------------------------------------------

artifact_url(BaseURL, FName) ->
    {ok, {Scheme, UserInfo, Host, Port, Path, Query}} = http_uri:parse(BaseURL),
    ArtifactPath = filename:join([Path, "artifacts", FName]),
    generate_uri({Scheme, UserInfo, Host, Port, ArtifactPath, Query}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% ibrowse support streaming request bodies, so in the case where we
%% have a Port/File to read from, we should stream it.
%% @end
%%--------------------------------------------------------------------

make_req_body(Body) when is_port(Body); is_pid(Body) ->
    read_fully(Body);
make_req_body(Body) when is_list(Body); is_binary(Body) ->
    Body.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Read the file/port fully until eof. This is a workaround for the
%% fact that ibrowse doesn't seem to send file streams correctly, or
%% giddyup dislikes them. (shrug)
%% @end
%%--------------------------------------------------------------------

-spec(read_fully(string() | port()) -> binary()).
read_fully(File) ->
    read_fully(File, <<>>).

-spec(read_fully(string() | port(), binary()) -> binary()).
read_fully(File, Data0) ->
    case file:read(File, ?STREAM_CHUNK_SIZE) of
        {ok, Data} ->
            read_fully(File, <<Data0/binary, Data/binary>>);
        eof ->
            Data0
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Guesses the MIME type of the file being uploaded.
%% @end
%%--------------------------------------------------------------------

-spec(guess_ctype(string()) -> string()).
guess_ctype(FName) ->
    case string:tokens(filename:basename(FName), ".") of
        [_, "log"|_] -> "text/plain"; %% console.log, erlang.log.5, etc
        [_, "conf"|_] -> "text/plain"; %% riak.conf
        [_, "config"|_] -> "text/plain"; %% advanced.config, etc
        ["erl_crash", "dump"] -> "text/plain"; %% An erl_crash.dump file
        [_, Else] ->
            case mochiweb_mime:from_extension(Else) of
                undefined -> "binary/octet-stream";
                CTG -> CTG
            end;
        _ -> "binary/octet-stream"
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verify that an application is running
%% @end
%%--------------------------------------------------------------------

load_and_start(Application) ->
    application:load(Application),
    application:start(Application).