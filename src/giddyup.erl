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
-module(giddyup).

-export([get_suite/1, post_result/1, post_artifact/2]).
-define(STREAM_CHUNK_SIZE, 8192).

-record(rt_webhook, {
    name :: string(),
    url :: string(),
    headers=[] :: [{atom(), string()}]
}).

-spec get_suite(string()) -> [{atom(), term()}].
get_suite(Platform) ->
    Schema = get_schema(Platform),
    Name = kvc:path('project.name', Schema),
    Version = rt_config:get_default_version_number(),
    lager:info("Retrieved Project: ~s", [Name]),
    Tests = kvc:path('project.tests', Schema),
    TestProps  =
        fun(Test) ->
            [
                {id, kvc:path(id, Test)},
                {backend,
                 case kvc:path('tags.backend', Test) of
                     [] -> undefined;
                     X -> binary_to_atom(X, utf8)
                 end},
                {platform, list_to_binary(Platform)},
                {version, Version},
                {project, Name}
            ] ++
            case kvc:path('tags.upgrade_version', Test) of
                [] -> [];
                UpgradeVsn -> [{upgrade_version, binary_to_atom(UpgradeVsn, utf8)}]
            end ++
            case kvc:path('tags.multi_config', Test) of
                [] -> [];
                MultiConfig -> [{multi_config, binary_to_atom(MultiConfig, utf8)}]
            end
        end,
    [ { binary_to_atom(kvc:path(name, Test), utf8), TestProps(Test) } || Test <- Tests].

get_schema(Platform) ->
    get_schema(Platform, 3).

get_schema(Platform, Retries) ->
    Host = rt_config:get(giddyup_host),
    Project = rt_config:get_default_version_product(),
    Version = rt_config:get_default_version_number(),
    URL = lists:flatten(io_lib:format("http://~s/projects/~s?platform=~s&version=~s", [Host, Project, Platform, Version])),
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
            get_schema(Platform, Retries - 1)
    end.

-spec post_result([{atom(), term()}]) -> {ok, string()} | error.
post_result(TestResult) ->
    Host = rt_config:get(giddyup_host),
    URL = "http://" ++ Host ++ "/test_results",
    lager:info("giddyup url: ~s", [URL]),
    rt:check_ibrowse(),
    case post_result(TestResult, #rt_webhook{name="GiddyUp", url=URL, headers=[basic_auth()]}) of
        {ok, RC, Headers} ->
            {_, Location} = lists:keyfind("Location", 1, Headers),
            lager:info("Test Result successfully POSTed to GiddyUp! ResponseCode: ~s, URL: ~s", [RC, Location]),
            {ok, Location};
        error ->
            error
    end.

-spec(post_result(proplists:proplist(), term()) -> tuple()|error).
post_result(TestResult, #rt_webhook{url=URL, headers=HookHeaders, name=Name}) ->
    try ibrowse:send_req(URL,
        [{"Content-Type", "application/json"}],
        post,
        mochijson2:encode(TestResult),
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
            lager:error("Payload: ~p", [TestResult]),
            error
    end.

post_artifact(TRURL, {FName, Body}) ->
    %% First compute the path of where to post the artifact
    URL = artifact_url(TRURL, FName),
    ReqBody = make_req_body(Body),
    CType = guess_ctype(FName),
    %% Send request
    try ibrowse:send_req(URL, [{"Content-Type", CType}],
                         post,
                         ReqBody,
                         [{content_type, CType}, basic_auth()],
                         300000) of
        {ok, [$2|_], Headers, _Body} ->
            {_, Location} = lists:keyfind("Location", 1, Headers),
            lager:info("Successfully uploaded test artifact ~s to GiddyUp! URL: ~s", [FName, Location]),
            ok;
        {ok, RC, Headers, Body} ->
            lager:info("Test artifact ~s failed to upload!", [FName]),
            lager:debug("Status: ~p~nHeaders: ~p~nBody: ~s~n", [RC, Headers, Body]),
            error;
        X ->
            lager:error("Error uploading ~s to giddyup. ~p~n"
                        "URL: ~p~nRequest Body: ~p~nContent Type: ~p~n",
                        [FName, X, URL, ReqBody, CType]),
            error
    catch
        Throws ->
            lager:error("Error uploading ~s to giddyup. ~p~n"
                        "URL: ~p~nRequest Body: ~p~nContent Type: ~p~n",
                        [FName, Throws, URL, ReqBody, CType])
    end.


basic_auth() ->
    {basic_auth, {rt_config:get(giddyup_user), rt_config:get(giddyup_password)}}.

%% Given a URI parsed by http_uri, reconstitute it.
generate({_Scheme, _UserInfo, _Host, _Port, _Path, _Query}=URI) ->
    generate(URI, http_uri:scheme_defaults()).

generate({Scheme, UserInfo, Host, Port, Path, Query}, SchemeDefaults) ->
    {Scheme, DefaultPort} = lists:keyfind(Scheme, 1, SchemeDefaults),
    lists:flatten([
                   [ atom_to_list(Scheme), "://" ],
                   [ [UserInfo, "@"] || UserInfo /= [] ],
                   Host,
                   [ [$:, integer_to_list(Port)] || Port /= DefaultPort ],
                   Path, Query
                  ]).

%% Given the test result URL, constructs the appropriate URL for the artifact.
artifact_url(TRURL, FName) ->
    {ok, {Scheme, UserInfo, Host, Port, Path, Query}} = http_uri:parse(TRURL),
    ArtifactPath = filename:join([Path, "artifacts", FName]),
    generate({Scheme, UserInfo, Host, Port, ArtifactPath, Query}).

%% ibrowse support streaming request bodies, so in the case where we
%% have a Port/File to read from, we should stream it.
make_req_body(Body) when is_port(Body); is_pid(Body) ->
    read_fully(Body);
make_req_body(Body) when is_list(Body);
                         is_binary(Body) ->
    Body.

%% Read the file/port fully until eof. This is a workaround for the
%% fact that ibrowse doesn't seem to send file streams correctly, or
%% giddyup dislikes them. (shrug)
read_fully(File) ->
    read_fully(File, <<>>).

read_fully(File, Data0) ->
    case file:read(File, ?STREAM_CHUNK_SIZE) of
        {ok, Data} ->
            read_fully(File, <<Data0/binary, Data/binary>>);
        eof ->
            Data0
    end.

%% Guesses the MIME type of the file being uploaded.
guess_ctype(FName) ->
    case string:tokens(filename:basename(FName), ".") of
        [_, "log"|_] -> "text/plain"; %% console.log, erlang.log.5, etc
        ["erl_crash", "dump"] -> "text/plain"; %% An erl_crash.dump file
        [_, Else] ->
            case mochiweb_mime:from_extension(Else) of
                undefined -> "binary/octet-stream";
                CTG -> CTG
            end;
        _ -> "binary/octet-stream"
    end.
