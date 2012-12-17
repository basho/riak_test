%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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

-export([get_suite/1, post_result/1]).

-spec get_suite(string()) -> [{atom(), term()}].
get_suite(Platform) ->
    Schema = get_schema(Platform),
    Name = kvc:path(project.name, Schema),
    lager:info("Retrieved Project: ~s", [Name]),
    Tests = kvc:path(project.tests, Schema),
    [ {
        binary_to_atom(kvc:path(name, Test), utf8),
        [
            {id, kvc:path(id, Test)},
            {backend, case kvc:path(tags.backend, Test) of [] -> undefined; X -> binary_to_atom(X, utf8) end},
            {platform, list_to_binary(Platform)},
            {version, rt:get_version()},
            {project, Name}
        ]
      } || Test <- Tests].
    
get_schema(Platform) ->
    Host = rt:config(giddyup_host),
    Project = rt:config(rt_project),
    Version = rt:get_version(),
    URL = lists:flatten(io_lib:format("http://~s/projects/~s?platform=~s&version=~s", [Host, Project, Platform, Version])),
    lager:info("giddyup url: ~s", [URL]),
    case ibrowse:send_req(URL, [], get, [], []) of
        {ok, "200", _Headers, JSON} -> mochijson2:decode(JSON);
        _ -> []
    end.

-spec post_result([{atom(), term()}]) -> atom().
post_result(TestResult) ->
    Host = rt:config(giddyup_host),
    URL = "http://" ++ Host ++ "/test_results",
    lager:info("giddyup url: ~s", [URL]),
    case ibrowse:send_req(URL, [{"Content-Type", "application/json"}], post, mochijson2:encode(TestResult), [ {content_type, "application/json"}, basic_auth()]) of

        {ok, RC=[$2|_], _Headers, _Body} ->
            lager:info("Test Result sucessfully POSTed to GiddyUp! ResponseCode: ~s", [RC]),
            ok;
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
    end.

basic_auth() ->
    {basic_auth, {rt:config(giddyup_user), rt:config(giddyup_password)}}.
