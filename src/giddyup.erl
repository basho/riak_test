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
    URL = "http://" ++ Host ++ "/projects/" ++ Project ++ "?platform=" ++ Platform,
    lager:info("giddyup url: ~s", [URL]),
    
    case ibrowse:send_req(URL, [], get, [], [ basic_auth()]) of
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
