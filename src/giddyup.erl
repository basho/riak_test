-module(giddyup).

-export([get_suite/1, post_result/1]).

get_suite(Platform) ->
    Schema = get_schema(Platform),
    Proj = kvc:path(project, Schema),
    Name = kvc:path(name, Proj),
    lager:info("Retrieved Project: ~s", [Name]),
    Tests = kvc:path(tests, Proj),
    [ {
        binary_to_atom(kvc:path(name, Test), utf8),
        [
            {id, kvc:path(id, Test)},
            {backend, case kvc:path(tags.backend, Test) of [] -> undefined; X -> binary_to_atom(X, utf8) end},
            {platform, list_to_binary(Platform)}
        ]
      } || Test <- Tests].
    
get_schema(Platform) ->
    Host = rt:config(rt_giddyup_host),
    Project = rt:config(rt_project),
    URL = "http://" ++ Host ++ "/projects/" ++ Project ++ "?platform=" ++ Platform,
    lager:info("giddyup url: ~s", [URL]),
    
    case ibrowse:send_req(URL, [], get, [], [ {basic_auth, {"basho", "basho"}}]) of
        {ok, "200", _Headers, JSON} -> mochijson2:decode(JSON);
        _ -> []
    end.

post_result(TestResult) ->
    Host = rt:config(rt_giddyup_host),
    URL = "http://" ++ Host ++ "/test_results",
    lager:info("giddyup url: ~s", [URL]),
    case ibrowse:send_req(URL, [], post, mochijson2:encode(TestResult), [ {content_type, "application/json"}, {basic_auth, {"basho", "basho"}}]) of
        %%{ok, "200", _Headers, JSON} -> mochijson2:decode(JSON);
        %%_ -> []
        {ok, _ResponseCode, _Headers, _Body} ->
            %% lager:info("Post"),
            %% lager:info("Response Code: ~p", [ResponseCode]),
            %% lager:info("Headers: ~p", [Headers]),
            %% lager:info("Body: ~p", [Body]);
            ok;
        X -> lager:warning("X: ~p", [X])
    end.
