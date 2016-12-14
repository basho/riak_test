-module(s3_api_gradle).
-include_lib("eunit/include/eunit.hrl").

-behavior(riak_test).
-export([
    confirm/0
]).

-define(NODE_COUNT, 3).

confirm() ->
    Nodes = rt:deploy_nodes(?NODE_COUNT),
    ok = rt:wait_until_nodes_ready(Nodes),

    %% Get S3 URLs
    URLs = rt:s3_url(Nodes),
    S3_URL = lists:nth(random:uniform(?NODE_COUNT), URLs),

    %% Run Gradle-based tests
    Cmd = lists:flatten([
                            "cd ../s3-api-tests",
                            " && ",
                            io_lib:format("./gradlew -Ds3.api.baseUrl=~s s3ApiTests --info", [S3_URL])
                        ]),
    lager:info(Cmd),
    Output = os:cmd(Cmd),
    Lines0 = string:tokens(Output, "\n"),

    %% Log Gradle output
    [lager:info(O) || O <- Lines0],

    %% Check output for lines that end in 'FAILED'
    Lines1 = lists:filter(fun ("DELIAF" ++ _) -> true;
                              (_) -> false
                          end,
                          [lists:reverse(L) || L <- Lines0]),

    case length(Lines1) of
        0 -> pass;
        _ -> gradle_tests_failed
    end.
