-module(s3_api_gradle).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
    confirm/0
]).

-define(NODE_COUNT, 3).
-define(TEST_USER, ["test", "test@example.com"]).
-define(METADATA_KEY, [{<<"security">>, <<"users">>}, <<"test">>]).
-define(S3_ACCESS_KEY, "s3.access_key_id").
-define(S3_SECRET_KEY, "s3.secret_access_key").

confirm() ->
    %% Build a simple cluster
    rt:set_backend(eleveldb),
    Nodes = rt:build_cluster(?NODE_COUNT),

    %% Choose random node to interact with
    Node = lists:nth(random:uniform(?NODE_COUNT), Nodes),

    %% Create test user and wait for it to appear on the cluster
    _ = rpc:call(Node, riak_s3_user, create, ?TEST_USER),
    rt:wait_until(fun() ->
                        {Results, []} = rpc:multicall(Nodes, riak_core_metadata, get, ?METADATA_KEY),
                        lists:all(fun(R) -> R =/= undefined end, Results)
                  end),
    S3User = rpc:call(Node, riak_core_metadata, get, ?METADATA_KEY),
    {?S3_ACCESS_KEY, S3AccessKey} = lists:keyfind(?S3_ACCESS_KEY, 1, S3User),
    {?S3_SECRET_KEY, S3SecretKey} = lists:keyfind(?S3_SECRET_KEY, 1, S3User),

    %% Get S3 URLs
    URLs = rt:s3_url(Nodes),
    S3_URL = lists:nth(random:uniform(?NODE_COUNT), URLs),

    %% Run Gradle-based tests
    Args = [
        S3AccessKey,
        S3SecretKey,
        S3_URL
    ],
    Cmd = lists:flatten(io_lib:format(
        "cd ../s3-api-tests"
        " && "
        "./gradlew -Daws.accessKeyId=~s -Daws.secretKey=~s -Ds3.api.baseUrl=~s test --stacktrace --info",
        Args)),
    lager:info(Cmd),
    Output = os:cmd(Cmd),
    Lines0 = string:tokens(Output, "\n"),

    %% Log Gradle output
    [lager:info(O) || O <- Lines0],

    %% Check output for lines that end in 'FAILED'
    Lines1 = lists:filter(fun("DELIAF" ++ _) -> true;
                             (_) -> false
                          end,
                          [lists:reverse(L) || L <- Lines0]),

    case length(Lines1) of
        0 -> pass;
        _ -> gradle_tests_failed
    end.
