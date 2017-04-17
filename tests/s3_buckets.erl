-module(s3_buckets).

-behaviour(riak_test).

-export([confirm/0]).

-define(BUCKET, <<"bucket">>).
-define(USERNAME, "user").

confirm() ->
    Cluster = [Node1, _Node2] = rt:build_cluster(2),
    create_bucket(Node1),
    lists:foreach(fun (Node) ->
                          rt:wait_until(fun () -> verify_bucket(Node) end)
                  end, Cluster),
    pass.

create_bucket(Node) ->
    rpc:call(Node, riak_s3_bucket, create, [?BUCKET, ?USERNAME]).

verify_bucket(Node) ->
    case rpc:call(Node, riak_s3_bucket, get, [?BUCKET]) of
        not_found ->
            not_found;
        {error, Reason} ->
            Reason;
        Bucket ->
            BucketName = riak_s3_bucket:get_name(Bucket),
            User = riak_s3_bucket:get_username(Bucket),
            if BucketName == ?BUCKET andalso  User == ?USERNAME ->
                    true;
               BucketName /= ?BUCKET ->
                    {incorrect_bucket_name, BucketName};
               User /= ?USERNAME ->
                    {incorrect_user, User}
            end
    end.
                          
