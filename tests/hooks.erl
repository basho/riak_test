%%
%% Pre/post commit hooks for testing
%%
-module(hooks).
-compile([export_all]).

precommit_nop(Obj) ->
    Obj.

precommit_fail(_Obj) ->
    fail.

precommit_failatom(_Obj) ->
    {fail, on_purpose}.

precommit_failstr(_Obj) ->
    {fail, "on purpose"}.

precommit_failbin(_Obj) ->
    {fail, <<"on purpose">>}.

precommit_failkey(Obj) ->
    case riak_object:key(Obj) of
        <<"fail">> ->
            fail;
        _ ->
            Obj
    end.


set_precommit(Bucket, Hook) when is_atom(Hook) ->
    set_precommit(Bucket, atom_to_binary(Hook, latin1));
set_precommit(Bucket, Hook) when is_list(Hook) ->
    set_precommit(Bucket, list_to_binary(Hook));
set_precommit(Bucket, Hook) ->
    {ok,C} = riak:local_client(),
    C:set_bucket(Bucket,
                 [{precommit, [{struct,[{<<"mod">>,<<"hooks">>},
                                        {<<"fun">>,Hook}]}]}]).
set_hooks() ->
    set_precommit(),
    set_postcommit().

set_precommit() ->
    hooks:set_precommit(<<"failatom">>,precommit_failatom),
    hooks:set_precommit(<<"failstr">>,precommit_failstr),
    hooks:set_precommit(<<"failbin">>,precommit_failbin),
    hooks:set_precommit(<<"failkey">>,precommit_failkey).

set_postcommit() ->
    {ok, C} = riak:local_client(),
    C:set_bucket(<<"fail">>,[{postcommit, [{struct,[{<<"mod">>,<<"hooks">>},{<<"fun">>, <<"postcommit_log">>}]}]}]).

postcommit_log(Obj) ->
    Bucket = riak_object:bucket(Obj),
    Key = riak_object:key(Obj),
    File = binary_to_list(<<"/tmp/", Bucket/binary, "_", Key/binary>>),
    Str = lists:flatten(io_lib:format("~p\n", [Obj])),
    file:write_file(File, Str).
