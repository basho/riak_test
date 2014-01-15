%% @doc Put some bugs on the reduced N mutators. This allows one to
%% capture and inspect the objects actually set to the mutate_put and
%% mutate_get funcitons. This module is currently used by
%% repl_reduced:read_repair_interaction test in that very capacity.
%%
%% A pid registers as the report target. The interceptors send the objects,
%% node, and pid a mutator is running under to `reduced_intercept_target'
%% on the node `riak_test@127.0.0.1'.
%%
%% For best results, install desired intercepts on all nodes.
-module(riak_repl_reduced_intercepts).

-include("intercept.hrl").
-compile([{parse_transform, lager_transform}]).

-export([recv_get_report/0, recv_get_report/1, report_mutate_get/1,
    register_as_target/0, get_all_reports/0, get_all_reports/1]).
-export([report_mutate_put/5, recv_put_report/0, recv_put_report/1,
    put_all_reports/0, put_all_reports/1]).
-export([tag_mutate_get/1, tag_mutate_put/5, is_get_tagged/1,
    is_put_tagged/1]).

-define(M, riak_repl_reduced_orig).

%% @doc Intercept a mutate_get of reduced repl to add a marker to the
%% object's user metadata indicating the intercept has happened.
tag_mutate_get(InObject) ->
    case ?M:mutate_get_orig(InObject) of
        notfound ->
            notfound;
        MidObject ->
            Contents = riak_object:get_contents(MidObject),
            TaggedContents = lists:map(fun({Meta, Value}) ->
                UserMeta = case dict:find(<<"X-Riak-Meta">>, Meta) of
                    error -> [];
                    {ok, UM} -> UM
                end,
                NewUserMeta = lists:keystore(<<"X-Riak-Meta-Tag-Mutate-Get">>, 1, UserMeta, {<<"X-Riak-Meta-Tag-Mutate-Get">>, <<"true">>}),
                NewMeta = dict:store(<<"X-Riak-Meta">>, NewUserMeta, Meta),
                {NewMeta, Value}
            end, Contents),
            riak_object:set_contents(InObject, TaggedContents)
    end.

%% @doc Intercept a mutate_put of reduced repl to add a marker to the
%% object's user metadata indicating the intercept has happened.
tag_mutate_put(InMeta, InVal, RevMeta, Obj, Props) ->
    {MidMeta, MidVal, MidRevMeta} = ?M:mutate_put_orig(InMeta, InVal, RevMeta, Obj, Props),
    UserMeta = case dict:find(<<"X-Riak-Meta">>, MidMeta) of
        error -> [];
        {ok, UM} -> UM
    end,
    NewUserMeta = lists:keystore(<<"X-Riak-Meta-Tag-Mutate-Put">>, 1, UserMeta, {<<"X-Riak-Meta-Tag-Mutate-Put">>, <<"true">>}),
    OutMeta = dict:store(<<"X-Riak-Meta">>, NewUserMeta, MidMeta),
    OutRev = dict:store(<<"X-Riak-Meta">>, NewUserMeta, MidRevMeta),
    {OutMeta, MidVal, OutRev}.

%% @doc Is the user metadata tagged with a mutate_get intercept?
is_get_tagged(RiakcObj) ->
    is_tagged(RiakcObj, <<"X-Riak-Meta-Tag-Mutate-Get">>).

%% @doc Is the user metadat tagged with a mutate_put intercept?
is_put_tagged(RiakcObj) ->
    is_tagged(RiakcObj, <<"X-Riak-Meta-Tag-Mutate-Put">>).

is_tagged(RiakcObj, TagKey) ->
    Metas = riakc_obj:get_metadatas(RiakcObj),
    lists:all(fun(Meta) ->
        GotValue = riakc_obj:get_user_metadata_entry(Meta, TagKey),
        %lager:info("Der usermeta: ~p", [UserMeta]),
        %GotValue = riakc_obj:get_user_metadata_entry(UserMeta, TagKey),
        GotValue == <<"true">>
    end, Metas).

%% @doc Intercept a mutate_get of reduced repl. When adding as intercept,
%% use `{{mutate_get, 1}, report_mutate_get}'. For best results, the
%% pid to get the reports should have already called register_as_target/0.
%% The report has the Node and Pid calling the mutator, as well as the
%% object passed in.
report_mutate_get(InObject) ->
    Node = node(),
    Pid = self(),
    TargetNode = 'riak_test@127.0.0.1',
    TargetProcess = reduced_intercept_target,
    {TargetProcess, TargetNode} ! {report_mutate_get, Node, Pid, InObject},
    ?M:mutate_get_orig(InObject).

%% @doc Intercepts a mutate_put of reduced repl. When adding as intercept
%% use `{{mutate_put, 5}, report_mutate_put}'. For best results, the
%% pid to get the reports should have already called register_as_target/0.
%% The report has the Node and Pid calling the mutator, as well as the
%% specific metadata and value to be processed.
report_mutate_put(InMeta, InVal, RevMeta, Obj, Props) ->
    Node = node(),
    Pid = self(),
    TargetNode = 'riak_test@127.0.0.1',
    TargetProcess = reduced_intercept_target,
    {TargetProcess, TargetNode} ! {report_mutate_put, Node, Pid, InMeta, InVal},
    ?M:mutate_put_orig(InMeta, InVal, RevMeta, Obj, Props).

%% @doc Registers the calling process with the correct name to recieve
%% messages from report_muatet_get and report_mutate_put. If there was
%% already a target registered, it is unregistered first.
register_as_target() ->
    Self = self(),
    case whereis(reduced_intercept_target) of
        Self ->
            true;
        undefined ->
            register(reduced_intercept_target, Self);
        _NotSelf ->
            unregister(reduced_intercept_target),
            register(reduced_intercept_target, Self)
    end.

%% @doc recv_get_report/1 with a timeout of 5 seconds.
recv_get_report() ->
    recv_get_report(5000).

%% @doc Receive a single messages sent to this calling process by
%% report_mutate_get/1 with a timeout of ms. Should be called by the same
%% process that called register_as_target/0.
recv_get_report(Timeout) ->
    receive
        {report_mutate_get, Node, Pid, InObject} ->
            {Node, Pid, InObject}
    after Timeout ->
        {error, timeout}
    end.

%% @doc same as get_all_reports/1 with a timeout of 5 seconds.
get_all_reports() ->
    get_all_reports(5000).

%% @doc Repeated call recv_get_report/1 with the given timeout of ms until
%% there is a timeout. Returns the results in the order gotten.
get_all_reports(Timeout) ->
    get_all_reports(Timeout, []).

get_all_reports(Timeout, Acc) ->
    case recv_get_report(Timeout) of
        {error, timeout} ->
            lists:reverse(Acc);
        Report ->
            get_all_reports(Timeout, [Report | Acc])
    end.

%% @doc recv_put_report/1 with a timeout of 5 seconds.
recv_put_report() ->
    recv_put_report(5000).

%% @doc Get a single message sent to the calling process by
%% report_mutate_put/5. Should be called by the same process that called
%% register_as_target/0.
recv_put_report(Timeout) ->
    receive
        {report_mutate_put, Node, Pid, InMeta, InVal} ->
            {Node, Pid, InMeta, InVal}
    after Timeout ->
        {error, timeout}
    end.

%% @doc put_all_reports/1 with a 5 second timeout.
put_all_reports() ->
    put_all_reports(5000).

%% @doc Keep calling recv_put_report/1 until there is a timeout. Should be
%% called by the same process that called register_as_target/0. The list
%% is returned in the order the messages were received.
put_all_reports(Timeout) ->
    put_all_reports(Timeout, []).

put_all_reports(Timeout, Acc) ->
    case recv_put_report(Timeout) of
        {error, timeout} ->
            lists:reverse(Acc);
        Report ->
            put_all_reports(Timeout, [Report | Acc])
    end.

