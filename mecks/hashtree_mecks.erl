-module(hashtree_mecks).
-compile(export_all).

%% @doc Delay the completion of hashtree:update_perform.
update_perform_sleep() ->
    update_perform_sleep(60).

update_perform_sleep(Seconds) ->
    MS = timer:seconds(Seconds),
    meck:new(hashtree, [passthrough]),
    meck:expect(hashtree, update_perform,
                fun(X) ->
                        timer:sleep(MS),
                        meck:passthrough([X])
                end).
