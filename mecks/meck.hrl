%% @doc Convert `MeckName' into module name it is mecking.  Assumes
%%      that `MeckName' ends with `_mecks'
mod_to_meck(MeckName) ->
    S = atom_to_list(MeckName),
    list_to_atom(string:substr(S, 1, length(S) - 6)).

%% The name of the module being mecked.  E.g. foo_bar_mecks => foo_bar
-define(M_MOD, mod_to_meck(?MODULE)).

-define(M_MOD_ORIG, list_to_atom(atom_to_list(mod_to_meck(?MODULE)) ++ "_meck_original")).

-define(M_TAG(S), "MECK: " ++ S).
-define(M_INFO(Msg), error_logger:info_msg(?M_TAG(Msg))).
-define(M_INFO(Msg, Args), error_logger:info_msg(?M_TAG(Msg), Args)).
-define(M_ERROR(Msg), error_logger:error_msg(?M_TAG(Msg))).
-define(M_ERROR(Msg, Args), error_logger:error_msg(?M_TAG(Msg), Args)).

%% A helper to define an expect against the module being mecked.  This
%% saves you from having to type out the module name by hand for every
%% meck.
-define(M_EXPECT(FunName, Body),
        meck:expect(mod_to_meck(?MODULE), FunName, Body)).

init() ->
    case code:is_loaded(?M_MOD_ORIG) of
        false ->
            ?M_INFO("mecking module ~p", [?M_MOD]),
            meck:new(?M_MOD, [passthrough, unstick, no_link]),
            ok;
        {file,_} ->
            ok
    end.
