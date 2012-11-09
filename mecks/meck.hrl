%% @doc Convert `MeckName' into module name it is mecking.  Assumes
%%      that `MeckName' ends with `_mecks'
mod_to_meck(MeckName) ->
    S = atom_to_list(MeckName),
    list_to_atom(string:substr(S, 1, length(S) - 6)).

%% The name of the module being mecked.  E.g. foo_bar_mecks => foo_bar
-define(M_MOD, mod_to_meck(?MODULE)).

%% A helper to define an expect against the module being mecked.  This
%% saves you from having to type out the module name by hand for every
%% meck.
-define(M_EXPECT(FunName, Body),
        meck:expect(mod_to_meck(?MODULE), FunName, Body)).
