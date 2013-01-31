
-module(rt_worker_sup).

-behavior(supervisor).

%% Helper macro for declaring children of supervisor
-define(CHILD(Id, Mod, Node, Backend), { 
    list_to_atom(atom_to_list(Node) ++ "_loader_" ++ integer_to_list(Id)), 
    {   Mod, 
        start_link, 
        [list_to_atom(atom_to_list(Node) ++ "_loader_" ++ integer_to_list(Id)), Node, Backend]}, 
        permanent, 5000, worker, [Mod]}).

-export([init/1]).
-export([start_link/1]).

start_link(Props) ->
    supervisor:start_link(?MODULE, Props).

init(Props) ->
    WorkersPerNode = proplists:get_value(concurrent, Props),
    Node = proplists:get_value(node, Props),
    Backend = proplists:get_value(backend, Props),

    ChildSpecs = [
        ?CHILD(Num, loaded_upgrade_worker, Node, Backend)
    || Num <- lists:seq(1, WorkersPerNode)],

    lager:debug("About to go all supervisor on ~p~n", [ChildSpecs]),

    {ok, {{one_for_one, 5, 60}, ChildSpecs}}.