
-module(rt_worker_sup).

-behavior(supervisor).

%% Helper macro for declaring children of supervisor
-define(CHILD(Id, Mod, Node), { 
    list_to_atom(atom_to_list(Node) ++ "_loader_" ++ integer_to_list(Id)), 
    {Mod, start_link, [list_to_atom(atom_to_list(Node) ++ "_loader_" ++ integer_to_list(Id)), Node]}, permanent, 5000, worker, [Mod]}).

-export([init/1]).
-export([start_link/1]).

start_link(Props) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Props).

init(Props) ->
    _WorkersPerNode = proplists:get_value(concurrent, Props),
    Nodes = proplists:get_value(nodes, Props),

    ChildSpecs = [
        ?CHILD(1, loaded_upgrade_worker, Node)
    || Node <- Nodes],

    lager:debug("About to go all supervisor on ~p~n", [ChildSpecs]),

    {ok, {{one_for_one, 5, 60}, ChildSpecs}}.