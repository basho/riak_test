-module(verify_riak_lager).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-define(UNIX_RW_R__R__, 8#100644).

confirm() ->
    lager:info("Staring a node"),
    Nodes = [Node] = rt:deploy_nodes(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    
    lager:info("Stopping that node"),
    rt:stop(Node),
    
    rt:start(Node),
    lager:info("Checking for log files"),
    
    
    {ok, LagerHandlers} = rt:rpc_get_env(Node, [{lager, handlers}]),
    
    Files = [element(1, Backend) || Backend <- proplists:get_value(lager_file_backend, LagerHandlers)],
    
    lager:info("Checking for files: ~p", [Files]),
    [?assert(rpc:call(Node, filelib, is_file, [File])) || File <- Files],
    
    FileInfos = [ FileInfo || {ok, FileInfo} <- [rpc:call(Node, file, read_file_info, [File]) || File <- Files]],
    
    [?assertEqual(?UNIX_RW_R__R__, ?UNIX_RW_R__R__ band FileInfo#file_info.mode) || FileInfo <- FileInfos],
    pass.
    