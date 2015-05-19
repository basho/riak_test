-module(rt_host).
-export([behaviour_info/1,
         command_line_from_command/1,
         connect/1,
         connect/2,
         consult/2,
         copy_dir/4,
         disconnect/1,
         exec/2,
         ip_addr/1,
         hostname/1,
         kill/3,
         killall/3,
         make_temp_directory_cmd/1,
         mkdirs/2,
         mvdir/3,
         rmdir/2,
         rmdir_cmd/1,
         mvdir_cmd/2,
         temp_dir/1,
         write_file/3]).

-type command() :: {filelib:filename(), [string()]}.
-type command_result() :: {ok, string()} | rt_util:error().
-type host_id() :: pid().
-type host() :: {module(), host_id()}.
-type hostname() :: atom().

-exporttype([command/0,
             command_result/0,
             host_id/0,
             host/0, 
             hostname/0]).

behaviour_info(callbacks) ->
    [{connect, 1}, {connect, 2}, {consult, 2}, {copy_dir, 4}, {disconnect, 1}, 
     {exec, 2}, {ip_addr, 1}, {hostname, 1}, {kill, 3}, {killall, 3}, 
     {mkdirs, 2}, {mvdir, 3}, {rmdir, 2}, {temp_dir, 1}, {write_file, 3}];
behaviour_info(_) ->    
    undefined. 

-spec command_line_from_command(command()) -> string().
command_line_from_command({Command, Args}) ->
    string:join([Command] ++ Args, " ").

-spec connect(hostname()) -> {ok, host()} | rt_util:error().
connect(Hostname) ->
    connect(Hostname, []).

%% To add remote host support, add an implementation of this function that 
%% handles all atoms != localhost and create a connect_remote function to
%% startup a rt_remote_host gen_server with the appropriate configuration 
%% (e.g. user name, creds, ports, etc) read from the r_t hosts file ...
-spec connect(hostname(), proplists:proplist()) -> {ok, host()} | rt_util:error().
connect(localhost, Options) ->
    connect_localhost(rt_local_host:connect(localhost, Options));
connect(_, _) ->
    erlang:error("Remote hosts are not supported.").

-spec connect_localhost({ok, host_id()} | rt_util:error()) -> {ok, host()} | rt_util:error().
connect_localhost({ok, Pid}) ->
    {ok, {rt_local_host, Pid}};
connect_localhost(Error) ->
    Error.

-spec consult(host(), filelib:filename()) -> {ok, term()} | rt_util:error().
consult({HostModule, HostPid}, Filename) ->
    HostModule:consult(HostPid, Filename).

-spec copy_dir(host(), filelib:dirname(), filelib:dirname(), boolean()) -> command_result().
copy_dir({HostModule, HostPid}, FromDir, ToDir, Recursive) ->
    HostModule:copy_dir(HostPid, FromDir, ToDir, Recursive).

-spec disconnect(host()) -> ok.
disconnect({HostModule, HostPid}) ->
    HostModule:disconnect(HostPid).

-spec exec(host(), rt_host:command()) -> command_result().
exec({HostModule, HostPid}, Command) ->
    HostModule:exec(HostPid, Command).

-spec hostname(host()) -> rt_host:hostname().
hostname({HostModule, HostPid}) ->
    HostModule:hostname(HostPid).

-spec ip_addr(host()) -> string().
ip_addr({HostModule, HostPid}) ->
    HostModule:ip_addr(HostPid).

-spec kill(host(), pos_integer(), pos_integer()) -> rt_util:result().
kill({HostModule, HostPid}, Signal, OSPid) ->
    HostModule:kill(HostPid, Signal, OSPid).

-spec killall(host(), pos_integer(), string()) -> rt_util:result().
killall({HostModule, HostPid}, Signal, Name) ->
    HostModule:killall(HostPid, Signal, Name).

-spec make_temp_directory_cmd(string()) -> command().
make_temp_directory_cmd(Template) ->
    {"/usr/bin/mktemp", ["-d", "-t", Template]}.

-spec mkdirs(host(), filelib:dirname()) -> rt_util:result().
mkdirs({HostModule, HostPid}, Path) ->
    HostModule:mkdirs(HostPid, Path).

-spec mvdir(host(), filelib:dirname(), filelib:dirname()) -> rt_util:result().
mvdir({HostModule, HostPid}, FromDir, ToDir) ->
    HostModule:mvdir(HostPid, FromDir, ToDir).

-spec rmdir(host(), filelib:dirname()) -> rt_util:result().
rmdir({HostModule, HostPid}, Dir) ->
    HostModule:rmdir(HostPid, Dir).

-spec rmdir_cmd(filelib:dirname()) -> command().
rmdir_cmd(Dir) ->
    {"/bin/rm", ["-rf", Dir]}.

-spec mvdir_cmd(filelib:dirname(), filelib:dirname()) -> command().
mvdir_cmd(FromDir, ToDir) ->
    {"/bin/mv", [FromDir, ToDir]}.

-spec temp_dir(host()) -> filelib:dirname().
temp_dir({HostModule, HostPid}) ->
    HostModule:temp_dir(HostPid).

-spec write_file(host(), filelib:filename(), term()) -> rt_util:result().
write_file({HostModule, HostPid}, Filename, Content) ->
    HostModule:write_file(HostPid, Filename, Content).
