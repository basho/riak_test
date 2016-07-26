%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc rt_slave is a modified, slimmed down version of ct_slave.erl
%%      from the standard OTP common_test lib. The stock version was
%%      found to have issues related to host name resolution, detailed
%%      here: http://erlang.org/pipermail/erlang-questions/2016-February/087632.html.
%%
%% This is a stop-gap solution until a patched version of ct_slave.erl
%% appears in OTP.

%% The original copyright notice of ct_slave.erl follows.

%%--------------------------------------------------------------------
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2010-2013. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%

%%% @doc Common Test Framework functions for starting and stopping nodes for
%%% Large Scale Testing.
%%%
%%% <p>This module exports functions which are used by the Common Test Master
%%% to start and stop "slave" nodes. It is the default callback module for the
%%% <code>{init, node_start}</code> term of the Test Specification.</p>

-module(rt_slave).

-export([start/1, start/2, start/3, stop/1, stop/2]).

-record(options, {username, password, boot_timeout, init_timeout,
                  startup_timeout, startup_functions,
                  kill_if_fail, erl_flags, env}).

start(Node) ->
    start(gethostname(), Node).

start(Node, Opts)
  when is_list(Opts) ->
    start(gethostname(), Node, Opts);

start(Host, Node) ->
    start(Host, Node, []).

start(Host, Node, Opts) ->
    ENode = enodename(Host, Node),
    case erlang:is_alive() of
        false->
            {error, not_alive, node()};
        true->
            case is_started(ENode) of
                false->
                    OptionsRec = fetch_options(Opts),
                    do_start(Host, Node, OptionsRec);
                {true, not_connected}->
                    {error, started_not_connected, ENode};
                {true, connected}->
                    {error, already_started, ENode}
            end
    end.

stop(Node) ->
    stop(gethostname(), Node).

stop(Host, Node) ->
    ENode = enodename(Host, Node),
    case is_started(ENode) of
        {true, connected}->
             do_stop(ENode);
        {true, not_connected}->
             {error, not_connected, ENode};
        false->
             {error, not_started, ENode}
    end.

%%% fetch an option value from the tagged tuple list with default
get_option_value(Key, OptionList, Default) ->
    case lists:keyfind(Key, 1, OptionList) of
        false->
             Default;
        {Key, Value}->
             Value
    end.

%%% convert option list to the option record, fill all defaults
fetch_options(Options) ->
    UserName = get_option_value(username, Options, []),
    Password = get_option_value(password, Options, []),
    BootTimeout = get_option_value(boot_timeout, Options, 3),
    InitTimeout = get_option_value(init_timeout, Options, 5),
    StartupTimeout = get_option_value(startup_timeout, Options, 1),
    StartupFunctions = get_option_value(startup_functions, Options, []),
    KillIfFail = get_option_value(kill_if_fail, Options, true),
    ErlFlags = get_option_value(erl_flags, Options, []),
    EnvVars = get_option_value(env, Options, []),
    #options{username=UserName, password=Password,
             boot_timeout=BootTimeout, init_timeout=InitTimeout,
             startup_timeout=StartupTimeout, startup_functions=StartupFunctions,
             kill_if_fail=KillIfFail,
             erl_flags=ErlFlags, env=EnvVars}.


% check if node is listed in the nodes()
is_connected(ENode) ->
    [N||N<-nodes(), N==ENode] == [ENode].

% check if node is alive (ping and disconnect if pingable)
is_started(ENode) ->
    case is_connected(ENode) of
        true->
            {true, connected};
        false->
            case net_adm:ping(ENode) of
                pang->
                    false;
                pong->
                    erlang:disconnect_node(ENode),
                    {true, not_connected}
            end
    end.

%% "make a Erlang node name from name and hostname."
%% -- Just let's not tack @Host to Node when the latter already has one.
enodename(Host, Node) ->
    Node_s = atom_to_list(Node),
    case string:chr(Node_s, $@) of
        0 ->
            list_to_atom(Node_s++"@"++atom_to_list(Host));
        _ ->
            Node
    end.

% performs actual start of the "slave" node
do_start(Host, Node, Options) ->
    ENode = enodename(Host, Node),
    Self = self(),
    %% MasterNode = node(),
    %% WillDieWithYou =
    %%     fun() ->
    %%             erlang:monitor_node(MasterNode, true),
    %%             receive
    %%                 {nodedown, MasterNode}->
    %%                     init:stop()
    %%             end
    %%     end,
    %%
    %% Pushing functions to a remote node, to be called when *this*
    %% node is no longer (which is when this particular function would
    %% ever be called), appears to result in some (un)surprising
    %% undefs.  To bring the remote node down, the code must be
    %% "sourced locally"; we have to just pass -pa ./ebin when opening
    %% a port for the remote node.  Let's.. write a TODO item for this,
    %% right here?
    Functions =
        lists:concat([[{erlang, send, [Self, {node_started, ENode}]}],
                      Options#options.startup_functions,
                      [{erlang, send, [Self, {node_ready, ENode}]}]]),
    MasterHost = gethostname(),
    case hosts_resolve_to_same_addr(MasterHost, Host) of
        true ->
            link(spawn_local_node(Node, Options));
        false ->
            spawn_remote_node(Host, Node, Options)
    end,

    BootTimeout = Options#options.boot_timeout,
    InitTimeout = Options#options.init_timeout,
    StartupTimeout = Options#options.startup_timeout,
    Result =
        case wait_for_node_alive(ENode, BootTimeout) of
            pong ->
                case test_server:is_cover() of
                    true ->
                        MainCoverNode = cover:get_main_node(),
                        rpc:call(MainCoverNode, cover, start, [ENode]);
                    false ->
                        ok
                end,
                call_functions(ENode, Functions),
                receive
                    {node_started, ENode} ->
                        receive
                            {node_ready, ENode}->
                                {ok, ENode}
                        after StartupTimeout*1000->
                                {error, startup_timeout, ENode}
                        end
                after InitTimeout*1000 ->
                        {error, init_timeout, ENode}
                end;
            pang->
                {error, boot_timeout, ENode}
        end,
    case Result of
        {ok, ENode}->
             ok;
        {error, Timeout, ENode}
             when ((Timeout==init_timeout) or (Timeout==startup_timeout)) and
                  Options#options.kill_if_fail->
             do_stop(ENode);
        _-> ok
    end,
    Result.

hosts_resolve_to_same_addr(Host1, Host2) ->
    inet:getaddr(hostname_to_list(Host1), inet)
        == inet:getaddr(hostname_to_list(Host2), inet).

hostname_to_list(H) when is_atom(H) ->
    atom_to_list(H);
hostname_to_list(H) ->
    H.


% are we using fully qualified hostnames
long_or_short() ->
    case net_kernel:longnames() of
        true->
            " -name ";
        false->
            " -sname "
    end.

% get the localhost's name, depending on the using name policy
gethostname() ->
    Hostname = case net_kernel:longnames() of
        true->
            net_adm:localhost();
        _->
            {ok, Name}=inet:gethostname(),
            Name
    end,
    list_to_atom(Hostname).

% get cmd for starting Erlang
get_cmd(Node, Flags) ->
    Cookie = erlang:get_cookie(),
    "erl -detached -setcookie "++ atom_to_list(Cookie) ++
    long_or_short() ++ atom_to_list(Node) ++ " " ++ Flags.

% spawn node locally
spawn_local_node(Node, Options) ->
    #options{env=Env,erl_flags=ErlFlags} = Options,
    Cmd = get_cmd(Node, ErlFlags),
    open_port({spawn, Cmd}, [stream,{env,Env}]).

% start crypto and ssh if not yet started
check_for_ssh_running() ->
    case application:get_application(crypto) of
        undefined->
            application:start(crypto),
            case application:get_application(ssh) of
                undefined->
                    application:start(ssh);
                {ok, ssh}->
                    ok
            end;
        {ok, crypto}->
            ok
    end.

% spawn node remotely
spawn_remote_node(Host, Node, Options) ->
    #options{username=Username,
             password=Password,
             erl_flags=ErlFlags,
             env=Env} = Options,
    SSHOptions = case {Username, Password} of
        {[], []}->
            [];
        {_, []}->
            [{user, Username}];
        {_, _}->
            [{user, Username}, {password, Password}]
    end ++ [{silently_accept_hosts, true}],
    check_for_ssh_running(),
    {ok, SSHConnRef} = ssh:connect(atom_to_list(Host), 22, SSHOptions),
    {ok, SSHChannelId} = ssh_connection:session_channel(SSHConnRef, infinity),
    ssh_setenv(SSHConnRef, SSHChannelId, Env),
    ssh_connection:exec(SSHConnRef, SSHChannelId, get_cmd(Node, ErlFlags), infinity).


ssh_setenv(SSHConnRef, SSHChannelId, [{Var, Value} | Vars])
  when is_list(Var), is_list(Value) ->
    success = ssh_connection:setenv(SSHConnRef, SSHChannelId,
                                    Var, Value, infinity),
    ssh_setenv(SSHConnRef, SSHChannelId, Vars);
ssh_setenv(_SSHConnRef, _SSHChannelId, []) -> ok.

% call functions on a remote Erlang node
call_functions(_Node, []) ->
    ok;
call_functions(Node, [{M, F, A}|Functions]) ->
    _Res = rpc:call(Node, M, F, A),
    call_functions(Node, Functions).

% wait N seconds until node is pingable
wait_for_node_alive(_Node, 0) ->
    pang;
wait_for_node_alive(Node, N) ->
    timer:sleep(1000),
    case net_adm:ping(Node) of
        pong ->
            pong;
        pang ->
            wait_for_node_alive(Node, N-1)
    end.

% call init:stop on a remote node
do_stop(ENode) ->
    {Cover,MainCoverNode} =
        case test_server:is_cover() of
            true ->
                Main = cover:get_main_node(),
                rpc:call(Main,cover,flush,[ENode]),
                {true,Main};
            false ->
                {false,undefined}
        end,
    spawn(ENode, init, stop, []),
    case wait_for_node_dead(ENode, 5) of
        {ok,ENode} ->
            if Cover ->
                    %% To avoid that cover is started again if a node
                    %% with the same name is started later.
                    rpc:call(MainCoverNode,cover,stop,[ENode]);
               true ->
                    ok
            end,
            {ok,ENode};
        Error ->
            Error
    end.

% wait N seconds until node is disconnected
wait_for_node_dead(Node, 0) ->
    {error, stop_timeout, Node};
wait_for_node_dead(Node, N) ->
    timer:sleep(1000),
    case lists:member(Node, nodes()) of
        true->
            wait_for_node_dead(Node, N-1);
        false->
            {ok, Node}
    end.
