-module(client_ruby_verify).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(RUBY_CLIENT_TAG, "master").
-define(RUBY_CHECKOUT, filename:join([rt_config:get(rt_scratch_dir), "riak-ruby-client"])).
-define(RUBY_GIT_URL, "https://github.com/basho/riak-ruby-client.git").
-define(RUBY_BUNDLE_PATH, filename:join([RUBY_CHECKOUT, "vendor/bundle"])).

-prereq("ruby").
-prereq("gem").

%% @todo Only Memory backend is supported

confirm() ->
    prereqs(),
    clone_repo(),
    install_dependencies(),
    Nodes = rt:deploy_nodes([{current,
                              [{riak_kv, [{test, true}]},
                               {riak_search, [{enabled, true}]}]}],
                            [riak_search]),
    [Node1] = Nodes,

    configure_test_client(Node1),

    Cmd = "./bin/rspec --profile --tag integration --no-color",

    lager:info("Cmd: ~s", [Cmd]),

    {Code, RubyLog} = rt_local:stream_cmd(Cmd, [{cd, ?RUBY_CHECKOUT}]),
    ?assert(rt:str(RubyLog, " 0 failures")),
    ?assert(Code =:= 0),
    pass.

prereqs() ->
    lager:info("Checking ruby version is 1.9.3 or 2.0.0"),
    RubyVersion = os:cmd("ruby -v"),
    ?assert(rt:str(RubyVersion, "1.9.3") orelse rt:str(RubyVersion, "2.0.0")),

    rt_local:install_on_absence("bundle", "gem install bundler --no-rdoc --no-ri"),
    ok.


clone_repo() ->
    lager:info("Cleaning up ruby scratch directory"),
    CleanupCmd = io_lib:format("rm -rf ~s", [?RUBY_CHECKOUT]),
    os:cmd(CleanupCmd),
    
    lager:info("Cloning riak-ruby-client from ~s", [?RUBY_GIT_URL]),
    CloneCmd = io_lib:format("git clone ~s ~s", [?RUBY_GIT_URL, ?RUBY_CHECKOUT]),
    rt_local:stream_cmd(CloneCmd),
    
    lager:info("Resetting ruby client to tag '~s'", [?RUBY_CLIENT_TAG]),
    TagCmd = io_lib:format("git checkout ~s", [?RUBY_CLIENT_TAG]),
    rt_local:stream_cmd(TagCmd, [{cd, ?RUBY_CHECKOUT}]).

install_dependencies() ->
    lager:info("Installing ruby client dependencies"),
    BundleCmd = "bundle install --without=guard --binstubs --no-color --path=vendor/bundle",
    rt_local:stream_cmd(BundleCmd, [{cd, ?RUBY_CHECKOUT},
                                    {env, [{"BUNDLE_PATH", "vendor/bundle"}]}
                                   ]).

configure_test_client(Node) ->
    [{Node, ConnectionInfo}] = rt:connection_info([Node]),
    Hostname = atom_to_list(Node),
    {_PB_Host, PB_Port} = orddict:fetch(pb, ConnectionInfo),
    ConfigYaml = io_lib:format("host: ~s~npb_port: ~b~n", [Hostname, PB_Port]),
    TestClientFilename = filename:join([?RUBY_CHECKOUT, "spec/support/test_client.yml"]),
    
    ?assertMatch(ok, file:write_file(TestClientFilename, ConfigYaml)).
