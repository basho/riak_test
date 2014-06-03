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
    [Node1] = rt:deploy_nodes(1, {cuttlefish, [
                                               {"search", "on"},
                                               {"storage_backend", "memory"},
                                               {"anti_entropy", "passive"}
                                              ]}),

    setup_bucket_types(Node1),

    configure_test_client(Node1),

    Cmd = "./bin/rspec --profile --tag integration --no-color",

    lager:info("Cmd: ~s", [Cmd]),

    {Code, RubyLog} = rt_local:stream_cmd(Cmd, [{cd, ?RUBY_CHECKOUT}]),
    ?assert(rt:str(RubyLog, " 0 failures")),
    ?assert(Code =:= 0),
    pass.

prereqs() ->
    lager:info("Checking ruby version is 1.9.3, 2.0.0, 2.1.0, or 2.1.1"),
    RubyVersion = os:cmd("ruby -v"),
    ?assert(rt:str(RubyVersion, "1.9.3") orelse 
            rt:str(RubyVersion, "2.0.0") orelse
            rt:str(RubyVersion, "2.1.0") orelse
            rt:str(RubyVersion, "2.1.1")),

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

setup_bucket_types(Node) ->
    lager:info("Creating bucket types"),

    CounterType = <<"counters">>,
    rt:create_and_activate_bucket_type(Node, CounterType, [{datatype, counter}, {allow_mult, true}]),

    MapType = <<"maps">>,
    rt:create_and_activate_bucket_type(Node, MapType, [{datatype, map}, {allow_mult, true}]),

    SetType = <<"sets">>,
    rt:create_and_activate_bucket_type(Node, SetType, [{datatype, set}, {allow_mult, true}]),

    lager:info("Waiting for bucket types"),
    rt:wait_until_bucket_type_status(CounterType, active, [Node]),
    rt:wait_until_bucket_type_status(MapType, active, [Node]),
    rt:wait_until_bucket_type_status(SetType, active, [Node]),
    lager:info("Bucket types ready").
    

configure_test_client(Node) ->
    [{Node, ConnectionInfo}] = rt:connection_info([Node]),
    {PB_IP, PB_Port} = orddict:fetch(pb, ConnectionInfo),
    ConfigYaml = io_lib:format("host: ~s~npb_port: ~b~n", [PB_IP, PB_Port]),
    TestClientFilename = filename:join([?RUBY_CHECKOUT, "spec/support/test_client.yml"]),
    
    ?assertMatch(ok, file:write_file(TestClientFilename, ConfigYaml)).
