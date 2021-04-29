# Simple Setup Process

This intended as a simplified setup set-by-step guide (in comparison to the [README](../README.md)).

## Decide Paths

You need to decide on destinations for the following:

- A runtime for Riak instances running tests (e.g. "~/rt/riak")

- A destination to install yokozuna tests should you wish to run yokozuna tests (e.g. "\~/test_sw/yokozuna") and basho_bench (e.g. "\~/test_sw/basho_bench")

- A destination in which to build riak releases to be tested (e.g. "~/test_build/riak")

- A location for the riak_test software itself, where the tests will be built and test scripts will be run and logs will be stored (e.g. "~/riak_test")

There will also need to be a test configuration file (".riak_test.config"), which must be in the root of the home directory of the user running tests.

## Clone Repos

Clone riak into your build area i.e.:

```
cd ~/test_build
git clone https://github.com/basho/riak
```

Clone riak_test into your test software location, checkout the test branch you intend to use, and make riak_test i.e.:

```
cd ~
git clone https://github.com/basho/riak_test
cd riak_test
git checkout develop-2.9
make all
```

Clone yokozuna into your test software location, checkout the test branch you intend to use, and make test i.e.:

```
cd ~/test_sw
git clone https://github.com/basho/yokozuna
cd yokozuna
git checkout develop-2.9
make all
make test
```

Also within yokozuna make the yokozuna bench test scripts (which are called within some of the yokozuna tests)

```
cd ~/test_sw/yokozuna/misc/bench
../../rebar get-deps
../../rebar compile
```

Clone basho_bench into your test software location, checkout the test branch you intend to use, and make i.e.:

```
cd ~/test_sw
git clone https://github.com/basho/basho_bench
cd basho_bench
git checkout develop-2.9
make all
```

## Setup Test Runtime

Initialise git in the test runtime environment i.e.:

```
cd ~/riak_test
./bin/rtdev-setup-releases.sh
```

This script will make `~/rt/riak` the path to setup releases.  If this is to be overridden, then it ca be do so by override [$RT_DEST_DIR](../bin/rtdev-setup-releases.sh#L11)

## Setup initial test Configuration

Create a `~/.riak_test.config` file with the following sample configuration:

```
{default, [
    {rt_max_wait_time, 600000},
    {rt_retry_delay, 1000},
    {rt_harness, rtdev},
    {rt_scratch_dir, "/tmp/riak_test_scratch"},
    {spam_dir, "~/riak_test/search-corpus/spam.0"},
    {platform, "osx-64"}
]}.

{rtdev, [
    {rt_project, "riak"},
    {basho_bench, "~/test_sw/basho_bench"},
    {yz_dir, "~/test_sw/yokozuna"},
    {test_paths, ["~/test_sw/yokozuna/riak_test/ebin"]},
    {rtdev_path, [{root,     "~/rt/riak"},
                  {current,  "~/rt/riak/current"},
                  {previous, "~/rt/riak/previous"},
                  {legacy, "~/rt/riak/riak-2.0.5"},
                  {"2.0.5", "~/rt/riak/riak-2.0.5"}
                 ]}
]}.
```

The `spam_dir` will need to point at the location of the riak_test software.  At that location there will be a tarred/zipped file containing test data, which you will need to untar/unzip to populate the spam directory.

The platform shout be set to `osx-64` or `linux`.

When testing yokozuna using the `group.sh` script, the yokozuna riak_tests are not copied as part of the initialisation of the script and so need to be found through setting the path in `test_paths`.  There are three yokozuna tests that require a specific reference to the "2.0.5" build, hence the last line in the `rtdev_path` list.

## Build each Test version

Within the test_build are, it is necessary to build each release and copy them over to the run time environment.  

```
# to make develop-2.9 'current'

git checkout develop-2.9
make devclean; make devrel
~/riak_test/bin/rtdev-install.sh current

# to make Riak 2.2.6 'previous'
git checkout riak-2.2.6
make devclean; make devrel
~/riak_test/bin/rtdev-install.sh previous

# to make Riak 2.0.5 'legacy'
git checkout riak-2.0.5
make devclean; make devrel
~/riak_test/bin/rtdev-install.sh legacy
```

Each time you change a branch for testing, re-run the `make devclean; make devrel` and the `rtdev-install.sh` script.

If you intend to test cluster upgrade tests that include replication, then you will need to use the enterprise release of Riak for nay release prior to `2.2.5`.

## Install JDK

To test yokozuna, it will be necessary to install a JDK.  The install instructions on Yokozuna github recommend "Java 1.6 or later, Oracle 7u25 is recommended".  Tests should run successfully if the retired [Java 7 is used](https://java.com/en/download/faq/java_7.xml), but please be aware of the security implications of running this software.

## Redbug support

tba - further work required to get redbug tests working.

## Run Tests

There are two primary ways of running tests:

### Run a test 'group'

There are pre-defined groups of tests under the `groups` folder in `riak_test`, or you could define your own.  The group is a list of test names, and the test script will run each of these tests in alphabetical order.

```
# To run the 'kv_all' group with the bitcask backend
cd ~/riak_test
./group.sh -g kv_all -c rtdev -b bitcask

# To run the '2i_all' group with the leveled backend
cd ~/riak_test
./group.sh -g 2i_all -c rtdev -b leveled
```

The `-c` reference must refer to a stanza in your `~/.riak_test.config`.

### Run an individual test

Tests can be run individually, with an additional facility that failing tests will be paused at the point they fail, if they fail.  This allows you to inspect the state of the cluster (within `~/rtdev/riak`) at the failure point.

```
# e.g. run verify_crdt_capability with leveled backend
./riak_test -c rtdev -t verify_crdt_capability -b leveled

# e.g. run an individual yokozuna test
./riak_test -c rtdev -t ~/riak_test_deps/yokozuna/riak_test/ebin/yz_security
```
