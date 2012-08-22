# Riak Test

## Bootstraping Your Test Environment

Welcome to the exciting world of riak_test. Running tests against a development version of Riak is just one of the things that you can do with riak_test. You can also test things involving upgrading from previous versions of Riak. Together, we'll get your test environment up and running. Scripts to help in this process are located in the `bin` directory of this project. 

### rtdev-build-releases.sh
The first one that we want to look at is `rtdev-build-releases.sh`. If left unchanged, this script is going to do the following:

1. Download the source for the past three major Riak versions (e.g. 1.0.3, 1.1.4, and 1.2.0)
1. Build the proper version of Erlang that release was built with, using kerl (which it will also download)
1. Build those releases of Riak.

You'll want to run this script from an empty directory. Also, you might be thinking that you already have all the required versions of erlang. Great! You can crack open the script and set the paths to your installation

```bash
R14B04=${R14B04:-$HOME/erlang-R14B04}
```

**Kerlveat**: If you want kerl yo build you erlangs with serious 64-bit macintosh action, you'll need a `~/.kerlrc` file that looks like this:

```
KERL_CONFIGURE_OPTIONS="--enable-hipe --enable-smp-support --enable-threads 
--enable-kernel-poll  --enable-darwin-64bit"
```

The script will check that all these paths exist. If even one of them is missing, it will prompt you to install kerl, even if you already have kerl. If you say no, the script quits. If you say yes, or all of your erlang paths check out, then go get a cup of coffee, you'll be building for a little while.

**Warning**: If you are running OS X 10.7+, then the erlang_js dependency won't compile for you, but fails silently. Fortunately, the precomipled OS X build includes this dependency in it's working form. Here's how you get it:

```bash
wget http://s3.amazonaws.com/downloads.basho.com/riak/1.0/1.0.3/riak-1.0.3-osx-x86_64.tar.gz
mkdir erlang_js-1.0.0 && tar xvzf riak-1.0.3-osx-x86_64.tar.gz -C erlang_js-1.0.0 riak-1.0.3/lib/erlang_js-1.0.0 
rm -rf riak-1.0.3/dev/dev1/lib/erlang_js-1.0.0
rm -rf riak-1.0.3/dev/dev2/lib/erlang_js-1.0.0
rm -rf riak-1.0.3/dev/dev3/lib/erlang_js-1.0.0
rm -rf riak-1.0.3/dev/dev4/lib/erlang_js-1.0.0
cp -r erlang_js-1.0.0/riak-1.0.3/lib/erlang_js-1.0.0 riak-1.0.3/dev/dev1/lib/.
cp -r erlang_js-1.0.0/riak-1.0.3/lib/erlang_js-1.0.0 riak-1.0.3/dev/dev2/lib/.
cp -r erlang_js-1.0.0/riak-1.0.3/lib/erlang_js-1.0.0 riak-1.0.3/dev/dev3/lib/.
cp -r erlang_js-1.0.0/riak-1.0.3/lib/erlang_js-1.0.0 riak-1.0.3/dev/dev4/lib/.
```

**Please run this patch before proceeding on to the next script**

### rtdev-setup-releases.sh
The `rtdev-setup-releases.sh` will get the releases you just built into a local git repository. Currently, running this script from the same directory that you just built all of your releases into. Currently this script initializes this repository into `/tmp/rt` but it's probably worth making that configurable in the near term.

### rtdev-current.sh
`rtdev-current.sh` is where it gets interesting. You need to run that from the riak source folder you're wanting to test as the current version of riak. Also, make sure that you've already run `make devrel` or `make stagedevrel` before you run `rtdev-current.sh`.

### Config file.
Now that you've got your releases all ready and gitified, you'll need to tell riak_test about them. The method of choice is to create a `~/.riak_test.config` that looks something like this:

```erlang
{rtdev_mixed, [
    {rt_deps, ["$PATH_TO_YOUR_RIAK_SOURCE/deps"]},
    {rt_max_wait_time, 180000},
    {rt_retry_delay, 500},
    {rt_harness, rtdev},
    {rtdev_path, [{root, "/tmp/rt"},
                  {current, "/tmp/rt/current"},
                  {"1.2.0", "/tmp/rt/riak-1.2.0"},
                  {"1.1.4", "/tmp/rt/riak-1.1.4"},
                  {"1.0.3", "/tmp/rt/riak-1.0.3"}]}
]}.

```

### Running riak_test for the first time
Run a test! `./riak_test rtdev_mixed verify_build_cluster`

Did that work? Great, try something harder: `./riak_test rtdev_mixed upgrade`

