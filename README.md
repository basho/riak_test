# Riak Test
Welcome to the exciting world of `riak_test`. 

## What is Riak Test?

`riak_test` is a system for testing Riak clusters. Tests are written
in Erlang, and can interact with the cluster using distributed Erlang.

### How does it work?

`riak_test` runs tests in a sandbox, typically `/tmp/rt`. The sanbox
uses `git` to reset back to a clean state after tests are run. The
contents of `/tmp/rt` might look something like this:

```
$ ls /tmp/rt
current riak-1.0.3 riak-1.1.4 riak-1.2.0
```

Inside each of these directories is a `dev` folder, typically
created with your normal `make [stage]devrel`. So how does
this sandbox get populated to begin with?

You'll create another directory that will contain full builds
of different version of Riak for your platform. Typically this directory
has been `~/test-releases` but it can be called anything and be anywhere
that you'd like. The `dev/` directory from each of these
releases will be copied into the sandbox (`/tmp/rt`).
There are helper scripts in `bin/` which will
help you get both `~/test-releases` and `/tmp/rt` all set up. A full
tutorial for using them exists further down in this README.

There is one folder in `/tmp/rt` that does not come from
`~/test-releases`: `current`. The `current` folder can refer
to any version of Riak, but is typically used for something
like the `master` branch, a feature branch, or a release candidate.
The `/tmp/rt/current` dev release gets populated from a devrel of Riak
that can come from anywhere, but is usually your 'normal' git checkout
of Riak. The `bin/rtdev-current.sh` can be run from within that folder
to copy `dev/` into `/tmp/rt/current`.

Once you have everything set up (again, instructions for this are below),
you'll want to run and write tests. This repository also holds code for
an Erlang application called `riak_test`. The actual tests exist in
the `test/` directory.

## Bootstraping Your Test Environment

Running tests against a
development version of Riak is just one of the things that you can do
with riak_test. You can also test things involving upgrading from
previous versions of Riak. Together, we'll get your test environment
up and running. Scripts to help in this process are located in the
`bin` directory of this project.

### rtdev-all.sh

This script is for the lazy. It performs all of the setup steps described
in the other scripts, including installing the current "master" branch from
Github into "current". The releases will be built in your current working
directory, so create an empty one in a place you'd like to store these
builds for posterity, so that you don't have to rebuild them if your
installation path (`/tmp/rt` by the way this script installs it) gets into
a bad state, or deleted as tmpfs things tend to get during reboot.

If you do want to restore your `/tmp/rt` folder to factory condition, see
`rtdev-setup-releases.sh` and if you want to change the current riak under
test, see `rtdev-current.sh`.

### rtdev-build-releases.sh

The first one that we want to look at is `rtdev-build-releases.sh`. If
left unchanged, this script is going to do the following:

1. Download the source for the past three major Riak versions (e.g.
   1.0.3, 1.1.4, and 1.2.0)
1. Build the proper version of Erlang that release was built with,
   using kerl (which it will also download)
1. Build those releases of Riak.

You'll want to run this script from an empty directory. Also, you
might be thinking that you already have all the required versions of
erlang. Great! You can crack open the script and set the paths to your
installation:

```bash
R14B04=${R14B04:-$HOME/erlang-R14B04}
```

**Kerlveat**: If you want kerl to build erlangs with serious 64-bit
macintosh action, you'll need a `~/.kerlrc` file that looks like this:

```
KERL_CONFIGURE_OPTIONS="--disable-hipe --enable-smp-support --enable-threads --enable-kernel-poll  --enable-darwin-64bit"
```

The script will check that all these paths exist. If even one of them
is missing, it will prompt you to install kerl, even if you already
have kerl. If you say no, the script quits. If you say yes, or all of
your erlang paths check out, then go get a cup of coffee, you'll be
building for a little while.

**Warning**: If you are running OS X 10.7+ and trying to build Riak
1.0.3, then the erlang_js dependency won't compile for you, but it
fails silently. Fortunately, the precomipled OS X build includes this
dependency in it's working form. Just run `rtdev-lion-fix.sh` after
`rtdev-build-releases.sh` to patch it. **Please run this patch before
proceeding on to the next script**

### rtdev-setup-releases.sh

The `rtdev-setup-releases.sh` will get the releases you just built
into a local git repository. Currently, running this script from the
same directory that you just built all of your releases into.
Currently this script initializes this repository into `/tmp/rt` but
it's probably worth making that configurable in the near term.

### rtdev-current.sh

`rtdev-current.sh` is where it gets interesting. You need to run that
from the Riak source folder you're wanting to test as the current
version of Riak. Also, make sure that you've already run `make devrel`
or `make stagedevrel` before you run `rtdev-current.sh`.

### Config file.

Now that you've got your releases all ready and gitified, you'll need
to tell riak_test about them. The method of choice is to create a
`~/.riak_test.config` that looks something like this:

```erlang
{default, [
    {rt_max_wait_time, 180000},
    {rt_retry_delay, 1000}
]}.

{rtdev, [
    {rt_deps, ["$PATH_TO_YOUR_RIAK_SOURCE/deps"]},
    {rt_retry_delay, 500},
    {rt_harness, rtdev},
    {rtdev_path, [{root, "/tmp/rt"},
                  {current, "/tmp/rt/current"},
                  {"1.2.0", "/tmp/rt/riak-1.2.0"},
                  {"1.1.4", "/tmp/rt/riak-1.1.4"},
                  {"1.0.3", "/tmp/rt/riak-1.0.3"}]}
]}.

```

The `default` section of the config file will be overridden by the config
name you specify. For example, running the command below will use an
`rt_retry_delay` of 500 and an `rt_max_wait_time` of 180000. If your 
defaults contain every option you need, you can run riak_test without
the `-c` argument.

### Running riak_test for the first time

Run a test! `./riak_test -c rtdev -t verify_build_cluster`

Did that work? Great, try something harder: `./riak_test -c
rtdev_mixed -t upgrade`


Using Mecks
----------

Riak Test has the ability to alter specific behavior of Erlang and
Riak on the nodes under test.  Specifically, it provides the ability
to redefine any number of functions, on-the-fly, for any test.  Some
examples:

* Redefined `riak_core_ring_manager:ring_trans` to be a noop.  See
  [riak_core_ring_manager_mecks.erl][rcrm].

* Delay the completion of `hashtree:update_perform`.  See
  [hashtree_mecks][hm].

* Cause partial writes by removing vnodes from the preflist in the put
  fsm.

* Periodically drop ring changed events on the floor.

* Have a specific KV vnode die after 60 seconds.

The possibilities are endless.  Riak Test uses [meck][] allowing you to
change the behavior of any function you want.  You can change the
function arguments.  You can cause side effects.  You can change the
return value.  You can completely replace the function with your own
definition.  You can effect global state like ETS tables and
distribute Erlang.  The fundamental concept is that it allows you to
Redefined any function and optionally call back into the original
function if you still want it to perform its work but modify it in
some way.

### How To Write Mecks

First, look at the [existing mecks][em] to get an idea of what a Riak
Test meck definition looks like.  Riak Test relies on some conventions
to make mecks easier to write and to give some sense of structure.
Here are a list of rules to keep in mind when writing mecks.

* The module name of the meck should be `<module_name_to_meck>_mecks`.
  E.g. if you want to meck the module `lists` then the file
  `lists_mecks.erl` should be created under the mecks dir.  This
  convention is **mandatory** for the meck macros to work correctly.

* All exported functions should add an _expect_ to the function it
  wishes to alter.  The term _expect_ comes from mock nomenclature.
  The idea is to "mock" a function so that it performs in the specific
  way you want it to.  You do this to isolate code to make testing
  easier.  E.g. replace a database call with a mock that returns a
  static result set so you can test the code that parses the result
  set.  It's the same idea in Riak Test except the focus is on
  altering or replacing function definitions in order to help diagnose
  what happens when things go wrong.

* The last line of all exported functions should be a call to the
  macro `?M_EXPECT(FunName, Definition)`.  Where `FunName` is the
  function name as an atom and `Definition` is a [fun][funs] which
  redefines the execution of `FunName`.  If `FunName` has multiple
  arities then meck will automatically mock the correct one based on
  the arity of the fun.

* If you still want to call into the original function then call
  `meck:passthrough([Arg1, Arg2, ...])`.  Notice the args need to be
  wrapped in a list.

* It is highly recommended that you add logging calls to your mock.
  This way there is an indication in the riak logs that a specific
  function call has been changed.  Keep in mind that these logs could
  be saved somewhere and logged at 3 months later.

* All logging calls **must** use the `?M_` macros.  Because of the way
  these modules are compiled no lager parse transform is performed.
  The macros use [error_logger][] which is automatically converted
  into equivalent lager statements by lager itself.

### How To Use Mecks

There are two ways to use mecks--via the config file of
programmatically in the test.  These methods may be used together.

Using the config file allows you to apply any number of mecks to any
test.  The meck is applied to all nodes and is effect for the entire
duration of the run.  Even after a node restart the mecks will be
re-loaded and re-initialized.  To use mecks add the section `{mecks,
[{MeckModName, Fun}, {MeckModName, Fun, Args}]}` to your project conf
in the configuration file.  Here is an example of using the noop ring
trans and hashtree mecks.

    {chaos, [
        {mecks, [{riak_core_ring_manager_mecks, noop_ring_trans},
                 {hashtree_mecks, update_perform_sleep, [60]}]},
    ]}.

This will apply all the listed mecks any time a node is started and
they will stay in effect for the entire duration of the run.  In some
cases you may want to control when the meck goes into effect.  In this
case you need to do 3 things.  See [gh_riak_core_155][] for an example.

1. Make sure the specific meck you want to control is **not** in the
   mecks list.

2. Before calling any of the specific meck functions
   (e.g. `hashtree_mecks:update_perform_sleep(60)`) first call the
   `init` function.  This function is automatically created by the
   meck header file.  Remember this call needs to be made on the
   remote node(s), not the Riak Test node.  E.g. `ok = rpc:call(Node,
   riak_core_vnode_proxy_sup_mecks, init, [])`.

3. Make a remote call to the meck functions you wish to apply.
   E.g. `ok = rpc:call(Node, riak_core_vnode_proxy_sup_mecks,
   delay_start_proxies, [3000])`.  At this point the
   `riak_core_vnode_proxy_sup:start_proxies` function has been altered
   to sleep for 3 seconds before continuing with its normal work.

After step 3 the meck will stay in effect for the duration of the
node's life.  Thus, if you restart it the meck will go away.

[em]: https://github.com/basho/riak_test/tree/4c72e13a229a52b005d9b7924f6f307cdca1ed7e/mecks

[error_logger]: http://www.erlang.org/doc/man/error_logger.html

[funs]: http://www.erlang.org/doc/programming_examples/funs.html

[gh_riak_core_155]: https://github.com/basho/riak_test/blob/4c72e13a229a52b005d9b7924f6f307cdca1ed7e/tests/gh_riak_core_155.erl#L43

[hm]: https://github.com/basho/riak_test/blob/4c72e13a229a52b005d9b7924f6f307cdca1ed7e/mecks/hashtree_mecks.erl

[meck]: https://github.com/eproxus/meck

[rcrm]: https://github.com/basho/riak_test/blob/4c72e13a229a52b005d9b7924f6f307cdca1ed7e/mecks/riak_core_ring_manager_mecks.erl
