# Riak Test
Welcome to the exciting world of `riak_test`. 

## What is Riak Test?

`riak_test` is a system for testing Riak clusters. Tests are written
in Erlang, and can interact with the cluster using distributed Erlang.

### How does it work?

`riak_test` runs tests in a sandbox, typically `$HOME/rt/riak`. The sanbox
uses `git` to reset back to a clean state after tests are run. The
contents of `$HOME/rt/riak` might look something like this:

```
$ ls $HOME/rt/riak
current riak-1.1.4 riak-1.2.1 riak-1.3.2
```

Inside each of these directories is a `dev` folder, typically
created with your normal `make [stage]devrel`. So how does
this sandbox get populated to begin with?

You'll create another directory that will contain full builds
of different version of Riak for your platform. Typically this directory
has been `~/test-releases` but it can be called anything and be anywhere
that you'd like. The `dev/` directory from each of these
releases will be copied into the sandbox (`$HOME/rt/riak`).
There are helper scripts in `bin/` which will
help you get both `~/test-releases` and `$HOME/rt/riak` all set up. A full
tutorial for using them exists further down in this README.

There is one folder in `$HOME/rt/riak` that does not come from
`~/test-releases`: `current`. The `current` folder can refer
to any version of Riak, but is typically used for something
like the `master` branch, a feature branch, or a release candidate.
The `$HOME/rt/riak/current` dev release gets populated from a devrel of Riak
that can come from anywhere, but is usually your 'normal' git checkout
of Riak. The `bin/rtdev-current.sh` can be run from within that folder
to copy `dev/` into `$HOME/rt/riak/current`.

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
installation path (`$HOME/rt/riak` by the way this script installs it) gets into
a bad state.

If you do want to restore your `$HOME/rt/riak` folder to factory condition, see
`rtdev-setup-releases.sh` and if you want to change the current riak under
test, see `rtdev-current.sh`.

### rtdev-build-releases.sh

The first one that we want to look at is `rtdev-build-releases.sh`. If
left unchanged, this script is going to do the following:

1. Download the source for the past three major Riak versions (e.g.
   1.1.4, 1.2.1 and 1.3.2)
1. Build the proper version of Erlang that release was built with,
   using kerl (which it will also download)
1. Build those releases of Riak.

You'll want to run this script from an empty directory. Also, you
might be thinking that you already have all the required versions of
erlang. Great! You can crack open the script and set the paths to your
installation or set them from the command-line:

```bash
R14B04=${R14B04:-$HOME/erlang-R14B04}
R15B01=${R15B01:-$HOME/erlang-R15B01}
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

### rtdev-setup-releases.sh

The `rtdev-setup-releases.sh` will get the releases you just built
into a local git repository. Run this script from the
same directory that you just built all of your releases into.
By default this script initializes the repository into `$HOME/rt/riak` but
you can override [`$RT_DEST_DIR`](https://github.com/basho/riak_test/blob/master/bin/rtdev-setup-releases.sh#L11).

### rtdev-current.sh

`rtdev-current.sh` is where it gets interesting. You need to run that
from the Riak source folder you're wanting to test as the current
version of Riak. Also, make sure that you've already run `make devrel`
or `make stagedevrel` before you run `rtdev-current.sh`. Like setting up
releases you can override [`$RT_DEST_DIR`](https://github.com/basho/riak_test/blob/master/bin/rtdev-current.sh#L6)
so all your riak builds are in one place.

### Config file.

Now that you've got your releases all ready and gitified, you'll need
to tell riak_test about them. The method of choice is to create a
`~/.riak_test.config` that looks something like this:

```erlang
{default, [
    {giddyup_host, "localhost:5000"},
    {giddyup_user, "user"},
    {giddyup_password, "password"},
    {rt_max_wait_time, 600000},
    {rt_retry_delay, 1000},
    {rt_harness, rtdev},
    {rt_scratch_dir, "/tmp/riak_test_scratch"},
    {basho_bench, "/home/you/basho/basho_bench"},
    {spam_dir, "/home/you/basho/riak_test/search-corpus/spam.0"},
    {platform, "osx-64"}
]}.

{rtdev, [
    {rt_project, "riak"},
    {rtdev_path, [{root,     "/home/you/rt/riak"},
                  {current,  "/home/you/rt/riak/current"},
                  {previous, "/home/you/rt/riak/riak-1.3.2"},
                  {legacy,   "/home/you/rt/riak/riak-1.2.1"}
                 ]}
]}.
```

The `default` section of the config file will be overridden by the config
name you specify. For example, running the command below will use an
`rt_retry_delay` of 500 and an `rt_max_wait_time` of 180000. If your 
defaults contain every option you need, you can run riak_test without
the `-c` argument.

Some configuration parameters:
 
#### rt_default_config
Default configuration parameters that will be used for nodes deployed by riak_test.  Tests can
override these.

```erlang
{rtdev, [
    { rt_default_config,
        [ {riak_core, [ {ring_creation_size, 16} ]} ] }
]}
```
#### Coverage
You can generate a coverage report for a test run through [Erlang Cover](http://www.erlang.org/doc/apps/tools/cover_chapter.html).
Coverage information for all **current** code run on any Riak node started by any of the tests in the run will be output as HTML in the coverage directory.
That is, legacy and previous nodes used in the test will not be included, as the tool can only work on one version of the code at a time.
Also, cover starts running in the Riak nodes after the node is up, so it will not report coverage of application initialization or other early code paths. 
You can specify a list of modules for which you want coverage generated like this:
```erlang
    {cover_modules, [riak_kv_bitcask_backend, riak_core_ring]}
```
Or entire applications by using:
```erlang
    {cover_apps, [riak_kv, riak_core]}
```

#### Web hooks
When reporting is enabled, each test result is posted to [Giddy Up](http://giddyup.basho.com). You can specify
any number of webhooks that will also receive a POST request with JSON formatted test information, plus the URL
of the Giddy Up resource page.  

```erlang
    {webhooks, [
                [{name, "Bishop"},
                 {url, "http://basho-engbot.herokuapp.com/"}]
                ]}
```

This is an example test result JSON message posted to a webhook:
```json
{ "test": "verify_build_cluster",
  "status": "fail",
  "log": "Some really long lines of log output",
  "backend": "bitcask",
  "id": "144",
  "platform": "osx-64",
  "version": "riak-1.4.0-9-g740a58d-master",
  "project": "riak",
  "reason": "{{assertion_failed, and_probably_a_massive_stacktrace_and stuff}}",
  "giddyup_url": "http://giddyup.basho.com/test_results/53" }
```
Notice that the giddyup URL is not the page for the test result, but a resource from which you can GET information about the test in JSON.
### Running riak_test for the first time

Run a test! `./riak_test -c rtdev -t verify_build_cluster`

Did that work? Great, try something harder: `./riak_test -c
rtdev_mixed -t upgrade`


Intercepts
----------

Intercepts are a powerful but easy to wield feature.  They allow you
to change the behavior of any function and affect global state in an
extremely lightweight manner.  You can modify
[the KV vnode to simulate dropped puts][dropped_puts].  You can
[sleep a call][hashtree_sleep] to discover what happens when certain
calls take a long time to finish.  You can even
[turn a call into a noop][ring_noop] to really cause havoc on a
cluster.  These are just some examples.  You should also be able to
change any function you want, including dependency functions and even
Erlang functions.  Furthermore, any state you can reach from a
function call can be affected such as function arguments but also ETS
tables.  This leads to the principle of intercepts.

> If you can do it in Riak source code you can do it with an
> intercept.

[dropped_puts]: https://github.com/basho/riak_test/blob/d284dcfc22d5d84ad301804691b16dbda6d91aa8/intercepts/riak_kv_vnode_intercepts.erl#L7

[hashtree_sleep]: https://github.com/basho/riak_test/blob/d284dcfc22d5d84ad301804691b16dbda6d91aa8/intercepts/hashtree_intercepts.erl#L5

[ring_noop]: https://github.com/basho/riak_test/blob/d284dcfc22d5d84ad301804691b16dbda6d91aa8/intercepts/riak_core_ring_manager_intercepts.erl#L5

### Writing Intercepts

Writing an intercept is nearly identical to writing any other Erlang
source with a few easy to remember conventions added.

1. All intercepts must live under the `intercepts` dir.

2. All intercept modules should be named the same as the module they
  affect with the suffix `_intercepts` added.  E.g. `riak_kv_vnode` =>
  `riak_kv_vnode_intercepts`.

3. All intercept modules should include the `intercept.hrl` file.
   This includes macros to properly log messages.  You **cannot** call
   lager.

4. All intercept modules should declare the macro `M` who's value is
   the affected module with the suffix `_orig` added.  E.g. for
   `riak_kv_vnode` add the line `-define(M, riak_kv_vnode_orig)`.
   This, along with the next convention is needed to call into the
   original function.

5. To call the origin function use the `?M:` follow by the name of the
   function with the `_orig` suffix appended.  E.g. to call
   `riak_kv_vnode:put` you would type `?M:put_orig`.

6. To log a message use the `I_` macros.  E.g. to log an info message
   use `?I_INFO`.

The easiest way to understand the above conventions is to see them all
at work in an example.

```erlang
-module(riak_kv_vnode_intercepts).
-compile(export_all).
-include("intercept.hrl").
-define(M, riak_kv_vnode_orig).

dropped_put(Preflist, BKey, Obj, ReqId, StartTime, Options, Sender) ->
    NewPreflist = lists:sublist(Preflist, length(Preflist) - 1),
    ?I_INFO("Preflist modified from ~p to ~p", [Preflist, NewPreflist]),
    ?M:put_orig(NewPreflist, BKey, Obj, ReqId, StartTime, Options, Sender).
```

### How Do I Use Intercepts?

Intercepts can be used in two ways: 1) added via the config, 2) added
via `rpc:call` in the test.  The first way is most convenient, is
persistent (survives node restarts), and is in effect for all tests.
The second method requires additional code, is specific to a test, is
ephemeral (does not survive a node restart), but allows more fine
grained control.

In both cases intercepts can be disabled by adding the following to
your config.  By default intercepts will be loaded and compiled, but
not added.  That is, they will be available but not in effect unless
you add them via one of the methods listed previously.

    {load_intercepts, false}

#### Config

Here is how you would add the `dropped_put` intercept via the config.

    {intercepts, [{riak_kv_vnode, [{{put,7}, dropped_put}]}]}

Breaking this down, the config key is `intercepts` and its value is a
list of intercepts to add.  Each intercept definition in the list
describes which functions to intercept and what functions to intercept
them with.  The example above would result in all calls to
`riak_kv_vnode:put/7` being intercepted by
`riak_kv_vnode_intercepts:dropped_put/7`.

    {ModuleToIntercept, [{{FunctionToIntercept, Arity}, InterceptFunction}]}

#### Manual

To add the `dropped_put` intercept manually you would do the following.

    `rt_intercept:add(Node, {riak_kv_vnode, [{{put,7}, dropped_put}]})`

### How Does it Work?

Knowing the implementation details is not needed to use intercepts but
this knowledge could come in handy if problems are encountered.  There
are two parts to understand: 1) how the intercept code works and 2)
how intercepts are applied on-the-fly in Riak Test.  It's important to
keep one thing in mind.

> Intercepts are based entirely on code generation and hot-swapping.
> The overhead of an intercept is always 1 or 2 function calls.  1 if
> a function is not being intercepted, 2 if it is and you call the
> original function.

The intercept code turns your original module into three.  Based on
the mapping passed to `intercept:add` code is generated to re-route
requests to your intercept code or forward them to the original code.
E.g. if defining intercepts on `riak_kv_vnode` the following modules
will exist.

* `riak_kv_vnode_orig` - Contains the original code from
  `riak_kv_vnode` but modified so that all original functions have the
  suffix `_orig` added to them and the original function definitions
  become passthrus to `riak_kv_vnode`, the proxy.

* `riak_kv_vnode_intercepts` - This contains code of your intercept as
  you defined it.  No modification of the code is performed.

* `riak_kv_vnode` - What once contained the original code is now a
  proxy.  All functions passthru to `riak_kv_vnode_orig`.  Unless an
  intercept is registered in the mapping passed to `intercept:add`.
  In that case the call will forward to `riak_kv_vnode_intercepts`.

The interceptor code also modifies the original module and proxy to
export all functions.  This fact, along with the fact that all the
original functions in `riak_kv_vnode_orig` will callback into the
proxy, means that you can intercept private functions as well.

In order for Riak Test to use intercepts they need to be compiled,
loaded, and registered on the nodes under test.  You can't use the
bytecode generated by Riak Tests' rebar because the Erlang version
used will often be different from that included with your Riak nodes.
You could require that the user compile with the oldest Erlang version
supported but that is extra burden on the user and still doesn't
guarantee things will work if there is a jump of more than 2 majors in
Erlang version.  No, this should be easy to use and thus the intercept
code is compiled **on** the Riak nodes guaranteeing that the bytecode
will be compatible.

After the code is compiled and loaded the intercepts need to be added.
All intercepts defined in the user's `riak_test.config` will be added
automatically any time a node is started.  Thus, intercepts defined in
the config survive restarts and are essentially always in play.  A
user can also manually add an intercept by making an `rpc` call from
the test code to the remote node.  This method is ephemeral and the
intercept will not survive restarts.
