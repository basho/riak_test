# Riak Test

## Quick Start

Assumes `riak_test` and `riak` and/or `riak-ee` are all checked out in the same parent folder

1. Edit your `rtdev.config` file's rt_deps url
1. `cd ../riak[-ee]`
1. `make devrel`
1. `. ../riak_test/rtdev-setup.sh`
1. `cd back to riak_test`
1. `riak_test rtdev.config verify_build_cluster`


