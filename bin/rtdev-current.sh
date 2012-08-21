cwd=$(pwd)
cd /tmp/rt
git reset HEAD --hard
git clean -fd
rm -rf /tmp/rt/current
mkdir /tmp/rt/current
cd $cwd
cp -a dev /tmp/rt/current
cd /tmp/rt
git add .
git commit -a -m "riak_test init" --amend


