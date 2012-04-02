rm -rf /tmp/rt
mkdir /tmp/rt
cp -a dev /tmp/rt
cd /tmp/rt/dev
git init
git add .
git commit -a -m "riak_test init"
