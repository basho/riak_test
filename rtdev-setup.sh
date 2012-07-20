rm -rf /tmp/rt
mkdir /tmp/rt
cp -a dev /tmp/rt
cd /tmp/rt
git init
git add .
git commit -a -m "riak_test init"
