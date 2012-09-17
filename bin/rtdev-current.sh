echo "Making $(pwd) the current release:"
cwd=$(pwd)
cd /tmp/rt
echo " - Resetting existing /tmp/rt"
git reset HEAD --hard > /dev/null 2>&1
git clean -fd > /dev/null 2>&1
echo " - Removing and recreating /tmp/rt/current"
rm -rf /tmp/rt/current
mkdir /tmp/rt/current
cd $cwd
echo " - Copying devrel to /tmp/rt/current"
cp -a dev /tmp/rt/current
cd /tmp/rt
echo " - Reinitializing git state"
git add .
git commit -a -m "riak_test init" --amend > /dev/null 2>&1
echo "Done!"
