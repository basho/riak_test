#!/bin/bash
sync
sync
sync
sleep 5
echo 3 >/proc/sys/vm/drop_caches
