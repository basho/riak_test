#!/usr/bin/env bash

SPARK1_LOG_FILE=${1}
SPARK2_LOG_FILE=${2}
if [ -z ${3} ]; then
  TIMEOUT=60
else 
  TIMEOUT=${3}
fi
ATTEMPTS=3

a=1

while [ $a -le "$ATTEMPTS" ]
do
  a=$(($a+1))
  if [ -f $SPARK1_LOG_FILE ] && [ -f $SPARK2_LOG_FILE ]; then
    LOG_STR=$(grep "RiakEnsembleLeaderElectionAgent: We have gained leadership" $SPARK1_LOG_FILE | tail -1)
    if [ -n "$LOG_STR" ]; then
      leader_log=$SPARK1_LOG_FILE
      leader_pid=`ps -ef | grep 'org.apache.spark.deploy.master.Master' | grep 'Dspark' | grep dev1 | awk '{print $2}'`
      standby_log=$SPARK2_LOG_FILE
      break
    else
      LOG_STR=$(grep "RiakEnsembleLeaderElectionAgent: We have gained leadership" $SPARK2_LOG_FILE | tail -1)
      if [ -n "$LOG_STR" ]; then
        leader_log=$SPARK2_LOG_FILE
        leader_pid=`ps -ef | grep 'org.apache.spark.deploy.master.Master' | grep 'Dspark' | grep dev2 | awk '{print $2}'`
        standby_log=$SPARK1_LOG_FILE
        break
      fi
    fi
  fi
  sleep $TIMEOUT
done

if [ -z "$leader_pid" ]; then
  echo "No leader found"
  exit 1
fi


if [[ $(ps -p $leader_pid -o comm=) =~ "java" ]]; then
   kill $leader_pid
fi

sleep $TIMEOUT

LOG_STR=$(grep "Master: I have been elected leader!" $standby_log | tail -1)
if [ -n "$LOG_STR" ]; then
  echo ok
else
  echo "New leader wasn't elected"
  exit 1
fi
