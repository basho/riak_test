#!/usr/bin/env bash

SPARK1_LOG_FILE=${1}
SPARK2_LOG_FILE=${2}
if [ -z ${3} ]; then
  TIMEOUT=60
else 
  TIMEOUT=${3}
fi

SPARK1_PID=`ps -ef | grep 'org.apache.spark.deploy.master.Master' | grep 'Dspark' | grep dev1 | awk '{print $2}'`
SPARK2_PID=`ps -ef | grep 'org.apache.spark.deploy.master.Master' | grep 'Dspark' | grep dev2 | awk '{print $2}'`

LOG_STR=$(grep "RiakEnsembleLeaderElectionAgent: We have gained leadership" "$SPARK1_LOG_FILE" | tail -1)
if [ -n "$LOG_STR" ]; then
  worker_str=$(grep "Registering worker" "$SPARK1_LOG_FILE" | tail -1)
  if [ -n "$worker_str" ]; then 
    leader_log=$SPARK1_LOG_FILE
    leader_pid=$SPARK1_PID
    standby_log=$SPARK2_LOG_FILE
  else 
    echo "No worker registered with master"
    exit 1
  fi
else
  LOG_STR=$(grep "RiakEnsembleLeaderElectionAgent: We have gained leadership" "$SPARK2_LOG_FILE" | tail -1)
  if [ -n "$LOG_STR" ]; then
    worker_str=$(grep "Registering worker" "$SPARK2_LOG_FILE" | tail -1)
    if [ -n "$worker_str" ]; then
      leader_log=$SPARK2_LOG_FILE
      leader_pid=$SPARK2_PID
      standby_log=$SPARK1_LOG_FILE
    else
      echo "No worker registered with master"
      exit 1
    fi
  else
    echo "No leader found"
    exit 1
  fi
fi

if [[ $(ps -p "$leader_pid" -o comm=) =~ "java" ]]; then
   kill "$leader_pid"
fi

sleep $TIMEOUT

LOG_STR=$(grep "Master: I have been elected leader!" "$standby_log" | tail -1)
if [ -n "$LOG_STR" ]; then
   worker_str=$(grep "Worker has been re-registered" "$standby_log" | tail -1)
    if [ -n "$worker_str" ]; then
      echo ok
    else 
      echo "Worker wasn't re-registered"
      exit 1
    fi
else
  echo "New leader wasn't elected"
  exit 1
fi
