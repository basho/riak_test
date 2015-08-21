#!/usr/bin/env bash

SPARK1_LOG_FILE=${1}
SPARK2_LOG_FILE=${2}

SPARK1_PID_FILE=${3}
SPARK2_PID_FILE=${4}

LOG_STR=$(grep "RiakEnsembleLeaderElectionAgent: We have gained leadership" "$SPARK1_LOG_FILE" | tail -1)
if [ -n "$LOG_STR" ]; then
  leader_log=$SPARK1_LOG_FILE
  leader_pid=$SPARK1_PID_FILE
  standby_log=$SPARK2_LOG_FILE
else
  LOG_STR=$(grep "RiakEnsembleLeaderElectionAgent: We have gained leadership" "$SPARK2_LOG_FILE" | tail -1)
  if [ -n "$LOG_STR" ]; then
    leader_log=$SPARK2_LOG_FILE
    leader_pid=$SPARK2_PID_FILE
    standby_log=$SPARK1_LOG_FILE
  else
    echo "No leader found"
    exit 1
  fi
fi

if [ -f "$leader_pid" ]; then
  PID="$(cat $leader_pid)"
  if [[ $(ps -p "$PID" -o comm=) =~ "java" ]]; then
     kill "$PID" && rm -f "$leader_pid"
  fi
else
  echo "No pid file found"
  exit 1
fi

sleep 60

LOG_STR=$(grep "Master: I have been elected leader!" "$standby_log" | tail -1)
if [ -n "$LOG_STR" ]; then
  echo ok
else
  echo "New leader wasn't elected"
  exit 1
fi
