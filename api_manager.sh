#!/bin/bash

cmd="$1"

python_cmd="/usr/local/anaconda3/bin/python "
echo $python_cmd
script_cmd="slitmask_api.py slitmask_cfg.ini"
echo $script_cmd
start_cmd=$python_cmd$script_cmd
echo $start_cmd

pid=$(ps -eaf | grep "$script_cmd" | grep -v "grep" | grep -v "ps -eaf" | awk '{print $2}')


if [ -n "$pid" ]; then
  if [ "$cmd" == "stop" ]; then
    echo "stopping $pid"
    kill $pid
  elif [ "$cmd" == "restart" ]; then
    echo "stopping $pid"
    kill $pid
    wait
    echo "starting $start_cmd"
    $start_cmd &
  elif [ "$cmd" == "start" ]; then
    echo "already running $start_cmd: $pid"
  else
    echo "command $cmd not found"
  fi
else
  if [ "$cmd" == "restart" ] || [ "$cmd" == "start" ]; then
    echo "starting $start_cmd"
    $start_cmd &
  elif [ "$cmd" == "stop" ]; then
    echo "no processes to stop"
  else
    echo "command $cmd not found"
  fi
fi




