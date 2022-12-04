#!/bin/bash

cmd="curl http://127.0.0.1:2000/?x="

for ((i=1;i<=100;i++))
  do
    $cmd$i &
    sleep 1
    echo
  done
