#!/usr/bin/env bash

# usage: sh test-single.sh [test1_name iter1 test2_name iter2...]

function test {
  echo $1
  shift
  local iter=$1
  shift
  local i=1
  while ((i<=$iter))
  do
    echo ======== iteration $i ========
    ((i+=1))
    for test in "$@"; do
      rm raft.log.ans
      touch raft.log.ans
      echo $test >> raft.log.ans
      output=$(go test -run $test -race)
      if [[ $output == *"PASS"* ]] || [[ $output == *"RACE"* ]]; then # see race as pass
        echo $(date '+%Y-%m-%d %H:%M:%S') "PASS $test!"
      else
        echo $output
        exit # fail then exit
        break
      fi
    done
  done
}

params="$@"
while true; do
  test $1 $2 $1
  shift; shift
  params="$@"
  if [ -z "${params}" ]; then
    break
  fi
done
# while [ ${#params[@]} != 0 ]; do
#   echo $1 $2
#   shift; shift
#   params="$@"
# done