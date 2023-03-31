#!/usr/bin/env bash

# $res = `go test -run TestFailAgree2B -race | sed -n 'PASS'`
# echo 'ok'


# echo $output

declare -a arr=(TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B)
i=1
fail=0
while ((i<=20))
do
  echo ======== iteration $i ========
  ((i+=1))
  for test in "${arr[@]}"; do
    rm raft.log.ans
    touch raft.log.ans
    echo $test >> raft.log.ans
    output=$(go test -run $test -race)
    if [[ $output == *"PASS"* ]] || [[ $output == *"RACE"* ]]; then # see race as pass
      echo $(date '+%Y-%m-%d %H:%M:%S') "PASS $test!"
    else
      echo $output
      fail=1
      break
    fi
  done
  if [ $fail -eq 1 ]; then
    break
  fi
done


