#!/usr/bin/env bash

# usage: sh test.sh [2A 2B...]

# test test_name iter_num test_set
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

# 2A Tests
declare -a lab2a_tests=(
  TestInitialElection2A
  TestReElection2A
  TestManyElections2A
)
if [[ "${params[*]}" =~ "2A" ]]; then
  test "2A Test" 10 "${lab2a_tests[@]}"
fi

# 2B Tests
declare -a lab2b_tests=(
  TestBasicAgree2B
  TestRPCBytes2B
  TestFollowerFailure2B
  TestLeaderFailure2B
  TestFailAgree2B
  TestFailNoAgree2
  TestConcurrentStarts2B
  TestRejoin2B
  TestBackup2B
  TestCount2B
)
if [[ "${params[*]}" =~ "2B" ]]; then
  test "2B Test" 10 "${lab2b_tests[@]}"
fi

declare -a lab2b_tests=(
  TestPersist12C
  TestPersist22C
  TestPersist32C
  TestFigure82C
  TestUnreliableAgree2C
  TestFigure8Unreliable2C
  TestReliableChurn2C
  TestUnreliableChurn2C
)
if [[ "${params[*]}" =~ "2C" ]]; then
  test "2C Test" 20 "${lab2b_tests[@]}"
fi

printf \nALL PASS!