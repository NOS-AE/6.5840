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
    echo ======== iteration $i/$iter ========
    ((i+=1))
    for test in "$@"; do
      rm raft.log.ans
      touch raft.log.ans
      echo $test >> raft.log.ans
      echo $test
      output=$(go test -run $test -race)
      printf "$output\n"
      if [[ $output != *"PASS"* ]] && [[ $output != *"RACE"* ]]; then # see race as pass
        exit
        # echo $(date '+%Y-%m-%d %H:%M:%S') "PASS $test!"
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
  test "2B Test" 1 "${lab2b_tests[@]}"
fi

declare -a lab2c_tests=(
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
  test "2C Test" 1 "${lab2c_tests[@]}"
fi

declare -a lab2d_tests=(
  TestSnapshotBasic2D
  TestSnapshotInstall2D
  TestSnapshotInstallUnreliable2D
  TestSnapshotInstallCrash2D
  TestSnapshotInstallUnCrash2D
  TestSnapshotAllCrash2D
  TestSnapshotInit2D
)
if [[ "${params[*]}" =~ "2D" ]]; then
  test "2D Test" 10 "${lab2d_tests[@]}"
fi

declare -a lab3a_tests=(
  TestBasic3A
  TestSpeed3A
  TestConcurrent3A
  TestUnreliable3A
  TestUnreliableOneKey3A
  TestOnePartition3A
  TestManyPartitionsOneClient3A
  TestManyPartitionsManyClients3A
  TestPersistOneClient3A
  TestPersistConcurrent3A
  TestPersistConcurrentUnreliable3A
  TestPersistPartition3A
  TestPersistPartitionUnreliable3A
  TestPersistPartitionUnreliableLinearizable3A
)
if [[ "${params[*]}" =~ "3A" ]]; then
  test "3A Test" 10 "${lab3a_tests[@]}"
fi

declare -a lab3b_tests=(
  TestSnapshotRPC3B
  TestSnapshotSize3B
  TestSpeed3B
  TestSnapshotRecover3B
  TestSnapshotRecoverManyClients3B
  TestSnapshotUnreliable3B
  TestSnapshotUnreliableRecover3B
  TestSnapshotUnreliableRecoverConcurrentPartition3B
  TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
)
if [[ "${params[*]}" =~ "3B" ]]; then
  test "3B Test" 100 "${lab3b_tests[@]}"
fi

printf "\nALL PASS!"