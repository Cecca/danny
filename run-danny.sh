#!/bin/bash

ARGS=$@
DANNY_EXEC=~/.cargo/bin/danny
RUST_LOG=${RUST_LOG:-warn}
DANNY_SEED=${DANNY_SEED:-2879349}
DANNY_SKETCH_EPSILON=${DANNY_SKETCH_EPSILON:-0.01}
DANNY_BATCH_SIZE=${DANNY_BATCH_SIZE:-1000000}
DANNY_BLOOM_ELEMENTS=${DANNY_BLOOM_ELEMENTS:-30}
DANNY_BLOOM_FPP=${DANNY_BLOOM_FPP:-0.05}
DANNY_REPETITION_BATCH=${DANNY_REPETITION_BATCH:-1}
DANNY_RECALL=${DANNY_RECALL:-0.5}
DANNY_NO_DEDUP=${DANNY_NO_DEDUP:-false}
DANNY_NO_VERIFY=${DANNY_NO_VERIFY:-false}
RUN_DIR=$(pwd)

function run_remotely () {
  ID=$1
  HOST_PAIR=$2
  IFS=: read -ra HOST <<< "$HOST_PAIR"
  HOST=${HOST[0]}

  ssh -n $HOST \
    "cd $RUN_DIR && DANNY_NO_VERIFY=$DANNY_NO_VERIFY DANNY_NO_DEDUP=$DANNY_NO_DEDUP DANNY_RECALL=$DANNY_RECALL RUST_LOG=$RUST_LOG DANNY_REPETITION_BATCH=$DANNY_REPETITION_BATCH DANNY_BLOOM_FPP=$DANNY_BLOOM_FPP DANNY_BLOOM_ELEMENTS=$DANNY_BLOOM_ELEMENTS DANNY_BATCH_SIZE=$DANNY_BATCH_SIZE DANNY_SKETCH_EPSILON=$DANNY_SKETCH_EPSILON DANNY_BASELINES_PATH=$DANNY_BASELINES_PATH DANNY_SEED=$DANNY_SEED RUST_LOG=$RUST_LOG DANNY_PROCESS_ID=$ID DANNY_HOSTS=$DANNY_HOSTS DANNY_THREADS=$DANNY_THREADS $DANNY_EXEC $ARGS 2> /tmp/danny.log"
}


DANNY_THREADS=${DANNY_THREADS:-1}
if [ -z "$DANNY_HOSTS" ]
then
  (>&2 echo "You should set the variable DANNY_HOSTS")
  exit 1
fi

HOSTS=$(echo $DANNY_HOSTS | tr ',' '\n')

declare -a PIDS
ID=0
for HOST_PAIR in $HOSTS
do
  sleep 1
  echo "Spinning up host $ID ( $HOST_PAIR )"
  # Detach from remote processes
  run_remotely $ID $HOST_PAIR &
  PIDS[${ID}]=$!
  ID=$(( $ID + 1 ))
done

# wait for all pids
for pid in ${PIDS[*]}; do
  wait $pid
done
echo "Done"

