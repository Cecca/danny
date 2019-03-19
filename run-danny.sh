#!/bin/bash

ARGS=$@
DANNY_EXEC=~/.cargo/bin/danny
RUST_LOG=${RUST_LOG:-warn}
DANNY_SEED=${DANNY_SEED:-2879349}
DANNY_SKETCH_EPSILON=${DANNY_SKETCH_EPSILON:-0.01}
DANNY_ESTIMATOR_SAMPLES=${DANNY_ESTIMATOR_SAMPLES:-100}
DANNY_BATCH_SIZE=${DANNY_BATCH_SIZE:-1000000}
RUN_DIR=$(pwd)

function run_remotely () {
  ID=$1
  HOST_PAIR=$2
  IFS=: read -ra HOST <<< "$HOST_PAIR"
  HOST=${HOST[0]}

  ssh -n $HOST \
    "cd $RUN_DIR && DANNY_BATCH_SIZE=$DANNY_BATCH_SIZE DANNY_ESTIMATOR_SAMPLES=$DANNY_ESTIMATOR_SAMPLES DANNY_SKETCH_EPSILON=$DANNY_SKETCH_EPSILON DANNY_BASELINES_PATH=$DANNY_BASELINES_PATH DANNY_SEED=$DANNY_SEED RUST_LOG=$RUST_LOG DANNY_PROCESS_ID=$ID DANNY_HOSTS=$DANNY_HOSTS DANNY_THREADS=$DANNY_THREADS $DANNY_EXEC $ARGS"
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

