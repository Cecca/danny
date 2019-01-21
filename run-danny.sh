#!/bin/bash

ARGS=$@
DANNY_EXEC=~/.cargo/bin/danny
RUST_LOG=${RUST_LOG:-warn}
DANNY_SEED=${DANNY_SEED:-2879349}
RUN_DIR=$(pwd)

function run_remotely () {
  ID=$1
  HOST_PAIR=$2
  IFS=: read -ra HOST <<< "$HOST_PAIR"
  HOST=${HOST[0]}

  ssh -n $HOST \
    "cd $RUN_DIR && DANNY_BASELINES_PATH=$DANNY_BASELINES_PATH DANNY_SEED=$DANNY_SEED RUST_LOG=$RUST_LOG DANNY_PROCESS_ID=$ID DANNY_HOSTS=$DANNY_HOSTS DANNY_THREADS=$DANNY_THREADS $DANNY_EXEC $ARGS"
}


DANNY_THREADS=${DANNY_THREADS:-1}
if [ -z "$DANNY_HOSTS" ]
then
  (>&2 echo "You should set the variable DANNY_HOSTS")
  exit 1
fi

HOSTS=$(echo $DANNY_HOSTS | tr ',' '\n')

declare -a PIDS
ID=1
for HOST_PAIR in $HOSTS
do
  sleep 5
  echo "Spinning up host $ID ( $HOST_PAIR )"
  # Detach from remote processes
  run_remotely $ID $HOST_PAIR &
  PIDS[$(( $ID - 1 ))]=$!
  ID=$(( $ID + 1 ))
done

# wait for all pids
for pid in ${pids[*]}; do
  wait $pid
done
echo "Done"

