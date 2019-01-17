#!/bin/bash

ARGS=$@
DANNY_EXEC=~/.cargo/bin/danny
RUST_LOG=${RUST_LOG:-warn}
DANNY_SEED=${DANNY_SEED:-2879349}

function run_remotely () {
  ID=$1
  HOST_PAIR=$2
  IFS=: read -ra HOST <<< "$HOST_PAIR"
  HOST=${HOST[0]}

  ssh -n $HOST \
    "DANNY_SEED=$DANNY_SEED RUST_LOG=$RUST_LOG DANNY_PROCESS_ID=$ID DANNY_HOSTS=$DANNY_HOSTS DANNY_THREADS=$DANNY_THREADS $DANNY_EXEC $ARGS"
}


DANNY_THREADS=${DANNY_THREADS:-1}
if [ -z "$DANNY_HOSTS" ]
then
  (>&2 echo "You should set the variable DANNY_HOSTS")
  exit 1
fi

FIRST_HOST=$(echo $DANNY_HOSTS | tr ',' '\n' | head -n1)
OTHER_HOSTS=$(echo $DANNY_HOSTS | tr ',' '\n' | sed 1d)

ID=1
for HOST_PAIR in $OTHER_HOSTS
do
  echo "Spinning up host $ID ( $HOST_PAIR )"
  # Detach from remote processes
  run_remotely $ID $HOST_PAIR &
  ID=$(( $ID + 1 ))
done

echo "Spinning up host 0 ( $FIRST_HOST )"
run_remotely 0 $FIRST_HOST
echo "Done"
