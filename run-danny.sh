#!/bin/bash

ARGS=$@
DANNY_EXEC=danny

DANNY_THREADS=${DANNY_THREADS:-1}
if [ -z "$DANNY_HOSTS" ]
then
  (>&2 echo "You should set the variable DANNY_HOSTS")
  exit 1
fi

ID=0
for HOST_PAIR in $(echo $DANNY_HOSTS | tr ',' ' ')
do
  IFS=: read -ra HOST <<< "$HOST_PAIR"
  HOST=${HOST[0]}

  ssh $HOST \
    DANNY_PROCESS_ID=$ID DANNY_HOSTS=$DANNY_HOSTS DANNY_THREADS=$DANNY_THREADS $DANNY_EXEC $ARGS

  ID=$(( $ID + 1 ))
done
