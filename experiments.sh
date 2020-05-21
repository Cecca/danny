#!/bin/bash

set -e 

function scalability() {
for BASE_DATA in sift-100nn-0.5 Livejournal Orkut Glove
do
  for SIZE_SPEC in sample-200000 sample-400000 sample-800000 inflated-2 inflated-5
  do
    for K in 9 10 11 12 13
    do
      DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA-$SIZE_SPEC.bin
      echo "Running on $DATASET"
      danny \
        --hosts ~/hosts.txt \
        --threads 8 \
        --threshold 0.5 \
        --algorithm one-round-lsh \
        --sketch-bits 256 \
        --k $K \
        $DATASET $DATASET
    done
  done
done
  
# full, original datasets
for DATA in glove.twitter.27B.200d.bin Orkut.bin Livejournal.bin sift-100-0.5.bin
do
  for K in 9 10 11 12 13
  do
    DATASET=/mnt/fast_storage/users/mcec/$DATA
    echo "Running on $DATASET"
    danny \
      --hosts ~/hosts.txt \
      --threads 8 \
      --threshold 0.5 \
      --algorithm one-round-lsh \
      --sketch-bits 256 \
      --k $K \
      $DATASET $DATASET
  done
done

}

scalability

