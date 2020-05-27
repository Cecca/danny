#!/bin/bash

set -e 

function baselines() {
  for BASE_DATA in sift-100nn-0.5 Livejournal Orkut Glove
  do
    for THRESHOLD in 0.5 0.7 0.9
    do
      DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA-sample-200000.bin
      echo "Running on $DATASET"
      test -d $DATASET
      danny \
        --hosts ~/hosts.txt \
        --threads 8 \
        --threshold $THRESHOLD \
        --algorithm all-2-all \
        $DATASET $DATASET
    done
  done
}

function recall() {
for RECALL in 0.5 0.8 0.9
do
  for BASE_DATA in Livejournal Orkut
  # for BASE_DATA in sift-100nn-0.5 Livejournal Orkut Glove
  do
    for ALGORITHM in one-round-lsh hu-et-al
    do
      for THRESHOLD in 0.9
      do
        DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA-sample-200000.bin
        echo "Running on $DATASET"
        test -d $DATASET
        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm $ALGORITHM \
          --recall $RECALL \
          --sketch-bits 0 \
          --k 7 \
          $DATASET $DATASET
      done
    done
  done
done
}

function scalability() {
for BASE_DATA in sift-100nn-0.5 Livejournal Orkut Glove
do
  for SIZE_SPEC in sample-200000 sample-400000 sample-800000 inflated-2 inflated-5
  do
    for K in 9 10 11 12 13
    do
      DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA-$SIZE_SPEC.bin
      echo "Running on $DATASET"
      test -d $DATASET
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
    test -d $DATASET
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

baselines
recall
scalability

