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
        $DATASET
    done
  done

  for BASE_DATA in sift-100-0.5 Livejournal Orkut glove.twitter.27B.200d
  do
    for THRESHOLD in 0.5 0.7 0.9
    do
      DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA.bin
      echo "Running on $DATASET"
      test -d $DATASET
      danny \
        --hosts ~/hosts.txt \
        --threads 8 \
        --threshold $THRESHOLD \
        --algorithm all-2-all \
        $DATASET
    done
  done
}

# This set of experiments searches for the best configuration of parameters for all the different datasets.
# We don't look at the 
function search_best() {
  RECALL=0.8 # <-- required recall

  for BASE_DATA in sift-100nn-0.5 Livejournal Orkut Glove
  do
    DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA-sample-200000.bin
    echo "Running on $DATASET"
    test -d $DATASET

    for THRESHOLD in 0.7 0.5
    do
      for SKETCH_BITS in 0 64 128 256 512
      do
        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm all-2-all \
          --sketch-bits $SKETCH_BITS \
          $DATASET

        for K in 4 8 6 12
        do
          for ALGORITHM in one-round-lsh hu-et-al
          do
            danny \
              --hosts ~/hosts.txt \
              --threads 8 \
              --threshold $THRESHOLD \
              --algorithm $ALGORITHM \
              --recall $RECALL \
              --sketch-bits $SKETCH_BITS \
              --k $K \
              $DATASET
          done

          for K2 in 2 4 6 8
          do
            danny \
              --hosts ~/hosts.txt \
              --threads 8 \
              --threshold $THRESHOLD \
              --algorithm two-round-lsh \
              --recall $RECALL \
              --sketch-bits $SKETCH_BITS \
              --k $K \
              --k2 $K2 \
              --repetition-batch 10000 \
              $DATASET
          done
        done
      done
    done
  done

}

function full_size() {
  for BASE_DATA in sift-100-0.5 Livejournal Orkut glove.twitter.27B.200d
  do
    for THRESHOLD in 0.5
    do
      DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA.bin
      echo "Running on $DATASET"
      test -d $DATASET
      danny \
        --hosts ~/hosts.txt \
        --threads 8 \
        --threshold $THRESHOLD \
        --algorithm all-2-all \
        $DATASET

      for K in 4 6 8
      do
        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm "hu-et-al" \
          --sketch-bits 256 \
          --k $K \
          $DATASET
        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm "one-round-lsh" \
          --sketch-bits 256 \
          --k $K \
          $DATASET
        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm "two-round-lsh" \
          --sketch-bits 256 \
          --repetition-batch 100000 \
          --k $K \
          --k2 6 \
          $DATASET
      done
    done
  done
}

# This set of experiments studies the effect of different _required recalls_.
# We look at the actual recall that we get when asking for a specific one.
# Sketching is disabled in this set of experiments in order not to pollute
# the recall measurement
function recall() {
for RECALL in 0.5 0.8 0.9
do
  for BASE_DATA in sift-100nn-0.5 Livejournal Orkut Glove
  do
    for THRESHOLD in 0.5
    do
      DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA-sample-200000.bin
      echo "Running on $DATASET"
      test -d $DATASET
      danny \
        --hosts ~/hosts.txt \
        --threads 8 \
        --threshold $THRESHOLD \
        --algorithm "one-round-lsh" \
        --recall $RECALL \
        --sketch-bits 0 \
        --k 8 \
        $DATASET

      danny \
        --hosts ~/hosts.txt \
        --threads 8 \
        --threshold $THRESHOLD \
        --algorithm "hu-et-al" \
        --recall $RECALL \
        --sketch-bits 0 \
        --k 8 \
        $DATASET

      danny \
        --hosts ~/hosts.txt \
        --threads 8 \
        --threshold $THRESHOLD \
        --algorithm "two-round-lsh" \
        --recall $RECALL \
        --sketch-bits 0 \
        --k 8 \
        --k2 6 \
        --repetition-batch 10000 \
        $DATASET
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
        $DATASET
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
      $DATASET
  done
done

}

function duplicate_removal_cost() {
  K=8
  K2=6
  for BASE_DATA in sift-100nn-0.5 Livejournal Orkut Glove
  do
    DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA-sample-200000.bin
    for ALGORITHM in one-round-lsh hu-et-al
    do
      echo "Running on $DATASET"
      test -d $DATASET
      danny \
        --hosts ~/hosts.txt \
        --threads 8 \
        --threshold 0.5 \
        --algorithm $ALGORITHM \
        --sketch-bits 0 \
        --no-verify \
        --k $K \
        $DATASET

      danny \
        --hosts ~/hosts.txt \
        --threads 8 \
        --threshold 0.5 \
        --algorithm $ALGORITHM \
        --sketch-bits 0 \
        --no-dedup \
        --k $K \
        $DATASET

      danny \
        --hosts ~/hosts.txt \
        --threads 8 \
        --threshold 0.5 \
        --algorithm $ALGORITHM \
        --sketch-bits 0 \
        --no-verify \
        --no-dedup \
        --k $K \
        $DATASET
    done
    ALGORITHM="two-round-lsh"
    echo "Running on $DATASET"
    test -d $DATASET
    danny \
      --hosts ~/hosts.txt \
      --threads 8 \
      --threshold 0.5 \
      --algorithm $ALGORITHM \
      --sketch-bits 0 \
      --no-verify \
      --k $K \
      --k2 $K2 \
      $DATASET

    danny \
      --hosts ~/hosts.txt \
      --threads 8 \
      --threshold 0.5 \
      --algorithm $ALGORITHM \
      --sketch-bits 0 \
      --no-dedup \
      --k $K \
      --k2 $K2 \
      $DATASET

    danny \
      --hosts ~/hosts.txt \
      --threads 8 \
      --threshold 0.5 \
      --algorithm $ALGORITHM \
      --sketch-bits 0 \
      --no-verify \
      --no-dedup \
      --k $K \
      --k2 $K2 \
      $DATASET
  done
}

case $1 in
  baselines)
    baselines
    ;;

  best)
    search_best
    ;;

  recall)
    recall
    ;;

  scalability)
    scalability
    ;;

  dedup)
    duplicate_removal_cost
    ;;

  full)
    full_size
    ;;
esac
