#!/bin/bash

# set -e 

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
        --algorithm cartesian \
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
        --algorithm cartesian \
        $DATASET
    done
  done
}

# This set of experiments searches for the best configuration of parameters for all the different datasets.
function search_best() {
  RECALL=0.8 # <-- required recall

  for BASE_DATA in sift-100-0.5 #Livejournal Orkut glove.twitter.27B.200d
  do
    DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA.bin
    # DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA-sample-200000.bin
    echo "Running on $DATASET"
    test -d $DATASET

    for THRESHOLD in 0.5 # 0.7
    do
      for SKETCH_BITS in 0 #256 512 #0 64 128 256 512
      do
        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm cartesian \
          --sketch-bits $SKETCH_BITS \
          $DATASET

        if [[ $DATASET =~ "glove" ]]; then
          TIMEOUT=8000
        elif [[ $DATASET =~ "sift" ]]; then
          TIMEOUT=3500
        else 
          TIMEOUT=12000
        fi
        echo "Timeout is $TIMEOUT"

        for K in 12 #3 4 6 8 #10 12 14 16
        do
          for ALGORITHM in one-level-lsh #local-lsh one-level-lsh
          do
            danny \
              --timeout $TIMEOUT \
              --hosts ~/hosts.txt \
              --threads 8 \
              --threshold $THRESHOLD \
              --algorithm $ALGORITHM \
              --recall $RECALL \
              --sketch-bits $SKETCH_BITS \
              --k $K \
              $DATASET
          done

          # for K2 in 4
          # do
          #   danny \
          #     --timeout $TIMEOUT \
          #     --hosts ~/hosts.txt \
          #     --threads 8 \
          #     --threshold $THRESHOLD \
          #     --algorithm two-level-lsh \
          #     --recall $RECALL \
          #     --sketch-bits $SKETCH_BITS \
          #     --k $K \
          #     --k2 $K2 \
          #     --repetition-batch 10000 \
          #     $DATASET
          # done
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
        --algorithm cartesian \
        $DATASET

      for K in 4 6 8
      do
        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm "one-level-lsh" \
          --sketch-bits 256 \
          --k $K \
          $DATASET
        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm "local-lsh" \
          --sketch-bits 256 \
          --k $K \
          $DATASET
        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm "two-level-lsh" \
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
      for K in 3 4 6 8
      do
        DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA-sample-200000.bin
        echo "Running on $DATASET"
        test -d $DATASET
        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm "local-lsh" \
          --recall $RECALL \
          --sketch-bits 0 \
          --k $K \
          $DATASET

        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm "one-level-lsh" \
          --recall $RECALL \
          --sketch-bits 0 \
          --k $K \
          $DATASET

        danny \
          --hosts ~/hosts.txt \
          --threads 8 \
          --threshold $THRESHOLD \
          --algorithm "two-level-lsh" \
          --recall $RECALL \
          --sketch-bits 0 \
          --k $K \
          --k2 6 \
          --repetition-batch 10000 \
          $DATASET
      done
    done
  done
done
}

function scalability_full() {
for SEED in 124351 12345 1451234 #1345 62345632 452345 2345 1456 12987 37568912 495
do
  for BASE_DATA in Livejournal Orkut #glove.twitter.27B.200d sift-100-0.5 
  do
    for HOSTS in 1 2 3 4 5
    do
      DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA.bin
      HOSTS_FILE=~/hosts$HOSTS.txt
      echo "Running on $DATASET"
      test -d $DATASET

      for K in 3 4 5 6 7 8
      do
        danny \
          --seed $SEED \
          --hosts $HOSTS_FILE \
          --threads 8 \
          --threshold 0.5 \
          --algorithm local-lsh \
          --sketch-bits 256 \
          --k $K \
          $DATASET

        danny \
          --seed $SEED \
          --hosts $HOSTS_FILE \
          --threads 8 \
          --threshold 0.5 \
          --algorithm two-level-lsh \
          --sketch-bits 256 \
          --k $K \
          --k2 6 \
          --repetition-batch 10000 \
          $DATASET

        danny \
          --seed $SEED \
          --hosts $HOSTS_FILE \
          --threads 8 \
          --threshold 0.5 \
          --algorithm one-level-lsh \
          --sketch-bits 256 \
          --k $K \
          $DATASET
        done
    done
  done
done
}

function scalability() {
for BASE_DATA in Livejournal Orkut Glove #sift-100nn-0.5
do
  for HOSTS in 1 2 3 4
  do
    for SEED in 124351 12345 1451234 #1345 62345632 452345 2345 1456 12987 37568912 495
    do
      DATASET=/mnt/fast_storage/users/mcec/$BASE_DATA-sample-200000.bin
      HOSTS_FILE=~/hosts$HOSTS.txt
      echo "Running on $DATASET"
      test -d $DATASET

      for K in 3 4 5 6 7 8
      do
        danny \
          --seed $SEED \
          --hosts $HOSTS_FILE \
          --threads 8 \
          --threshold 0.5 \
          --algorithm local-lsh \
          --sketch-bits 256 \
          --k $K \
          $DATASET

        danny \
          --seed $SEED \
          --hosts $HOSTS_FILE \
          --threads 8 \
          --threshold 0.5 \
          --algorithm two-level-lsh \
          --sketch-bits 256 \
          --k $K \
          --k2 6 \
          --repetition-batch 10000 \
          $DATASET

        danny \
          --seed $SEED \
          --hosts $HOSTS_FILE \
          --threads 8 \
          --threshold 0.5 \
          --algorithm one-level-lsh \
          --sketch-bits 256 \
          --k $K \
          $DATASET
        done
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
      --algorithm local-lsh \
      --sketch-bits 256 \
      --k $K \
      $DATASET
  done
done

}

function profiling() {

  DATASET=/mnt/fast_storage/users/mcec/sift-100nn-0.5-sample-200000.bin
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm local-lsh \
    --sketch-bits 512 \
    --k 8 \
    $DATASET
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm two-level-lsh \
    --sketch-bits 512 \
    --k 6 \
    --k2 6 \
    --repetition-batch 10000 \
    $DATASET
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm one-level-lsh \
    --sketch-bits 512 \
    --k 6 \
    $DATASET

  DATASET=/mnt/fast_storage/users/mcec/Glove-sample-200000.bin
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm local-lsh \
    --sketch-bits 512 \
    --k 8 \
    $DATASET
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm two-level-lsh \
    --sketch-bits 512 \
    --k 6 \
    --k2 6 \
    --repetition-batch 10000 \
    $DATASET
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm one-level-lsh \
    --sketch-bits 512 \
    --k 6 \
    $DATASET

  DATASET=/mnt/fast_storage/users/mcec/Orkut-sample-200000.bin
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm local-lsh \
    --sketch-bits 256 \
    --k 8 \
    $DATASET
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm two-level-lsh \
    --sketch-bits 128 \
    --k 4 \
    --k2 6 \
    --repetition-batch 10000 \
    $DATASET
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm one-level-lsh \
    --sketch-bits 128 \
    --k 4 \
    $DATASET

  DATASET=/mnt/fast_storage/users/mcec/Livejournal-sample-200000.bin
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm local-lsh \
    --sketch-bits 256 \
    --k 8 \
    $DATASET
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm two-level-lsh \
    --sketch-bits 128 \
    --k 4 \
    --k2 6 \
    --repetition-batch 10000 \
    $DATASET
  danny \
    --profile 9998 \
    --hosts ~/hosts.txt \
    --threads 8 \
    --threshold 0.5 \
    --recall 0.8 \
    --algorithm one-level-lsh \
    --sketch-bits 128 \
    --k 4 \
    $DATASET
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

  scalability_full)
    scalability_full
    ;;

  full)
    full_size
    ;;

  profiling)
    profiling
    ;;
esac
