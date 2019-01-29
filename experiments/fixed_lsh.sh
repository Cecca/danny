#!/bin/bash

# fail on subcommand failure
set -e

DATA_DIR=/mnt/fast_storage/users/mcec
# DATA_DIR=~/Datasets
export DANNY_BASELINES_PATH=$DATA_DIR/baselines.csv
RUN_DANNY=$(pwd)/../run-danny.sh
# RUN_DANNY=$(pwd)/../target/release/danny
RESULTS_DIR=results

export DANNY_HOSTS=sss00:2001,sss01:2001,sss02:2001,sss03:2001,sss04:2001
export DANNY_THREADS=8
export RUST_LOG=info

function nth {
  echo $2 | cut -d " " -f $1
}

function small() {
  declare -a DATASETS=(
    # "cosine GoogleWords/google-1k.txt GoogleWords/google-1k.txt 0.6"
    "cosine google-10k-left.txt google-10k-right.txt 0.6"
    "cosine google-100k-left.txt google-100k-right.txt 0.6"
    "cosine google-500k-left.txt google-500k-right.txt 0.6"
  )
  
  test -d $RESULTS_DIR || mkdir $RESULTS_DIR
  pushd $RESULTS_DIR

  for TUPLE in "${DATASETS[@]}"
  do
    MEASURE=`nth 1 "$TUPLE"`
    LEFT=$DATA_DIR/`nth 2 "$TUPLE"`
    RIGHT=$DATA_DIR/`nth 3 "$TUPLE"`
    THRESHOLD=`nth 4 "$TUPLE"`
    for K in 12 14 16
    do
      for SEED in 24834 
      do
        export DANNY_SEED=$SEED
        $RUN_DANNY --algorithm fixed-lsh -k $K --range $THRESHOLD --measure $MEASURE $LEFT $RIGHT
      done
    done
  done

  # declare -a DATASETS=(
  #   "jaccard wiki-10k-100k-left.txt wiki-10k-100k-right.txt 0.5"
  #   "jaccard wiki-10k-500k-left.txt wiki-10k-500k-right.txt 0.5"
  # )
  # for TUPLE in "${DATASETS[@]}"
  # do
  #   MEASURE=`nth 1 "$TUPLE"`
  #   LEFT=$DATA_DIR/`nth 2 "$TUPLE"`
  #   RIGHT=$DATA_DIR/`nth 3 "$TUPLE"`
  #   THRESHOLD=`nth 4 "$TUPLE"`
  #   for K in 1 2 3 4 5
  #   do
  #     for SEED in 24834 
  #     do
  #       export DANNY_SEED=$SEED
  #       $RUN_DANNY --algorithm fixed-lsh -k $K --range $THRESHOLD --measure $MEASURE $LEFT $RIGHT
  #     done
  #   done
  # done

  python ../json_to_table.py results.json > table.txt
  python ../plot_fixed_lsh.py results.json result

  popd

}

small
