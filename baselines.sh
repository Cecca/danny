for DATA in sift-100nn-0.5-sample-200000.bin Livejournal-sample-200000.bin Orkut-sample-200000.bin Glove-sample-200000.bin
do
  for THRESH in 0.5 0.7 0.9
  do
    DATASET=/mnt/fast_storage/users/mcec/data-master/$DATA
    danny \
      --hosts ~/hosts.txt \
      --threads 8 \
      --threshold $THRESH \
      --algorithm all-2-all \
      $DATASET $DATASET
  done
done
