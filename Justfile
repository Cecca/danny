deploy:
  ansible-playbook -i cluster.hosts deploy.yml

run-less:
  [ ! -f /danny.out ] || rm /tmp/danny.out
  cargo run > /tmp/danny.out || true
  less /tmp/danny.out

bench-cosine:
  cargo build --release
  LEFT=~/Datasets/GoogleWords/google-10k-left.txt RIGHT=~/Datasets/GoogleWords/google-10k-right.txt \
    hyperfine --min-runs 3 --parameter-scan num_threads 1 4 \
    'cargo run --release -- {num_threads} cosine 0.2 $LEFT $RIGHT'

bench-jaccard:
  cargo build --release
  hyperfine --min-runs 5 --parameter-scan num_threads 1 4 \
    'cargo run --release -- {num_threads} jaccard 0.5 ~/Datasets/Wikipedia/wiki-bow-10k-left.txt ~/Datasets/Wikipedia/wiki-bow-10k-right.txt'

