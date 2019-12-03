# DANNY: Distributed Approximate Near Neighbors, Yo!

## Prepare the environment

Install `rust` nightly using

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

and then

```
rustup install nightly
```

## Building all the binaries

```
cargo install --force --path danny
cargo install --force --path danny-utilities
```

## Prepare the datasets

Move to the `experiments` subdirectory and define the following two environment variables

```
DANNY_DATA_DIR=/path/to/directory/to/store/datasets
DANNY_MINIONS=hostnames,separated,by,comma,that,execute,experiments # Or localhost if you are running locally
```

The following command with download and preprocess **all** datasets, if they are not already in your machine! Takes a **long** time.

```
./run.py --list
```

If you are interested in just one dataset, edit the `run.py` and edit the `DATASETS` dictionary, removing the ones you don't need.
Also, find and comment out the following loop:

```python
for d in derived_datasets:
    DATASETS[d.name] = d
```

### Sampling a dataset

If running locally, you might find more convenient to work with a small dataset.
You can use the `sampledata` binary that was installed alongside the other utilities.
An example usage is the following, for taking 5000 points from dataset `livejournal`:

```
sampledata --measure jaccard --size 5000 $DANNY_DATA_DIR/Livejournal.bin $DANNY_DATA_DIR/Livejournal-5000.bin
```

If you are sampling from a dataset which uses the cosine distance, use `--measure cosine`.

## Running locally

You can define several environment variables to control the behavior of `danny`, which are described in `danny --help`.

Example invocation of the one round, fixed parameter LSH algorithm:

```
danny -m jaccard --algorithm lsh -k 8 --rounds one  --range 0.5 $DANNY_DATA_DIR/Livejournal-5000.bin $DANNY_DATA_DIR/Livejournal-5000.bin
```

For a list of all available options and algorithms, please consult `danny --help`.

## Hacking

If you are changing the code, you can run the modified versions without reinstalling
everything using, instead of `danny`, the command

```
cargo run --release --bin danny -- 
```

The trailing `--` is very important: after it you can put the program's arguments, before it are `cargo`'s arguments.

### Conditional compilation

The compilation takes a _long_ time. To reduce this time, during development only some parts of the code can be compiled, using cargo feature gates. In particular the following features are available:

- `one-round-lsh`
- `two-round-lsh`
- `hu-et-al`
- `all-2-all`
- `seq-all-2-all`
- `sketching`

By default they are all enabled. To disable them one can pass the `--no-default-features` flag to `cargo`, along with the `--features` flag to actually use the ones that are needed.

For instant, while modifying and testing locally the code for `two-round-lsh` one can use the command:

```
cargo run --no-default-features --features two-round-lsh --release --bin danny -- -m jaccard --algorithm two-round-lsh -k 4 -l 5 --range 0.75 --sketch-bits 0 $DANNY_DATA_DIR/Livejournal-5000.bin $DANNY_DATA_DIR/Livejournal-5000.bin
```

This command must be run inside the `danny` subdirectory, otherwise all features are built nonetheless, I don't know why. 
