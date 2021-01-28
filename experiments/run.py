#!/usr/bin/env python

import shlex
import numpy
import numpy as np
import datetime
import argparse
from sklearn.feature_extraction.text import CountVectorizer
from itertools import product
import subprocess
from pprint import pprint
import json
import bz2
import yaml
import jinja2
import subprocess
import glob
import zipfile
import shutil
import tempfile
import sys
import os


try:
    from urllib import urlretrieve
except ImportError:
    from urllib.request import urlretrieve  # Python 3


try:
    DATA_DIRECTORY = os.environ["DANNY_DATA_DIR"]
    print("Datasets are stored in", DATA_DIRECTORY)
except:
    print("The environment variable DANNY_DATA_DIR is not set.")
    sys.exit(1)

assert os.path.isabs(DATA_DIRECTORY), "Data directory should be an absolute path"

try:
    MINIONS = os.environ["DANNY_MINIONS"].split(",")
    print("Minions are", MINIONS)
except:
    print("The environment variable DANNY_MINIONS is not set.")
    sys.exit(2)


RUN_SCRIPT = os.environ.get("DANNY_RUN_SCRIPT", os.path.abspath("../run-danny.sh"))


def download(src, dst):
    if not os.path.exists(dst):
        print("downloading {} to {}".format(src, dst))
        urlretrieve(src, dst)


def sync():
    for host in MINIONS:
        subprocess.run(
            "rsync --progress --ignore-existing -avzr {}/*.bin {}:{}/".format(
                DATA_DIRECTORY, host, DATA_DIRECTORY
            ),
            shell=True,
        )


class Dataset(object):
    def __init__(self, name, url, local_filename, preprocess_fn):
        self.name = name
        self.url = url
        download_dir = os.path.join(DATA_DIRECTORY, "download")
        self.download_file = os.path.join(download_dir, os.path.basename(self.url))
        if not os.path.isdir(download_dir):
            os.mkdir(download_dir)
        assert not os.path.isabs(local_filename)
        self.local_filename = os.path.join(DATA_DIRECTORY, local_filename)
        self.preprocess = preprocess_fn

    def prepare(self):
        print("Preparing dataset", self.name)
        if not os.path.exists(self.download_file):
            download(self.url, self.download_file)
        self.preprocess(self.download_file, self.local_filename)
        # TODO Sync file

    def get_path(self):
        if not os.path.exists(self.local_filename):
            print("File {} missing, preparing it".format(self.local_filename))
            self.prepare()
        # sync()
        return self.local_filename


class DerivedDataset(object):

    def __init__(self, name, filename, base, preprocess_fn):
        self.name = name
        self.base = base
        self.filename = os.path.join(DATA_DIRECTORY, filename)
        self.preprocess_fn = preprocess_fn

    def prepare(self):
        print("Preparing dataset", self.name)
        self.preprocess_fn(self.base.get_path(), self.filename)

    def get_path(self):
        if not os.path.exists(self.filename):
            print("File {} missing, preparing it".format(self.filename))
            self.prepare()
        # sync()
        return self.filename


def preprocess_random(download_file, final_output):
    import sklearn.datasets

    X, _ = sklearn.datasets.make_blobs(
        n_samples=1000, n_features=50,
        centers=10, random_state=1)
    
    txt = os.path.join(os.path.dirname(download_file), "random.txt")
    with open(txt, 'w') as f:
        for x in X:
            f.write(" ".join(map(str, x)) + "\n")
    
    print("Converting {} to {}".format(txt, final_output))
    subprocess.run(
        [
            "danny_convert",
            "-i",
            txt,
            "-o",
            final_output,
            "-t",
            "vector-normalized",
            "-n",
            "40",
        ],
        check=True
    )

def preprocess_glove_6b(download_file, final_output):
    """Preprocess all datasets present in the glove archive"""
    tmp_dir = os.path.join(os.path.dirname(download_file), "glove_unzipped")
    print("Extracting zip file")
    zip_ref = zipfile.ZipFile(download_file, "r")
    zip_ref.extractall(tmp_dir)
    zip_ref.close()
    for txt in glob.glob(os.path.join(tmp_dir, "*.txt")):
        output = os.path.join(
            DATA_DIRECTORY, os.path.splitext(os.path.basename(txt))[0] + ".bin"
        )
        print("Converting {} to {}".format(txt, output))
        subprocess.run(
            [
                "danny_convert",
                "-i",
                txt,
                "-o",
                output,
                "-t",
                "vector-normalized",
                "-n",
                "40",
            ],
            check=True
        )




def _preprocess_wiki(vocab_size, download_file, final_output):
    tmp_dir = os.path.join(os.path.dirname(download_file), "wiki_preprocess")
    # run the script to convert to json the elements
    if not os.path.isdir(tmp_dir):
        print("Extracting pages")
        proc = subprocess.run(
            ["./WikiExtractor.py", "--json", download_file, "-o", tmp_dir]
        )
        proc.check_returncode()

    print("Building bag of words")
    bow_file = os.path.join(tmp_dir, "bag-of-words-{}.txt".format(vocab_size))
    vectorizer = CountVectorizer(
        stop_words="english", binary=False, max_features=vocab_size
    )
    preprocessor = vectorizer.build_analyzer()

    def iter_pages():
        for root, dirs, files in os.walk(tmp_dir):
            for f in files:
                if f.startswith("wiki_"):
                    with open(os.path.join(root, f)) as fp:
                        for line in fp.readlines():
                            yield json.loads(line)["text"]

    vocab = {}
    for txt in iter_pages():
        for token in preprocessor(txt):
            if token not in vocab:
                vocab[token] = 1
            else:
                vocab[token] += 1
    vocab = sorted(vocab.items(), key=lambda p: p[1], reverse=True)[:vocab_size]
    vocab = [p[0] for p in vocab]
    cnt = 0
    with open(bow_file, "w") as fp:
        for txt in iter_pages():
            page = set(preprocessor(txt))
            nonzero = [str(i) for i, w in enumerate(vocab) if w in page]
            if len(nonzero):
                fp.write(str(cnt))
                fp.write(" ")
                fp.write(str(vocab_size))
                fp.write(" ")
                fp.write(" ".join(nonzero))
                fp.write("\n")
            cnt += 1

    print("Converting {} to {}".format(bow_file, final_output))
    subprocess.run(
        [
            "danny_convert",
            "-i",
            bow_file,
            "-o",
            final_output,
            "-t",
            "bag-of-words",
            "-n",
            "40",
        ],
        check=True
    )

def _load_texmex_vectors(f, n, k):
    import struct

    v = numpy.zeros((n, k))
    for i in range(n):
        f.read(4)  # ignore vec length
        v[i] = struct.unpack('f' * k, f.read(k * 4))

    return v


# From ann-benchmarks
def _get_irisa_matrix(t, fn):
    import struct
    m = t.getmember(fn)
    f = t.extractfile(m)
    k, = struct.unpack('i', f.read(4))
    n = m.size // (4 + 4 * k)
    f.seek(0)
    return _load_texmex_vectors(f, n, k)


# Based on "On the Error of Random Fourier Features" by Sutherland and Schneider
# https://arxiv.org/abs/1506.02785
class Embedder(object):
    def __init__(self, dim_in, dim_out, seed):
        numpy.random.seed(seed)
        self.scale_factor = numpy.sqrt(2/dim_out)
        self.vecs = numpy.random.normal(size=(dim_out//2, dim_in))
    def embed(self, vec):
        prods = np.matmul(self.vecs, vec)
        cosines = np.cos(prods)
        sines = np.sin(prods)
        res = np.empty(cosines.size + sines.size, dtype=cosines.dtype)
        res[0::2] = cosines
        res[1::2] = sines
        return res * self.scale_factor


def inflate_cosine(X, factor=10, seed=1234):
    numpy.random.seed(seed)
    d = X.shape[1]
    Y = numpy.array(X)
    for _ in range(factor - 1):
        shift_matrix = 1/numpy.sqrt(d) * numpy.random.normal(size=(d, d))
        Y = numpy.vstack((Y, numpy.matmul(X, shift_matrix)))
    return Y

def rescale_sift_data(train, n_neighbors, sim_threshold):
    from sklearn.kernel_approximation import RBFSampler
    from sklearn.neighbors import NearestNeighbors
    from scipy.spatial.distance import pdist, squareform

    sample = np.random.permutation(train)[:10000]
    nn_builder = NearestNeighbors(n_neighbors=n_neighbors, metric='euclidean').fit(sample)
    print("built nn data structure")
    nn, _ = nn_builder.kneighbors(train)
    print("computed nearest neighbors")
    k_nn_dist_avg = np.mean(nn[:,-1])
    # scale_factor = (k_nn_dist_avg / np.sqrt(2*np.log(1/sim)))
    scale_factor = (k_nn_dist_avg / (2*np.log(1/sim_threshold)))
    return train / scale_factor

# From ann-benchmarks
def preprocess_sift(download_file, final_output):
    import tarfile
    tmp_dir = os.path.join(os.path.dirname(download_file))
    pre, ext = os.path.splitext(final_output)
    tokens = pre.split("-")
    target_neighbors = int(tokens[-2])
    target_inner = float(tokens[-1])
    print("Target neigbors is", target_neighbors)
    print("Target inner is", target_inner)

    with tarfile.open(download_file, 'r:gz') as t:
        train = _get_irisa_matrix(t, 'sift/sift_base.fvecs')
        test = _get_irisa_matrix(t, 'sift/sift_query.fvecs')
    print("Rescaling data")
    train = rescale_sift_data(train, target_neighbors, target_inner)

    embedder = Embedder(128,128,2198579145)
    tmp_file = os.path.join(tmp_dir, "sift-temp.txt")
    print("Embed the vectors")
    with open(tmp_file, "w") as fp:
      i = 0
      for vec in train:
            proj = embedder.embed(vec)
            fp.write(str(i))
            fp.write(" ")
            fp.write(" ".join([str(x) for x in proj]))
            fp.write("\n")
            i += 1
    print("Convert the file to binary")
    subprocess.run(
        [
            "danny_convert",
            "-i",
            tmp_file,
            "-o",
            final_output,
            "-t",
            "vector-normalized",
            "-n",
            "40",
        ],
        cwd=tmp_dir,
        check=True
    )


def preprocess_wiki_builder(vocab_size):
    return lambda download_file, final_output: _preprocess_wiki(
        vocab_size, download_file, final_output
    )


AOL_DATA = os.path.abspath("scripts/aol-data.sh")
CREATE_RAW = os.path.abspath("scripts/createraw.py")
SHUF_LENGTH = os.path.abspath("scripts/shuflength.py")
ORKUT_PY = os.path.abspath("scripts/orkut.py")


def preprocess_aol(download_file, final_output):
    """As provided by Mann et al."""
    directory = os.path.dirname(download_file)
    print("Unpacking")
    subprocess.run(["tar", "xzvf", download_file], cwd=directory, check=True)
    print("Extracting raw data")
    subprocess.run(
        "{} {}/AOL-user-ct-collection/user-ct-test-collection-*.txt.gz > aol-data.txt".format(
            AOL_DATA, directory
        ),
        shell=True,
        check=True,
        cwd=directory,
    )
    print("Creating data")
    subprocess.run(
        "{} --bywhitespace --dedup aol-data.txt aol-data-white-dedup-raw.txt ".format(
            CREATE_RAW
        ),
        shell=True,
        check=True,
        cwd=directory,
    )
    print("Shuffling")
    subprocess.run(
        "{} aol-data-white-dedup-raw.txt > tmp.txt".format(SHUF_LENGTH),
        shell=True,
        check=True,
        cwd=directory,
    )
    print("Converting to binary")
    subprocess.run(
        [
            "danny_convert",
            "-i",
            "tmp.txt",
            "-o",
            final_output,
            "-t",
            "bag-of-words",
            "-n",
            "40",
        ],
        cwd=directory,
        check=True
    )


def preprocess_orkut(download_file, final_output):
    directory = os.path.dirname(download_file)
    subprocess.run(
        "cat {} | gunzip > tt.txt".format(download_file), cwd=directory, shell=True, check=True
    )
    subprocess.run(
        "{} users tt.txt > orkut.out".format(ORKUT_PY), cwd=directory, shell=True, check=True
    )
    subprocess.run(
        "{} --bywhitespace orkut.out orkut-userswithgroups-raw.txt".format(CREATE_RAW),
        cwd=directory,
        shell=True,
        check=True
    )
    subprocess.run(
        "uniq orkut-userswithgroups-raw.txt > orkut.tmp", cwd=directory, shell=True, check=True
    )
    subprocess.run(
        [
            "danny_convert",
            "-i",
            "orkut.tmp",
            "-o",
            final_output,
            "-t",
            "bag-of-words",
            "-n",
            "40",
        ],
        check=True,
        cwd=directory,
    )


def preprocess_livejournal(download_file, final_output):
    directory = os.path.dirname(download_file)
    subprocess.run(
        "cat {} | gunzip > lj.txt".format(download_file), cwd=directory, shell=True
    )
    subprocess.run(
        "{} users lj.txt > livejournal.out".format(ORKUT_PY), cwd=directory, shell=True
    )
    subprocess.run(
        "{} --bywhitespace livejournal.out livejournal-userswithgroups-raw.txt".format(
            CREATE_RAW
        ),
        cwd=directory,
        shell=True,
    )
    subprocess.run(
        "uniq livejournal-userswithgroups-raw.txt > livejournal.tmp",
        cwd=directory,
        shell=True,
    )
    subprocess.run(
        [
            "danny_convert",
            "-i",
            "livejournal.tmp",
            "-o",
            final_output,
            "-t",
            "bag-of-words",
            "-n",
            "40",
        ],
        cwd=directory,
    )


def sample_dataset(base_path, filepath):
    pre, ext = os.path.splitext(filepath)
    tokens = pre.split("-")
    size = tokens[-1]
    subprocess.run(
        [
            "sampledata",
            "--size",
            str(size),
            base_path,
            filepath,
        ]
    )


def inflate(base_path, filepath):
    pre, ext = os.path.splitext(filepath)
    tokens = pre.split("-")
    inflation = tokens[-1]
    subprocess.run(
        [
            "inflate",
            "--factor",
            str(inflation),
            base_path,
            filepath,
        ]
    )


DATASETS = {
    "SIFT-100nn-0.5": Dataset(
        "SIFT-100nn-0.5",
        "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz",
        "sift-100-0.5.bin",
        preprocess_sift
    ),
    "Glove-27-200": Dataset(
        # This dataset is the Glove-2m dataset in ANN-benchmarks
        "Glove-27-200",
        "https://nlp.stanford.edu/data/glove.twitter.27B.zip",
        "glove.twitter.27B.200d.bin",
        preprocess_glove_6b,
    ),
    "Orkut": Dataset(
        "Orkut",
        "http://socialnetworks.mpi-sws.mpg.de/data/orkut-groupmemberships.txt.gz",
        "Orkut.bin",
        preprocess_orkut,
    ),
    "Livejournal": Dataset(
        "Livejournal",
        "http://socialnetworks.mpi-sws.mpg.de/data/livejournal-groupmemberships.txt.gz",
        "Livejournal.bin",
        preprocess_livejournal,
    ),
    "RandomAngular" : Dataset(
        "RandAngular",
        "",
        "random.bin",
        preprocess_random,
    )
}

derived_datasets = []

# Sampled datasets
for size in [200000, 400000, 800000]:
    derived_datasets.append(DerivedDataset(
        'SIFT-100nn-0.5-sample-{}'.format(size),
        'sift-100nn-0.5-sample-{}.bin'.format(size),
        DATASETS['SIFT-100nn-0.5'],
        sample_dataset
    ))
    derived_datasets.append(DerivedDataset(
        'Glove-sample-{}'.format(size),
        'Glove-sample-{}.bin'.format(size),
        DATASETS['Glove-27-200'],
        sample_dataset
    ))
    derived_datasets.append(DerivedDataset(
        'Orkut-sample-{}'.format(size),
        'Orkut-sample-{}.bin'.format(size),
        DATASETS['Orkut'],
        sample_dataset
    ))
    derived_datasets.append(DerivedDataset(
        'Livejournal-sample-{}'.format(size), 
        'Livejournal-sample-{}.bin'.format(size),
        DATASETS['Livejournal'],
        sample_dataset
    ))


# inflated datasets
for factor in [2,5,10]:
    derived_datasets.append(DerivedDataset(
        'Livejournal-inflated-{}'.format(factor),
        'Livejournal-inflated-{}.bin'.format(factor),
        DATASETS['Livejournal'],
        inflate
    ))
    derived_datasets.append(DerivedDataset(
        'Orkut-inflated-{}'.format(factor),
        'Orkut-inflated-{}.bin'.format(factor),
        DATASETS['Orkut'],
        inflate
    ))
    derived_datasets.append(DerivedDataset(
        'Glove-inflated-{}'.format(factor),
        'Glove-inflated-{}.bin'.format(factor),
        DATASETS['Glove-27-200'],
        inflate
    ))
    derived_datasets.append(DerivedDataset(
        'SIFT-100nn-0.5-inflated-{}'.format(factor),
        'sift-100nn-0.5-inflated-{}.bin'.format(factor),
        DATASETS['SIFT-100nn-0.5'],
        inflate
    ))


for d in derived_datasets:
    DATASETS[d.name] = d


def should_run(exp_tags, only_tags):
    if only_tags is None:
        return True
    for t in only_tags:
        if t in exp_tags:
            return True
    return False


def is_clean():
    proc = subprocess.run(["git", "status", "--porcelain"], stdout=subprocess.PIPE)
    return len(proc.stdout) == 0


def create_experiment_directory():
    date = datetime.datetime.today().strftime("%Y%m%d%H%M")
    git_revision = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], stdout=subprocess.PIPE
    )
    dirname = "{}-{}".format(date, git_revision.stdout.strip().decode("utf-8"))
    dirname = os.path.join("experiment-runs", dirname)
    if os.path.isdir(dirname):
        raise Exception("directory {} already exists".format(dirname))
    os.makedirs(dirname)
    for minion in MINIONS:
        subprocess.run(["ssh", minion, "mkdir", "-p", os.path.abspath(dirname)])
    return dirname


def run_experiment(executable, global_environment, experiment):
    environment = experiment.get("environment", {})  # name: list of values
    parameters = experiment.get("parameters", {})  #  parameter: list of values
    arguments = experiment.get("arguments", [])  # list of lists
    for name, values in environment.items():
        if not isinstance(values, list):
            environment[name] = [values]
    for name, values in parameters.items():
        if not isinstance(values, list):
            parameters[name] = [values]
    env_names, env_values = zip(*list(environment.items()))
    params_names, params_values = zip(*list(parameters.items()))

    for env_comb in product(*env_values):
        env_comb = [str(e) for e in env_comb]
        run_env = global_environment.copy()
        run_env.update(zip(env_names, env_comb))
        for k in run_env.keys():
            run_env[k] = str(run_env[k])
        for param_comb in product(*params_values):
            run_params = dict(zip(params_names, param_comb))
            for arg_list in arguments:
                command_line = [executable]
                for p, v in run_params.items():
                    command_line.extend([p, v])
                command_line.extend(arg_list)
                command_line = [str(t) for t in command_line]
                pprint(command_line)
                pprint(run_env)
                # run the command
                process = subprocess.run(command_line, env=run_env)
                process.check_returncode()


def run_file(path, only_tags=None):
    with open(path) as fp:
        template = jinja2.Template(fp.read())
    config = template.render(dataset=lambda name: DATASETS[name].get_path())
    config = yaml.load(config)
    if not is_clean():
        print("Working directory is not in a clean state, cannot run experiment")
        sys.exit(1)
    directory = create_experiment_directory()
    os.chdir(directory)
    environment = config["environment"]
    executable = config.get("executable", RUN_SCRIPT)
    for experiment in config["experiments"]:
        tags = experiment.get("tags", [])
        if should_run(tags, only_tags):
            print("=== Running {} {}".format(experiment["name"], tags))
            run_experiment(executable, environment, experiment)
    if "post" in config:
        cmd = shlex.split(config["post"])
        print("running post processing command", cmd)
        subprocess.run(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Danny experiments")
    parser.add_argument(
        "--list-datasets", action="store_true", help="list the available datasets"
    )
    parser.add_argument(
        "--dataset-prefix", help="install only datasets that have this prefix"
    )
    parser.add_argument("--sync", action="store_true", help="sync the datasets")
    parser.add_argument("--tags", help="run only experiments with these tags")
    parser.add_argument(
        "exp_file", nargs="?", help="the file storing the experiment specification"
    )

    args = vars(parser.parse_args())

    print(args)
    if args["list_datasets"]:
        print("Available datasets are:")
        for k in DATASETS.keys():
            if not "dataset_prefix" in args or k.lower().startswith(args["dataset_prefix"].lower()):
                print("  - {} ({})".format(k, DATASETS[k].get_path()))
        sys.exit(0)

    if args["sync"]:
        sync()
        sys.exit(0)

    if args["tags"] is not None:
        tags = args["tags"].split(",")
        print("Running only experiments tagged with ", tags)
    else:
        tags = None

    run_file(args["exp_file"], tags)

