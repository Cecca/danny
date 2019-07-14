#!/usr/bin/env python

import shlex
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
        self.base_path = base.get_path()
        self.filename = os.path.join(DATA_DIRECTORY, filename)
        self.preprocess_fn = preprocess_fn

    def prepare(self):
        print("Preparing dataset", self.name)
        self.preprocess_fn(self.base_path, self.filename)

    def get_path(self):
        if not os.path.exists(self.filename):
            print("File {} missing, preparing it".format(self.filename))
            self.prepare()
        # sync()
        return self.filename


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
                "unit-norm-vector",
                "-n",
                "40",
            ]
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
        ]
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
    subprocess.run(["tar", "xzvf", download_file], cwd=directory)
    print("Extracting raw data")
    subprocess.run(
        "{} {}/AOL-user-ct-collection/user-ct-test-collection-*.txt.gz > aol-data.txt".format(
            AOL_DATA, directory
        ),
        shell=True,
        cwd=directory,
    )
    print("Creating data")
    subprocess.run(
        "{} --bywhitespace --dedup aol-data.txt aol-data-white-dedup-raw.txt ".format(
            CREATE_RAW
        ),
        shell=True,
        cwd=directory,
    )
    print("Shuffling")
    subprocess.run(
        "{} aol-data-white-dedup-raw.txt > tmp.txt".format(SHUF_LENGTH),
        shell=True,
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
    )


def preprocess_orkut(download_file, final_output):
    directory = os.path.dirname(download_file)
    subprocess.run(
        "cat {} | gunzip > tt.txt".format(download_file), cwd=directory, shell=True
    )
    subprocess.run(
        "{} users tt.txt > orkut.out".format(ORKUT_PY), cwd=directory, shell=True
    )
    subprocess.run(
        "{} --bywhitespace orkut.out orkut-userswithgroups-raw.txt".format(CREATE_RAW),
        cwd=directory,
        shell=True,
    )
    subprocess.run(
        "uniq orkut-userswithgroups-raw.txt > orkut.tmp", cwd=directory, shell=True
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


def preprocess_diverse(base_path, filepath):
    datatype = "unit-norm-vector" if "glove" in base_path else "bag-of-words"
    pre, ext = os.path.splitext(filepath)
    tokens = pre.split("-")
    similarity_range = tokens[-2]
    size = tokens[-1]
    pre, ext = os.path.splitext(base_path)
    lid_path = pre + ".lid"
    if not os.path.exists(lid_path):
        print("Missing lid file for dataset", base_path)
        print("Aborting!")
        sys.exit(1)
    subprocess.run(
        [
            "gendiverse",
            "-t",
            datatype,
            "-s",
            str(size),
            "-r",
            str(similarity_range),
            base_path,
            lid_path,
            filepath,
        ]
    )

def preprocess_diverse_expansion(base_path, filepath):
    datatype = "unit-norm-vector" if "glove" in base_path else "bag-of-words"
    pre, ext = os.path.splitext(filepath)
    tokens = pre.split("-")
    similarity_range = tokens[-2]
    size = tokens[-1]
    pre, ext = os.path.splitext(base_path)
    exp_path = pre + ".exp"
    if not os.path.exists(exp_path):
        print("Missing expansion file for dataset", base_path)
        print("Aborting!")
        sys.exit(1)
    subprocess.run(
        [
            "gendiverse",
            "-t",
            datatype,
            "-s",
            str(size),
            "-r",
            str(similarity_range),
            base_path,
            exp_path,
            filepath,
        ]
    )


DATASETS = {
    "Glove-6B-100": Dataset(
        "Glove-6B-100",
        "http://nlp.stanford.edu/data/glove.6B.zip",
        "glove.6B.100d.bin",
        preprocess_glove_6b,
    ),
    "Glove-27-200": Dataset(
        # This dataset is the Glove-2m dataset in ANN-benchmarks
        "Glove-27-200",
        "https://nlp.stanford.edu/data/glove.twitter.27B.zip",
        "glove.twitter.27B.200d.bin",
        preprocess_glove_6b,
    ),
    "wiki-10k": Dataset(
        "wiki-10k",
        "https://dumps.wikimedia.org/enwiki/20190220/enwiki-20190220-pages-articles-multistream.xml.bz2",
        "wiki-10k.bin",
        preprocess_wiki_builder(10000),
    ),
    "AOL": Dataset(
        "AOL",
        "http://www.cim.mcgill.ca/~dudek/206/Logs/AOL-user-ct-collection/aol-data.tar.gz",
        "AOL.bin",
        preprocess_aol,
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
    "GNews": Dataset(
        "GNews",
        "",
        "GoogleNews-vectors-negative300.bin",
        lambda a, b: print("Preprocessing unimplemented")
    )
}

# Derived datasets

derived_datasets = []
for r in [0.5,0.7,0.9]:
    d = DerivedDataset(
        'Livejournal-diverse-{}-3M'.format(r),
        'Livejournal-diverse-{}-3000000.bin'.format(r),
        DATASETS['Livejournal'],
        preprocess_diverse
    )
    derived_datasets.append(d)

    d = DerivedDataset(
        'Orkut-diverse-{}-3M'.format(r),
        'Orkut-diverse-{}-3000000.bin'.format(r),
        DATASETS['Orkut'],
        preprocess_diverse
    )
    derived_datasets.append(d)

    d = DerivedDataset(
        'AOL-diverse-{}-3M'.format(r),
        'AOL-diverse-{}-3000000.bin'.format(r),
        DATASETS['AOL'],
        preprocess_diverse
    )
    derived_datasets.append(d)

    d = DerivedDataset(
        'Glove-27-diverse-{}-3M'.format(r),
        'Glove-27-diverse-{}-3000000.bin'.format(r),
        DATASETS['Glove-27-200'],
        preprocess_diverse
    )
    derived_datasets.append(d)

derived_datasets.append(DerivedDataset(
    'Livejournal-diverse-exp-{}-3M'.format(0.5),
    'Livejournal-diverse-exp-{}-3000000.bin'.format(0.5),
    DATASETS['Livejournal'],
    preprocess_diverse_expansion
))
derived_datasets.append(DerivedDataset(
    'Orkut-diverse-exp-{}-3M'.format(0.5),
    'Orkut-diverse-exp-{}-3000000.bin'.format(0.5),
    DATASETS['Orkut'],
    preprocess_diverse_expansion
))
derived_datasets.append(DerivedDataset(
    'AOL-diverse-exp-{}-3M'.format(0.5),
    'AOL-diverse-exp-{}-3000000.bin'.format(0.5),
    DATASETS['AOL'],
    preprocess_diverse_expansion
))
derived_datasets.append(DerivedDataset(
    'wiki-10k-diverse-exp-{}-3M'.format(0.5),
    'wiki-10k-diverse-exp-{}-3000000.bin'.format(0.5),
    DATASETS['wiki-10k'],
    preprocess_diverse_expansion
))
derived_datasets.append(DerivedDataset(
    'Glove-27-diverse-exp-{}-3M'.format(r),
    'Glove-27-diverse=exp-{}-3000000.bin'.format(r),
    DATASETS['Glove-27-200'],
    preprocess_diverse
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
    parser.add_argument("--sync", action="store_true", help="sync the datasets")
    parser.add_argument("--tags", help="run only experiments with these tags")
    parser.add_argument(
        "exp_file", nargs="?", help="the file storing the experiment specification"
    )

    args = vars(parser.parse_args())

    if args["list_datasets"]:
        print("Available datasets are:")
        for k in DATASETS.keys():
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

