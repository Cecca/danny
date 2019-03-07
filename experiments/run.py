#!/usr/bin/env python

import shlex
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
    from urllib.request import urlretrieve # Python 3


try:
    DATA_DIRECTORY = os.environ['DANNY_DATA_DIR']
    print('Datasets are stored in', DATA_DIRECTORY)
except:
    print('The environment variable DANNY_DATA_DIR is not set.')
    sys.exit(1)

assert os.path.isabs(DATA_DIRECTORY), "Data directory should be an absolute path"

try:
    MINIONS = os.environ['DANNY_MINIONS'].split(',')
    print('Minions are', MINIONS)
except:
    print('The environment variable DANNY_MINIONS is not set.')
    sys.exit(2)


RUN_SCRIPT = os.environ.get('DANNY_RUN_SCRIPT', '../run-danny.sh')


def download(src, dst):
    if not os.path.exists(dst):
        print('downloading {} to {}'.format(src, dst))
        urlretrieve(src, dst)


def sync():
    for host in MINIONS:
        subprocess.run([
          'rsync',
          '--progress',
          '--ignore-existing',
          '-avzr',
          '{}/*.bin'.format(DATA_DIRECTORY),
          '{}:{}/'.format(host, DATA_DIRECTORY)
        ])


class Dataset(object):
    def __init__(self, name, url, local_filename, preprocess_fn):
        self.name = name
        self.url = url
        download_dir = os.path.join(DATA_DIRECTORY, 'download')
        self.download_file = os.path.join(download_dir, os.path.basename(self.url))
        if not os.path.isdir(download_dir):
            os.mkdir(download_dir)
        assert not os.path.isabs(local_filename)
        self.local_filename = os.path.join(DATA_DIRECTORY, local_filename)
        self.preprocess = preprocess_fn

    def prepare(self):
        print('Preparing dataset', self.name)
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


def preprocess_glove_6b(download_file, final_output):
    """Preprocess all datasets present in the glove6b archive"""
    tmp_dir = os.path.join(os.path.dirname(download_file), 'glove_unzipped')
    print('Extracting zip file')
    zip_ref = zipfile.ZipFile(download_file, 'r')
    zip_ref.extractall(tmp_dir)
    zip_ref.close()
    for txt in glob.glob(os.path.join(tmp_dir, '*.txt')):
        output = os.path.join(DATA_DIRECTORY, os.path.splitext(os.path.basename(txt))[0] + '.bin')
        print('Converting {} to {}'.format(txt, output))
        subprocess.run(['danny_convert',
                        '-i', txt,
                        '-o', output,
                        '-t', 'unit-norm-vector',
                        '-n', '40'])


def _preprocess_wiki(vocab_size, download_file, final_output):
    tmp_dir = os.path.join(os.path.dirname(download_file), 'wiki_preprocess')
    # run the script to convert to json the elements
    if not os.path.isdir(tmp_dir):
        print('Extracting pages')
        proc = subprocess.run(['./WikiExtractor.py', '--json', download_file, '-o', tmp_dir])
        proc.check_returncode()

    print('Building bag of words')
    bow_file = os.path.join(tmp_dir, 'bag-of-words-{}.txt'.format(vocab_size))
    vectorizer = CountVectorizer(stop_words='english',
                                 binary=False,
                                 max_features=vocab_size)
    preprocessor = vectorizer.build_analyzer()
    def iter_pages():
        for root, dirs, files in os.walk(tmp_dir):
            for f in files:
                if f.startswith('wiki_'):
                    with open(os.path.join(root, f)) as fp:
                        for line in fp.readlines():
                            yield json.loads(line)['text']

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
    with open(bow_file, 'w') as fp:
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

    print('Converting {} to {}'.format(bow_file, final_output))
    subprocess.run(['danny_convert',
                    '-i', bow_file,
                    '-o', final_output,
                    '-t', 'bag-of-words',
                    '-n', '40'])


def preprocess_wiki_builder(vocab_size):
    return lambda download_file, final_output: _preprocess_wiki(vocab_size, download_file, final_output)


DATASETS = {
    'Glove-6B-100': Dataset(
        'Glove-6B-100', 
        'http://nlp.stanford.edu/data/glove.6B.zip', 
        'glove.6B.100d.bin', 
        preprocess_glove_6b),
    'wiki-10k': Dataset(
        'wiki-10k', 
        'https://dumps.wikimedia.org/enwiki/20190220/enwiki-20190220-pages-articles-multistream.xml.bz2', 
        'wiki-10k.bin', 
        preprocess_wiki_builder(10000))
}


def should_run(exp_tags, only_tags):
    if only_tags is None:
        return True
    for t in only_tags:
        if t in exp_tags:
            return True
    return False


def run_experiment(executable, global_environment, experiment):
    environment = experiment.get('environment', {}) # name: list of values
    parameters = experiment.get('parameters', {}) # parameter: list of values
    arguments = experiment.get('arguments', []) # list of lists
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
    environment = config['environment']
    executable = config.get('executable', RUN_SCRIPT)
    for experiment in config['experiments']:
        tags = experiment.get('tags', [])
        if should_run(tags, only_tags):
            print('=== Running {} {}'.format(experiment['name'], tags))
            run_experiment(executable, environment, experiment)
    if 'post' in config:
        cmd = shlex.split(config['post'])
        print('running post processing command', cmd)
        subprocess.run(cmd)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Danny experiments')
    parser.add_argument('--list-datasets', 
                        action='store_true',
                        help='list the available datasets')
    parser.add_argument('--tags',
                        help='run only experiments with these tags')
    parser.add_argument('exp_file', nargs='?',
                        help='the file storing the experiment specification')

    args = vars(parser.parse_args())

    if args['list_datasets']:
        print('Available datasets are:')
        for k in DATASETS.keys():
            print('  - {}'.format(k))
        sys.exit(0)

    if args['tags'] is not None:
        tags = args['tags'].split(',')
        print("Running only experiments tagged with ", tags)
    else:
        tags = None

    run_file(args['exp_file'], tags)

