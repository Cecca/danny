#!/usr/bin/env python

import pandas
import experiment
import json
import sys

path = sys.argv[1]

with open(path) as fp:
    table_names = set()
    tag_names = set()
    for line in fp.readlines():
        data = json.loads(line)
        table_names = table_names.union(set(data['tables'].keys()))
        tag_names = tag_names.union(set(data['tags'].keys()))

for name in table_names:
    table = experiment.load_table(path, name)
    print(table)

