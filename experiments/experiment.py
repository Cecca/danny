#!/usr/bin/env python

import os
from glob import glob
import numpy as np
import json
import dateutil
import pandas as pd
import gzip
import bz2
import matplotlib.pyplot as plt
from functools import wraps

def load_table(globpath, tablename):
    rows = []
    for path in glob(globpath):
        if path.endswith(".mpk") or path.endswith(".mpk.bz2"):
            return load_mpk(path, tablename)
        if path.endswith(".gz"):
            fh = gzip.open(path, "rb")
        elif path.endswith(".bz2"):
            fh = bz2.open(path, "rb")
        else:
            fh = open(path, "r")

        raw = fh.read()
        if isinstance(raw, str):
            multi_str = raw
        else:
            multi_str = raw.decode("utf-8")
        for line in multi_str.splitlines():
            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                raise e
            # Skip lines without the required table
            if tablename in data["tables"]:
                try:
                    date = dateutil.parser.parse(data['date'])
                    tags = data["tags"]
                    for data_row in data["tables"][tablename]:
                        row = dict()
                        row["date"] = date
                        row.update(data_row)
                        row.update(tags)
                        rows.append(row)
                except:
                    print("*ERROR* while handling a line of", path)

    df = pd.DataFrame(rows)
    if len(df) == 0:
        raise Exception("Empty dataframe!")
    df.set_index("date", inplace=True)
    return df


def cached_table(name, input_glob):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_file = ".{}.cache.mpk".format(name)
            if os.path.isfile(cache_file):
                modified_p = False
                mtime = os.path.getmtime(cache_file)
                for f in glob(input_glob):
                    if os.path.getmtime(f) > mtime:
                        modified_p = True
                        break
            else:
                modified_p = True
            if modified_p:
                print("Inputs are newer than the cache file", cache_file)
                table = func(*args, **kwargs)
                if not isinstance(table, (pd.DataFrame, pd.Series)):
                    raise TypeError(
                        "Function should return a DataFrame of Series")
                table.to_msgpack(cache_file)
                return table
            else:
                print("Found cached table in file", cache_file)
                table = pd.read_msgpack(cache_file)
                return table
        return wrapper
    return decorator


def components_plot(x=None, y=None, components=None,
                    total='total', component_name=None,
                    data=None,
                    palette=None,
                    ax=None):
    data = data.copy()
    if ax is None:
        ax = plt.gca()

    if palette is None:
        palette = sns.color_palette('cubehelix', len(components))

    # Draw bars for totals
    sns.barplot(data=data[data[component_name] == total],
                x=x, y=y, ax=ax, ci=None, color='gray')

    # data = data.set_index(component_name)
    # print(data)
    # #for i in range(len(components - 1)):

    num_patches_per_layer = len(ax.patches)

    base = [0.0] * num_patches_per_layer

    for component, color in zip(components, palette[1:]):
        #print("======", component)
        pdat = data[data[component_name] == component]
        sns.barplot(data=pdat,
                    x=x, y=y, ax=ax, ci=None, color=color)
        rects = ax.patches[-num_patches_per_layer:]
        for i, rect in enumerate(rects):
            print(i, rect.get_height(), component)
            rect.set_y(rect.get_y() + base[i])
            rect.set_width(0.9 * rect.get_width())
            base[i] = rect.get_y() + rect.get_height()

    return ax


def _confusion_matrix_numpy(ground, predicted, num_negatives=None):
    tp = np.intersect1d(ground, predicted, assume_unique=True).size
    fp = np.setdiff1d(predicted, ground, assume_unique=True).size
    fn = np.setdiff1d(ground, predicted, assume_unique=True).size
    tn = np.NaN

    if num_negatives is not None:
        tn = num_negatives - fp

    cm = {
        'tp': tp,
        'tpr': tp / ground.size,
        'fp': fp,
        'fn': fn,
        'tn': tn,
        'positives': len(ground),
        'precision': tp / (tp + fp) if tp + fp > 0 else np.NaN,
        'recall': tp / ground.size,
        'fnr': fn / len(ground)
    }
    if num_negatives is not None:
        cm['fpr'] = fp / num_negatives
    return cm


def confusion_matrix(ground, predicted):
    tp = 0  # len(ground.intersection(predicted))
    fp = 0  # len(predicted.difference(ground))
    fn = 0  # len(ground.difference(predicted))
    tn = 0  # We cannot count all the true negatives, without the entire labelled ground set

    for e in predicted:
        if e in ground:
            tp += 1
        else:
            fp += 1
    for e in ground:
        if e not in predicted:
            fn += 1
        else:
            tn += 1

    return {
        'tp': tp,
        'fp': fp,
        'fn': fn,
        'precision': tp / (tp + fp) if tp + fp > 0 else np.NaN,
        'recall': tp / len(ground)
    }
