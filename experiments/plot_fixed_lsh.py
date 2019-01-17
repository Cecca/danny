import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from plotnine import *
import sys
import experiment
import re
import os

path = sys.argv[1]
table = sys.argv[2]

data = experiment.load_table(path, table)
data['input'] = [os.path.basename(p) for p in data['left_path']]
print(data.columns)

g = (ggplot(data, aes(x='k', y='total_time_ms'))
     + geom_point()
     + geom_errorbar(stat='summary')
     + facet_wrap('input')
     + theme_bw()
     + theme())
g.draw()
g.save('total_time.png')


