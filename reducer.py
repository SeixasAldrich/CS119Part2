#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
import pandas as pd

pres_list = []
value_list = []

for line in sys.stdin.readlines():
    key ,value = line.strip().split('\t')
    pres_list.append(key)
    value_list.append(int(value))

d = {'president': pres_list, 'valence': value_list}
df = pd.DataFrame.from_dict(d)
mean_valence = df.groupby(['president']).mean()
print(mean_valence)