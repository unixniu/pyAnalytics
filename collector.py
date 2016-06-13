__author__ = 'william'

import tushare as ts
import pandas as pd
import numpy as np
import os
import random
import os.path

HIST_DATA_PATH = r'e:\analytics\stock\hist-0610'
CURRENT_TERM = 2016, 1


def get_hist_data(start=None, end=None, exclude_cyb=True, sample=0, persist=False):
    basics = ts.get_stock_basics()
    codes = basics.index if not exclude_cyb else [x for x in basics.index if not x.startswith('300')]
    hist_data = {}
    codes_selected = codes if sample == 0 else random.sample(codes, sample)
    if not os.path.isdir(HIST_DATA_PATH):
        os.makedirs(HIST_DATA_PATH)
    for code in codes_selected:
        try:
            df = ts.get_hist_data(code, start, end)
            hist_data[code] = df
            if persist:
                df.to_csv(os.path.join(HIST_DATA_PATH, '%s.csv' % code))
            print('retrieved hist data for %s' % code)
        except Exception as ex:
            print('error occurred in retrieving %s:%s' % code, ex)
    return pd.Panel(hist_data)

def find_lis(s):
    # files = sc.wholeTextFiles(DATA_FILE_PATH, use_unicode=False)
    lis = (1, 0)    # tuple for LIS found so far in form of (period_length, starting_position)
    start = 0   # starting position of the increasing subsequence evaluated currently
    while start < len(s) - lis[0]:
        last = start # last element added to the current subsequence
        for i in range(start+1, len(s)):
            if s.ix[i] >= s.ix[last]:
                last = i
            else:
                l = last - start + 1
                if l > lis[0]:
                    lis = (l, start)
                start = i
                break
        if last == len(s) - 1 and (last - start + 1) > lis[0]:
            lis = (last - start + 1, start)
            break
    return lis


def get_financial_data(terms=1, exclude_cyb=True, sample=0, persist=False):
    pass


if __name__ == "__main__":
    # get_hist_data(start='2016-01-01', exclude_cyb=True, persist=True, sample=20)
    s = pd.Series(np.random.randint(1, 100, 80))
    print(list(s))
    lis = find_lis(s)
    print(lis)


