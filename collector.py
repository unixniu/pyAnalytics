__author__ = 'william'

import tushare as ts
import pandas as pd
import numpy as np
import os
import random
import os.path

HIST_DATA_PATH = r'e:\analytics\stock\hist-W-2016'
REPORT_DATA_PATH = r'e:\analytics\stock\report'
CURRENT_TERM = 2016, 1


def get_hist_data(start=None, end=None, type='D', exclude_cyb=True, sample=0, persist=False):
    basics = ts.get_stock_basics()
    codes = basics.index if not exclude_cyb else [x for x in basics.index if not x.startswith('300')]
    hist_data = {}
    codes_selected = codes if sample == 0 else random.sample(codes, sample)
    if not os.path.isdir(HIST_DATA_PATH):
        os.makedirs(HIST_DATA_PATH)
    for code in codes_selected:
        try:
            df = ts.get_hist_data(code, start, end, ktype=type)
            hist_data[code] = df
            if persist:
                df.to_csv(os.path.join(HIST_DATA_PATH, '%s.csv' % code))
            print('retrieved hist data for %s' % code)
        except Exception as ex:
            try:
                print('error occurred in retrieving {}: {}'.format(code, ex))
            except Exception as innerex:
                print('exception: {}'.format(innerex))
    return pd.Panel(hist_data)


def get_report_data(year, term):
    try:
        if not os.path.isdir(REPORT_DATA_PATH):
            os.makedirs(REPORT_DATA_PATH)
        df = ts.get_report_data(year, term)
        path = os.path.join(REPORT_DATA_PATH, '{}-{}.csv'.format(year, term))
        df.to_csv(path)
    except Exception as ex:
        print("error occurred in retrieving report data: ", ex)


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
    # get_hist_data(start='2016-01-01', exclude_cyb=True, type='W', persist=True)
    get_report_data(2016, 1)
    get_report_data(2015, 4)
    get_report_data(2015, 3)
    get_report_data(2015, 2)

