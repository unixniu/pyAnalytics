__author__ = 'william'

import tushare as ts
import pandas as pd
import numpy as np
import os
import random
import os.path
import re

HIST_DATA_PATH = r'e:\analytics\stock\hist-W-2016-0627'
REPORT_DATA_PATH = r'e:\analytics\stock\report'

def find_recent_increase(filepath):
    try:
        df = pd.read_csv(filepath, index_col=0, parse_dates=True,
                                   usecols=['date', 'close', 'p_change', 'ma5', 'ma10', 'ma20'],
                                   error_bad_lines=False)
        # df.sort_index(ascending=False)
        s = df['ma5']
        s.sort_index(ascending=False)
        change_point = None
        for i in range(0, len(s)-1):
            if s[i] < s[i+1]:
                change_point = i
                break
        if change_point is None:
            change_point = len(s) - 1
        startdate = df.index[change_point]
        increase = round(s[0] / s[change_point] - 1, 4) * 100
        return startdate, increase, change_point+1, df[:change_point+1]['p_change'].mean(), \
              round(df[:change_point+1]['p_change'].std(), 2)
    except Exception as ex:
        print('error occurred in processing {}: {}'.format(filepath, ex))


def run():
    reg = re.compile('([0-9]{6}).csv')
    m = {reg.search(x).group(1) : HIST_DATA_PATH + '\\' + x for x in os.listdir(HIST_DATA_PATH) if reg.search(x)}
    result = {}
    for code in m:
        try:
            print('processing ', code, '...')
            res = find_recent_increase(m[code])
            if res[2] >= 5:
                result[code] = res
        except Exception as ex:
            print('error occurred in processing {}: {}'.format(code, ex))
    df = pd.DataFrame.from_dict(result, orient='index')
    df.columns = ['startdate', 'increase', 'length', 'mean', 'std']
    df.to_csv('e:/analytics/stock/analytics.csv')


if __name__ == '__main__':
    run()


