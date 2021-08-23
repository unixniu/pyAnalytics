__author__ = 'xniu'

import pandas as pd
import numpy as np

import re
import glob
import os
import os.path
import sys
from enum import Enum

DAILY_DATA_FOLDER = 'E:/analytics/stock/hist-D-2016-04-01-0718'
DAILY_SAMPLE_PATH = 'E:/analytics/stock/hist/002415.csv'

WEEKLY_DATA_FOLDER = 'E:/analytics/stock/hist-W-20160401-0709'
WEEKLY_SAMPLE_PATH = 'E:/analytics/stock/hist-W-2016-0627/002643.csv'

BASIC_DATA_PATH = r'E:\analytics\stock\basics\basic.csv'
REPORT_DATA_FOLDER = r'E:/analytics/stock/report'

DATE_FOR_PAUSE_CHECK = '2016-07-18'

pd.set_option('max_rows', 20)

''' detect increase trend by use of MA, applicable for both daily (kind: 'd') and weekly data (kind: 'w')
'''
def find_increase_trend(df, trend_threshold=3):

    if len(df) == 0:
        return None

    s = df['ma5']
    # the position from which (exclusive) MA starts to rise until latest
    ma_change_point = -1
    for i in range(0, len(s)-1):
        if s[i] < s[i+1]:
            ma_change_point = i
            break
    else:
        ma_change_point = len(s) - 1

    # the position from which (exclusive) closing price starts to rise until latest
    # 0 means it has been falling in recent period, may rise 'tomorrow'
    if not (df['p_change']<0).any():
        starting_rise_point = len(df)-1
    else:
        starting_rise_point = df.index.get_loc(df[df['p_change']<0].index[0])

    trend_start = starting_rise_point if starting_rise_point > ma_change_point else ma_change_point
    # a trend is established at least lasting for 3 occurrences
    if trend_start < trend_threshold:
        return None
    else:
        startdate = df.index[trend_start]
        # use close price of two ends to calc total increase percent
        total_increase = round(df.iat[0, 0] / df.iat[trend_start, 0] - 1, 4) * 100
        # the observation at change point doesn't count as increase, but only later ones
        trend_length = trend_start
        # mean week-over-week increase percent over this period
        mean_increase = round(df[:trend_start]['p_change'].mean(), 2)
        first_above_mean_position = df.index.get_loc(df[df['p_change'] >= mean_increase].index[0])
        # num of recent consecutive obserations whose WoW increase is lower than mean
        recent_below_mean_count = first_above_mean_position
        std = round(df[:trend_start]['p_change'].std(), 2)
        return startdate, total_increase, trend_length, mean_increase, std, recent_below_mean_count


NUDGE_THRESHOLD = 0.02

def prepare(df):
    df['pct_change'] = df['close'].pct_change()
    df['ma5_pct_change'] = df['ma5'].pct_change()


def analyze(df):
    df['var'] = df['ma5_pct_change'].map(variation, na_action='ignore')
    df['norm_var'] = df['var'].map(lambda x: 0 if abs(x) < 2 else x )
    df.dropna(inplace=True)

    grouped = df.groupby((df['norm_var'] != df['norm_var'].shift()).cumsum(), as_index=False)
    trend = grouped.agg(trend=('norm_var', 'first'), len=('norm_var', 'count'))
    trend['prev'], trend['next'] = trend['trend'].shift(), trend['trend'].shift(-1)
    trend['norm_trend'] = trend.apply(lambda x: x['prev'] if x['trend'] == 0 and x['len'] <= 2 and x['prev'] == x['next'] else x['trend'], axis=1)
    
    grouped = trend.groupby((trend['norm_trend'] != trend['norm_trend'].shift()).cumsum(), as_index=False)
    final_trend = grouped.agg(trend=('norm_trend', 'first'), len=('len', 'sum'))


def variation(pct_change):
    if pct_change > NUDGE_THRESHOLD:
        return 2
    elif pct_change > 0:
        return 1
    elif pct_change == 0:
        return 0
    elif pct_change > -1 * NUDGE_THRESHOLD:
        return -1
    else:
        return -2


def analyze_trend(data, folder, kind='w', trend_threshold=3, increase_threshold=10, max_recent_slowdown=1):
    # load stock data from files
    if data is None and folder:
        data = {}
        reg = re.compile(r'(\d{6}).csv')
        stockfiles = {t[1].group(1):t[0] for t in ((x, reg.search(x)) for x in glob.glob(folder + '/*.csv')) if t[1]}
        for code in stockfiles:
            try:
                df = pd.read_csv(stockfiles[code], index_col=0, parse_dates=True, \
                                usecols=['date', 'close', 'p_change', 'ma5'], \
                                error_bad_lines=False)
                if len(df) == 0:
                    continue
                data[code] = df
            except Exception as ex:
                print('error reading file %s: %s' % (stockfiles[code], ex))
    if not data:
        return None

    resultmap = {}
    latest = pd.Timestamp('20000101')
    for code in data:
        try:
            df = data[code]

            # delete first entry if it doesn't stand for weekly data (whose timestamp should be Fri)
            # usually daily data for date at retrieval is also collected
            if kind == 'w' and df.index[0].dayofweek != 4:
                df = df[1:]

            # latest date available in input stock data,
            # absence of it indicates the stock's trading is paused at that time
            if df.index[0] > latest:
                latest = df.index[0]
            elif df.index[0] < latest:
                continue

            res = find_increase_trend(df, trend_threshold)
            ''' take as valid entry when following conditions are met:
                    1. increasing trend lasts longer than 3 observations
                    2. actual increase percent over the period is above 10% (MA trails behind actual varation)
                    3. increasing trend didn't considerably slowdown lately
            '''
            if res and res[1] > increase_threshold and res[-1] <= max_recent_slowdown:
                resultmap[code] = res
        except Exception as ex:
            print('error occurred in processing %s: %s' % (code, ex))
    df = pd.DataFrame.from_dict(resultmap, orient='index')
    print(df.head())
    df.columns = ['startdate', 'increase', 'length', 'mean', 'std', 'RSL']
    return df

def get_basic_data(basic_data_path):
    basic_df = pd.read_csv(basic_data_path, index_col=False, dtype={'code':np.str}, \
                           usecols=['code', 'pe', 'pb', 'outstanding', 'totals', 'esp', 'timeToMarket'], \
                           error_bad_lines=False)
    basic_df.set_index('code', inplace=True)
    basic_df['timeToMarket'] = pd.to_datetime(basic_df['timeToMarket'], errors='coerce', format='%Y%m%d')
    basic_df = basic_df[basic_df['timeToMarket'].notnull()]
    return basic_df

def get_report_data(report_data_folder):
    reg = re.compile(r'(\d{4}-\d).csv')
    reports = {t[1].group(1):t[0] for t in ((x, reg.search(x)) for x in glob.glob(report_data_folder + '/*.csv')) if t[1]}
    report_dfs = []
    report_terms = []
    for term in reports:
        rdf = pd.read_csv(reports[term], index_col=False, dtype={'code':np.str}, \
                               usecols=['code', 'roe', 'profits_yoy'], \
                               error_bad_lines=False)
        rdf.set_index('code', inplace=True)
        d = rdf.index.duplicated()
    #     print('duplicates in %s: %d (%s)' % (term, len(d[d==True]), rdf.index[d==True][:5]))
        rdf.drop_duplicates(inplace=True)
        report_dfs.append(rdf)
        report_terms.append(term)
    # print(report_dfs)
    all_report_df = pd.concat(report_dfs, keys=report_terms, axis=1, join='outer')
    return all_report_df