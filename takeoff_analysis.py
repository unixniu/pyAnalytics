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



def analyze(df, recent_n1=4, recent_n2=8, recent_n3=12):
    result = {}

    # change perctange from different dimensions
    close_col_pos = df.columns.get_loc('close')
    result['recent_' + str(recent_n1)] = last_n_increase(df, close_col_pos, recent_n1)
    result['recent_' + str(recent_n2)] = last_n_increase(df, close_col_pos, recent_n2)
    result['recent_' + str(recent_n3)] = last_n_increase(df, close_col_pos, recent_n3)
    # idxmin, idxmax = df['close'].idxmin(), df['close'].idxmax()
    first, high, low, last = df['close'][0], df['close'].max(), df['close'].min(), df['close'][len(df)-1]
    result['change_from_high'] = round((last / high - 1) * 100, 2)
    result['change_from_low'] = round((last / low - 1) * 100, 2)
    result['change_since_start'] = round((last / first - 1) * 100, 2)
    result['high_over_low'] = round((high / low - 1) * 100, 2)

    trend_df = trend(df)
    # take in last n trend periods (n is 3 by default)
    for i in range(1, 4):
        result['trend-{}'.format(i)] = trend_df['trend'].iat[i * -1]
        result['trend-{}-len'.format(i)] = trend_df['duration'].iat[i * -1]
        result['trend-{}-scale'.format(i)] = trend_df['scale'].iat[i * -1]
    return result

# get increase percent over last n slots based on close price of last slot and -(n+1) slot
def last_n_increase(df, close_col_pos, n):
    return np.nan if len(df) < n+1 else pct_change(df, close_col_pos, len(df)-n-1, len(df)-1)

# calculate change pencent of y-th element over x-th element based on specified column value
def pct_change(df, col, start, end):
    return round((df.iat[end, col] / df.iat[start, col] - 1) * 100, 2)

def trend(df):
    #  prepare necessary input
    df['pct_change'] = df['close'].pct_change()
    df['ma5_pct_change'] = df['ma5'].pct_change()

    # determine variation by MA instead of close price to smooth trend
    variation(df, 'ma5_pct_change', 'var')
    # normalize variation of shake (1 or -1) to value 0 
    df['norm_var'] = df['var'].map(lambda x: 0 if abs(x) < 2 else x )
    # df.dropna(inplace=True)

    grouped = df.groupby((df['norm_var'] != df['norm_var'].shift()).cumsum(), as_index=False)
    trend = grouped.agg(trend=('norm_var', 'first'), duration=('norm_var', 'count'))
    trend['prev'], trend['next'] = trend['trend'].shift(), trend['trend'].shift(-1)
    # in case of short shake (within 2 slots) surrended by same trend, normalize it to that trend
    trend['norm_trend'] = trend.apply(lambda x: x['prev'] if x['trend'] == 0 and x['duration'] <= 2 and x['prev'] == x['next'] else x['trend'], axis=1)
    
    grouped = trend.groupby((trend['norm_trend'] != trend['norm_trend'].shift()).cumsum(), as_index=False)
    final_trend = grouped.agg(trend=('norm_trend', 'first'), duration=('duration', 'sum'))
    final_trend['scale'] = np.nan
    accumulator = 0
    close_col_pos = df.columns.get_loc('close')
    # caculate scale of each trending period backward
    for i in range(len(final_trend)-1, 0, -1):
        # ending index of current trend period
        end = len(df) - accumulator - 1
        duration = final_trend['duration'][i]
        # starting index of current trend period
        start = len(df) - accumulator - duration - 1  
        accumulator += duration
        final_trend['scale'][i] = pct_change(df, close_col_pos, start, end)
    return final_trend

# calculate variation (rise, drop or shake) for each slot
def variation(df, pct_change_column, varation_column_name='var'):
    if len(df) <= 1:
        return

    colPos = 0
    try:
        colPos = df.columns.get_loc(pct_change_column)
    except:
        return

    df[varation_column_name] = np.nan

    for i in range(0, len(df)):
        current_pct_change = df.iat[i, colPos]
        if pd.isna(current_pct_change):
            continue
        else:
            var = map_variation(current_pct_change)
            if i > 0:
                prev = df.at[df.index[i-1], varation_column_name]
                # align shake to previous trend (2 or -2) if they are same direction
                if not pd.isna(prev):
                    if abs(prev) == 2 and var * prev > 0:
                        var = prev
                df.at[df.index[i], varation_column_name] = var

    # align shake to subseuqnet trend (2 or -2) if they are same direction
    for i in range(len(df)-2, -1, -1):
        var = df[varation_column_name][i]
        if (not pd.isna(var)) and abs(var) < 2:
            next = df[varation_column_name][i+1]
            if abs(next) == 2 and var * next > 0:
                df.at[df.index[i], varation_column_name] = next
        
NUDGE_THRESHOLD = 0.02

def map_variation(pct_change):
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