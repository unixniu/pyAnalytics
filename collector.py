__author__ = 'william'

import sys
import tushare as ts
import pandas as pd
import numpy as np
import os
import random
import os.path
import datetime
import akshare as ak

from tushare.stock.fundamental import get_report_data

BASE_FOLDER = 'data'
root_data_path = ''
tushare_api = ts.pro_api('3cf89e9c77146b4d041f7aea8f8e93c1ef887581a586bcb14f97f510')

def tu_hist_data(folder, codes, start=None, end=None, freq='D', sample=0, persist=True):
    os.makedirs(folder, exist_ok=True)
    hist_data = {}
    codes_selected = codes if sample == 0 else random.sample(codes, sample)
    
    for code in codes_selected:
        path = os.path.join(folder, '%s.csv' % code)
        if not (persist and os.path.isfile(path)):
            try:
                df = ts.pro_bar(
                    ts_code=code,
                    asset='E',
                    adj='qfq', 
                    start_date=start.strftime('%Y%m%d'), 
                    end_date=end.strftime('%Y%m%d'), 
                    freq=freq)
                hist_data[code] = df
                if persist:
                    df.to_csv(path)
                print('retrieved hist data for %s' % code)
            except Exception as ex:
                try:
                    print('error occurred in retrieving {}: {}'.format(code, ex))
                except Exception as innerex:
                    print('exception: {}'.format(innerex))
    # return pd.Panel(hist_data)
    return True

def ak_hist_data(folder, codes, start=None, end=None, freq='D', sample=0, persist=True):
    os.makedirs(folder, exist_ok=True)
    hist_data = {}
    codes_selected = codes if sample == 0 else random.sample(codes, sample)
    
    for code in codes_selected:
        path = os.path.join(folder, '%s.csv' % code)
        if not (persist and os.path.isfile(path)):
            try:
                code = code[:code.find('.')]
                df = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust='qfq')
                
                df['date'] = pd.to_datetime(df['日期'], format='%Y-%m-%d')
                df.set_index('date', inplace=True)

                if freq == 'W' or freq == 'M':
                    df = df.resample(freq).agg({'开盘':'first', '收盘':'last', '最高':'max', '最低':'min','成交量':'sum'})

                # df['ma3'] = df['收盘'].rolling(window=3).mean()
                df['ma5'] = df['收盘'].rolling(window=5).mean()

                df['p_change'] = df['close'].pct_change()
                df.rename(columns={'收盘':'close', '成交量':'volume'}, inplace=True)
                hist_data[code] = df
                if persist:
                    df.to_csv(path)
                print('retrieved hist data for %s' % code)
            except Exception as ex:
                try:
                    print('error occurred in retrieving {}: {}'.format(code, ex))
                except Exception as innerex:
                    print('exception: {}'.format(innerex))
    return hist_data

def collect_report_data(year, term):
    try:
        report_data_path = os.path.join(root_data_path, 'report')
        if not os.path.isdir(report_data_path):
            os.makedirs(report_data_path)
        path = os.path.join(report_data_path, '{}-{}.csv'.format(year, term))
        if not os.path.exists(path):
            df = ts.get_report_data(year, term)
            df.to_csv(path)
        return report_data_path
    except Exception as ex:
        print("error occurred in retrieving report data: ", ex)
        return None


def tu_basic_data(folder):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, 'basic.csv')
    df = ts.pro_api().stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    df.to_csv(path)
    return df


def get_financial_data(terms=1, exclude_cyb=True, sample=0, persist=False):
    pass


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise ValueError('wrong arguments. e.g. \"python collector.py 20210101 D\"')
    start_date = datetime.datetime.strptime(sys.argv[1].strip(), '%Y%m%d')
    freq = sys.argv[2]
    end_date = datetime.datetime.today()

    ts.set_token('3cf89e9c77146b4d041f7aea8f8e93c1ef887581a586bcb14f97f510')
    root_data_path = os.path.join(os.getcwd(), BASE_FOLDER)
    basics = tu_basic_data(os.path.join(root_data_path, 'basics'))
    hist_folder_name = 'hist-{}-{:%y%m%d}-{:%y%m%d}'.format(freq, start_date, end_date)
    ak_hist_data(os.path.join(root_data_path, hist_folder_name), basics['ts_code'], start_date, end_date, freq)
    #tu_hist_data(os.path.join(root_data_path, hist_folder_name), basics['ts_code'], start_date, end_date, freq)
    #collect_hist_data(start='2021-1-1', exclude_cyb=True, type='d', persist=True)

