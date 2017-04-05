__author__ = 'william'

import tushare as ts
import pandas as pd
import numpy as np
import os
import random
import os.path
import datetime

# CURRENT_TERM = 2016, 3
BASE_FOLDER = r'E:\analytics\stock'

def collect_hist_data(start=None, end=None, type='D', exclude_cyb=True, sample=0, persist=False):
    folder_name = 'hist-{}-{:%y%m%d}-{:%y%m%d}'.format(
        type,
        datetime.datetime.strptime(start, '%Y-%m-%d'),
        datetime.datetime.today())
    storage_path = os.path.join(BASE_FOLDER, folder_name)

    # if target storage path already exists, consider the data has been collected already
    if not os.path.isdir(storage_path):
        os.makedirs(storage_path)
    if not os.listdir(storage_path):
        basics = ts.get_stock_basics()
        codes = basics.index if not exclude_cyb else [x for x in basics.index if not x.startswith('300')]
        hist_data = {}
        codes_selected = codes if sample == 0 else random.sample(codes, sample)
        
        for code in codes_selected:
            try:
                df = ts.get_hist_data(code, start, end, ktype=type)
                hist_data[code] = df
                if persist:
                    df.to_csv(os.path.join(storage_path, '%s.csv' % code))
                print('retrieved hist data for %s' % code)
            except Exception as ex:
                try:
                    print('error occurred in retrieving {}: {}'.format(code, ex))
                except Exception as innerex:
                    print('exception: {}'.format(innerex))
    # return pd.Panel(hist_data)
    return storage_path


def collect_report_data(year, term):
    try:
        report_data_path = os.path.join(BASE_FOLDER, 'report')
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


def collect_basic_data():
    basic_data_path = os.path.join(BASE_FOLDER, 'basics')
    if not os.path.exists(basic_data_path):
        os.makedirs(basic_data_path)
    path = os.path.join(basic_data_path, 'basic.csv')
    if not os.path.exists(path):
        ts.get_stock_basics().to_csv(path)
    return path


def get_financial_data(terms=1, exclude_cyb=True, sample=0, persist=False):
    pass


if __name__ == "__main__":
    collect_hist_data(start='2016-12-1', exclude_cyb=True, type='w', persist=True)

    # get_report_data(2016, 3)
    # get_report_data(2016, 2)
    # get_report_data(2016, 1)
    # get_report_data(2015, 4)

    # get_basic_data()

