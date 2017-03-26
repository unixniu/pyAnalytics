__author__ = 'xniu'

import collector
import takeoff_analysis
import sys
import datetime

if len(sys.argv) < 3:
    sys.exit(0)

start = sys.argv[1]
freq = sys.argv[2]
threshold = int(sys.argv[3])

hist_data_path = collector.collect_hist_data(start=start, exclude_cyb=True, type=freq, persist=True)
print('hist data stored into {}'.format(hist_data_path))

collector.collect_report_data(2016, 3)
collector.collect_report_data(2016, 2)
collector.collect_report_data(2016, 1)
collector.collect_report_data(2015, 4)

collector.collect_basic_data()

df = takeoff_analysis.analyze_trend(hist_data_path, kind=freq, increase_threshold=threshold, max_recent_slowdown=2)
print('done analyzing trend analysis, found %d records' % len(df))

today = datetime.datetime.now().strftime('%Y-%m-%d')
TREND_ANALYSIS_OUTPUT_PATH = 'E:/analytics/stock/analysis_weekly_{}_{}.csv'.format(start, today)
df.to_csv(TREND_ANALYSIS_OUTPUT_PATH)

basic_df = takeoff_analysis.get_basic_data(collector.BASIC_DATA_PATH)
report_df = takeoff_analysis.get_report_data(collector.REPORT_DATA_PATH)

CONSOLIDATED_DATA_PATH = r'E:/analytics/stock/consolidated_{}_{}_{}.csv'.format(freq, start, today)
consolidated = df.join(basic_df).join(report_df)
consolidated.to_csv(CONSOLIDATED_DATA_PATH)
print('done, consolidated data ouput to ', CONSOLIDATED_DATA_PATH)

