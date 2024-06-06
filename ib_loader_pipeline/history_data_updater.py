#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
Automated historical data loader
Parameters:
1 JSON description file
        "Source": {"Type":
                "Bucket":
                "Path":
                "Folder":
        "Destination": {"Type":
                "Bucket":
                "Path":
                "Folder":
        "Symbols": []
        "TimeLikeQuantopian":
        "Levels":
"""

import sys
import os
import json
from datetime import datetime, timedelta
import pytz
import s3fs
import pandas as pd

import history_data_utils

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
MAX_BACK_DATE = 7

def history_data_updater():
    if len(sys.argv) < 2:
        # sys.argv.append(os.path.expanduser('~/fr_config/test_download_options_minute'))
        sys.argv.append(os.path.expanduser('~/fr_config/download_stocks_minute'))

    with open(sys.argv[1], 'r') as f:
        parameters = json.load(f)

    date_now_7 = datetime.now().date() - timedelta(days=MAX_BACK_DATE)
    source_folder = parameters['Source']['Folder']
    destination_folder = parameters['Destination']['Folder']
    if parameters['Source']['Type'] == 'S3' or parameters['Destination']['Type'] == 'S3':
        key = os.environ['AWS_ACCESS_KEY']
        secret = os.environ['AWS_SECRET_ACCESS_KEY']
        s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret)
    else:
        s3 = None
    if 'Path' in parameters['Source']:
        parameters['Source']['Path'] = os.path.expanduser(parameters['Source']['Path'])
    if 'Path' in parameters['Destination']:
        parameters['Destination']['Path'] = os.path.expanduser(parameters['Destination']['Path'])

    if parameters['Source']['Type'] == 'S3':
        source_full_list = history_data_utils.get_dir_levels(parameters['Levels'], [parameters['Source']['Bucket'] + '/' + source_folder], parameters['Source'], s3)
    else:
        source_full_list = history_data_utils.get_dir_levels(parameters['Levels'], [parameters['Source']['Path'] + '/' + source_folder], parameters['Source'], s3)

    if parameters['Frequency'] == 'daily':
        localize = False
    elif parameters['Frequency'] == 'minute':
        localize = True
    time_like_quantopian = pd.Timedelta(minutes=0)
    if "TimeLikeQuantopian" in parameters:
        if parameters["TimeLikeQuantopian"]:
            time_like_quantopian = pd.Timedelta(minutes=1)

    # t0 = 0; t1 = 0; t2 = 0; t3 = 0; t4 = 0; t5 = 0
    # _t0 = datetime.now()
    for source_symbol in source_full_list:
        # _t1 = datetime.now()
        symbol = source_symbol.split('/')[-1].split('_')[0]
        if parameters['Levels'] > 1:
            asset_symbol = source_symbol.split('/')[-parameters['Levels']]
        else:
            asset_symbol = symbol
        if not parameters['Symbols'] or asset_symbol in parameters['Symbols']:
            if history_data_utils.is_file_exist(parameters['Destination'], destination_folder, symbol, s3):
                if parameters['Destination']['Type'] == 'S3':
                    with s3.open(parameters['Destination']['Bucket'] + '/' + destination_folder + '/' + symbol + '.csv', 'r') as f:
                        df_destination = pd.read_csv(f, index_col=0, parse_dates=True)
                else:
                    df_destination = pd.read_csv(parameters['Destination']['Path'] + '/' + destination_folder + '/' + symbol + '.csv', index_col=0, parse_dates=True)
                max_date = df_destination.index[-1].date()
            else:
                df_destination = None
                max_date = 0

            slice_files = history_data_utils.get_dir(source_symbol, parameters['Source'], s3)
            updated = False
            # t1 += (datetime.now() - _t1).seconds + (datetime.now() - _t1).microseconds/1000000
            for slice_file in slice_files:
                # _t2 = datetime.now()
                file_date = datetime.strptime(slice_file.split('/')[-1].split("_")[1].split(".")[0], "%Y-%m-%d").date()
                if max_date == 0 or (max_date <= file_date and file_date >= date_now_7): #!!!
                    updated = True
                    if parameters['Source']['Type'] == 'S3':
                        with s3.open(slice_file, 'r') as f:
                            df_slice = pd.read_csv(f, index_col=0, parse_dates=True)
                    else:
                        df_slice = pd.read_csv(slice_file, index_col=0, parse_dates=True)
                    # t2 += (datetime.now() - _t2).seconds + (datetime.now() - _t2).microseconds/1000000
                    # _t3 = datetime.now()
                    df_slice['volume'] = df_slice['volume'] * 100
                    df_slice['ex_dividend'] = 0.0
                    df_slice['split_ratio'] = 1.0
                    if localize:
                        df_slice.index = df_slice.index.tz_localize(EASTERN).tz_convert(UTC).tz_localize(None) + time_like_quantopian

                    # t3 += (datetime.now() - _t3).seconds + (datetime.now() - _t3).microseconds/1000000
                    # _t4 = datetime.now()

                    if not df_destination is None:
                        df_destination.update(df_slice)
                        df_destination = pd.concat([df_destination, df_slice.loc[~df_slice.index.isin(df_destination.index)]])
                    else:
                        df_destination = df_slice
                    # t4 += (datetime.now() - _t4).seconds + (datetime.now() - _t4).microseconds/1000000
            if not updated:
                continue
            # _t5 = datetime.now()
            df_destination.sort_index(inplace=True)

            print(symbol, parameters['Frequency'], "updated from", max_date, "to", df_destination.index[-1])
            if parameters['Destination']['Type'] == 'S3':
                with s3.open(parameters['Destination']['Bucket'] + '/' + destination_folder + '/' + symbol + '.csv', 'w') as f:
                    df_destination.to_csv(f)
            else:
                df_destination.to_csv(parameters['Destination']['Path'] + '/' + destination_folder + '/' + symbol + '.csv')
            # t5 += (datetime.now() - _t5).seconds + (datetime.now() - _t5).microseconds/1000000
    # t0 += (datetime.now() - _t0).seconds + (datetime.now() - _t0).microseconds/1000000
    # print(t0, t1, t2, t3, t4, t5)

    return 0

if __name__ == '__main__':
    print("start history_data_updater", datetime.now())
    history_data_updater()
    print("finish history_data_updater", datetime.now())
