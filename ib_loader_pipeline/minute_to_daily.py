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
RISK_FREE = 0 # for very short expirations it is negligible

def minute_to_daily():
    if len(sys.argv) < 2:
        sys.argv.append(os.path.expanduser('~/fr_config/minute_to_daily'))

    with open(sys.argv[1], 'r') as f:
        parameters = json.load(f)

    source_folder = parameters['Source']['Options']
    metadata_folder = parameters['Source']['Metadata']
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
        metadata_full_list = history_data_utils.get_dir_levels(2, [parameters['Source']['Bucket'] + '/' + metadata_folder], parameters['Source'], s3)
    else:
        metadata_full_list = history_data_utils.get_dir_levels(2, [parameters['Source']['Path'] + '/' + metadata_folder], parameters['Source'], s3)

    max_date = 0
    for metadata_file in metadata_full_list:
        file_date = datetime.strptime(metadata_file.split('/')[-1].split("_")[1].split(".")[0], "%Y-%m-%d")
        if max_date == 0 or max_date <= file_date:
            max_date = file_date
    max_date -= timedelta(days=15)

    if parameters['Source']['Type'] == 'S3':
        options_full_list = history_data_utils.get_dir_levels(1, [parameters['Source']['Bucket'] + '/' + source_folder], parameters['Source'], s3)
    else:
        options_full_list = history_data_utils.get_dir_levels(1, [parameters['Source']['Path'] + '/' + source_folder], parameters['Source'], s3)

    for option_file in options_full_list:
        option_file_name = option_file.split('/')[-1].split(".")[0]
        option_file_date = option_file_name[-15:-9]
        if not option_file_date:
            continue
        symbol = option_file_name[:-15]
        
        # direction = option_file_name[-9:-8]
        # strike = int(option_file_name[-8:])/1000
        if not symbol in parameters['Symbols']:
            continue
        if datetime.strptime(option_file_date, "%y%m%d") < max_date:
            continue
        if parameters['Source']['Type'] == 'S3':
            with s3.open(option_file, 'r') as f:
                df_option = pd.read_csv(f, index_col=0, parse_dates=True)
        else:
            df_option = pd.read_csv(option_file, index_col=0, parse_dates=True)

        if history_data_utils.is_file_exist(parameters['Destination'], destination_folder, option_file_name, s3):
            if parameters['Destination']['Type'] == 'S3':
                with s3.open(parameters['Destination']['Bucket'] + '/' + destination_folder + '/' + option_file_name + '.csv', 'r') as f:
                    df_destination = pd.read_csv(f, index_col=0, parse_dates=True)
            else:
                df_destination = pd.read_csv(parameters['Destination']['Path'] + '/' + destination_folder + '/' + option_file_name + '.csv', index_col=0, parse_dates=True)
            max_destination_date = df_destination.index[-1]
        else:
            df_destination = None
            max_destination_date = 0

        df_option['Date'] = pd.to_datetime(df_option.index.date)
        df_option.index.name = 'date'
        if max_destination_date != 0:
            df_option = df_option.loc[df_option['Date'] > max_destination_date]
        if len(df_option) == 0:
            continue

        df2 = df_option.groupby(['Date']).agg({'open': ['first'],
                                               'high': ['max'],
                                               'low': ['min'],
                                               'close': ['last'],
                                               'volume': ['sum'],
                                               'ex_dividend': ['last'],
                                               'split_ratio': ['last']})

        df2.columns = df2.columns.levels[0]
        if not df_destination is None:
            df3 = pd.concat([df_destination, df2])
        else:
            df3 = df2
        df3.sort_index(inplace=True)

        if parameters['Destination']['Type'] == 'S3':
            with s3.open(parameters['Destination']['Bucket'] + '/' + destination_folder + '/' + option_file_name + '.csv', 'w') as f:
                df3.to_csv(f)
        else:
            df3.to_csv(parameters['Destination']['Path'] + '/' + destination_folder + '/' + option_file_name + '.csv')

    return 0

if __name__ == '__main__':
    print("start minute_to_daily", datetime.now())
    minute_to_daily()
    print("finish minute_to_dailyL", datetime.now())
