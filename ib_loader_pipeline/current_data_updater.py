#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
Automated historical data loader
Parameters:
1 JSON description file
        "Frequency":
        "Source": {"Type":
                "Bucket":
                "Path":
                "Folder":
        "Destination": {"Type":
                "Bucket":
                "Path":
                "Folder":
        "Levels":
        "TermHours":
        "Symbols": []

"""

import sys
import os
import json
from datetime import datetime, timedelta
import pytz
import s3fs

import history_data_utils

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC

def current_data_updater():
    if len(sys.argv) < 2:
        sys.argv.append(os.path.expanduser('~/fr_config/download_stocks_current'))

    with open(sys.argv[1], 'r') as f:
        parameters = json.load(f)

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
    if 'TermHours' in parameters:
        term = parameters['TermHours']
    else:
        term = None

    if parameters['Source']['Type'] == 'S3':
        source_full_list = history_data_utils.get_dir_levels(parameters['Levels'], [parameters['Source']['Bucket'] + '/' + source_folder], parameters['Source'], s3)
    else:
        source_full_list = history_data_utils.get_dir_levels(parameters['Levels'], [parameters['Source']['Path'] + '/' + source_folder], parameters['Source'], s3)

    now_time = datetime.now(EASTERN).replace(tzinfo=None)
    symbol_dates = {}
    for symbol in parameters['Symbols']:
        symbol_dates[symbol] = None

    for source_symbol in source_full_list:
        symbol = source_symbol.split('/')[-1].split('_')[0]
        if parameters['Levels'] > 1:
            asset_symbol = source_symbol.split('/')[-parameters['Levels']]
        else:
            asset_symbol = symbol
        if asset_symbol in symbol_dates:
            file_date_time = datetime.strptime(source_symbol.split('/')[-1].split('_')[1].split('.')[0], "%Y-%m-%d %H:%M:%S")
            if symbol_dates[asset_symbol] is None or symbol_dates[asset_symbol][0] < file_date_time:
                symbol_dates[asset_symbol] = (file_date_time, source_symbol)

    if not term is None:
        for symbol in symbol_dates:
            if not symbol_dates[symbol] is None and symbol_dates[symbol][0] < now_time - timedelta(hours=term):
                symbol_dates[symbol] = None

    for symbol in symbol_dates:
        if symbol_dates[symbol] is None:
            continue
        file_date_time = datetime.strftime(symbol_dates[symbol][0], "%Y-%m-%d %H:%M:%S")
        if parameters['Source']['Type'] == 'S3':
            with s3.open(symbol_dates[symbol][1], 'r') as f:
                df_slice = f.read()
        else:
            with open(symbol_dates[symbol][1], 'r') as f:
                df_slice = f.read()

        if parameters['Destination']['Type'] == 'S3':
            with s3.open(parameters['Destination']['Bucket'] + '/' + destination_folder + '/' + symbol + '_' + file_date_time + '.csv', 'w') as f:
                f.write(df_slice)
        else:
            with open(parameters['Destination']['Path'] + '/' + destination_folder + '/' + symbol + '_' + file_date_time + '.csv', 'w') as f:
                f.write(df_slice)
        print(symbol, "current data slice updated", file_date_time)

    return 0

if __name__ == '__main__':
    print("start current_data_updater", datetime.now())
    current_data_updater()
    print("finish current_data_updater", datetime.now())
