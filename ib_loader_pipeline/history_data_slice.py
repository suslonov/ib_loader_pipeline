#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
Automated historical data loader
Parameters:
1 JSON description file
        "Client_id":
        "Frequency":
        "Destination": {"Type":
                "Bucket":
                "Path":
                "Folder":
        "DepthDays":
        "Symbols": []
        "Field":

"""

import sys
import os
import json
from datetime import datetime, timedelta
import pytz
import s3fs

from broker_matrix import IBLayer, stocks_contract
import history_data_utils

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
START_SESSION = "09:30"
END_SESSION = "15:59"

def calculate_date(days):
    return datetime.now(EASTERN).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)

def history_data_slice():
    if len(sys.argv) < 2:
        # sys.argv.append(os.path.expanduser('~/fr_config/ib_daily_minute'))
        sys.argv.append(os.path.expanduser('~/fr_config/test_data_slices'))

    with open(sys.argv[1], 'r') as f:
        parameters = json.load(f)

    host = parameters["HOST"]
    port = parameters["PORT"]
    folder = parameters['Destination']['Folder']
    if parameters['Destination']['Type'] == 'S3':
        key = os.environ['AWS_ACCESS_KEY']
        secret = os.environ['AWS_SECRET_ACCESS_KEY']
        s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret)
    else:
        parameters['Destination']['Path'] = os.path.expanduser(parameters['Destination']['Path'])
        s3 = None

    parameters_depth_days = parameters['DepthDays']
    if parameters['Frequency'] == 'daily':
        parameters_depth_date = calculate_date(parameters_depth_days)
        bar_size_setting = '1 day'
        duration_str_suffix = " D"
        localize = False
    elif parameters['Frequency'] == 'minute':
        parameters_depth_date = calculate_date(parameters_depth_days + 1)
        bar_size_setting = '1 min'
        duration_str_suffix = " D"
        localize = True
    else:
        print('unsupported historical data frequency')
        return 1

    max_points = 0
    symbol_dates = {}
    now_date = datetime.now(EASTERN).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
    for symbol in parameters['Symbols']:
        slice_files = history_data_utils.get_file_list(parameters['Destination'], folder, symbol, s3)
        max_file_date = None
        for slice_file in slice_files:
            file_date = datetime.strptime(slice_file.split('/')[-1].split("_")[1].split(".")[0], "%Y-%m-%d")
            if not max_file_date or max_file_date < file_date:
                max_file_date = file_date

        if max_file_date:
            if max_file_date < now_date:
                file_points = (now_date - max_file_date).days - 1
                symbol_dates[symbol] = (file_points, max_file_date + timedelta(days=1))
                if file_points > max_points:
                    max_points = file_points
            elif max_file_date == now_date:
                file_points = 1
                symbol_dates[symbol] = (file_points, max_file_date)
                if file_points > max_points:
                    max_points = file_points
        else:
            symbol_dates[symbol] = (parameters_depth_days, parameters_depth_date)
            max_points = max(max_points, parameters_depth_days)
        max_points = min(max_points, parameters_depth_days)

    if max_points == 0:
        return 0

    what_to_show = parameters['Field']
    client_id = parameters['Client_id']

    print(symbol_dates, flush=True)

    symbol_requests = {}
    for symbol in symbol_dates:
        symbol_requests[symbol] = (symbol_dates[symbol][0] + 1, stocks_contract(symbol))
    with IBLayer(client_id=client_id, host=host, port=port) as ib:
        collected_data = ib.retrieve_ib_historical_data_general(symbol_requests, duration_str_suffix, bar_size_setting, what_to_show, localize=localize)

    print("collected_data", collected_data.keys(), flush=True)

    for symbol in collected_data:
        if not collected_data[symbol] is None:
            collected_data[symbol].index = collected_data[symbol].index.tz_localize(None)
            to_write = collected_data[symbol].loc[(collected_data[symbol].index >= symbol_dates[symbol][1]) &
                                                  (collected_data[symbol].index < now_date)]
            if len(to_write) == 0:
                continue

            if parameters['Frequency'] == 'daily':
                to_write.index = to_write.index.date
                to_write.index.name = 'Date'
                date_for_file_name = str(to_write.index[-1])
            else:
                to_write = to_write.between_time(START_SESSION, END_SESSION)
                if len(to_write) == 0:
                    continue
                date_for_file_name = str(to_write.index[-1].date())

            if parameters['Destination']['Type'] == 'S3':
                print(parameters['Destination']['Bucket'] + '/' + folder + '/' + symbol + '/' + symbol + '_' + date_for_file_name + '.csv', len(to_write))
                with s3.open(parameters['Destination']['Bucket'] + '/' + folder + '/' + symbol + '/' + symbol + '_' + date_for_file_name + '.csv', 'w') as f:
                    to_write.to_csv(f)
            else:
                if not os.path.isdir(parameters['Destination']['Path'] + '/' + folder + '/' + symbol):
                    os.mkdir(parameters['Destination']['Path'] + '/' + folder + '/' + symbol)
                print(parameters['Destination']['Path'] + '/' + folder + '/' + symbol + '/' + symbol + '_' + date_for_file_name + '.csv', len(to_write))
                to_write.to_csv(parameters['Destination']['Path'] + '/' + folder + '/' + symbol + '/' + symbol + '_' + date_for_file_name + '.csv')
    return 0

if __name__ == '__main__':
    print("start history_data_slice", datetime.now())
    history_data_slice()
    print("finish history_data_slice", datetime.now())
