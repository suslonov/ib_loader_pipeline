#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
Automated historical data loader
Parameters:
1 JSON description file
        "Client_id":
        "HOST":
        "PORT":
        "Frequency":
        "Destination": {"Type":
                "Bucket":
                "Path":
                "Folder":
        "Field":
        "Symbols": []

"""

import sys
import os
import json
from datetime import datetime
import pytz
import s3fs

from broker_matrix import IBLayer, stocks_contract

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
START_SESSION_HOUR = 9; START_SESSION_MIN = 30; END_SESSION_HOUR = 16; END_SESSION_MIN = 0
START_SESSION = "09:30"
END_SESSION = "15:59"

def current_data_slice():
    if len(sys.argv) < 2:
        sys.argv.append(os.path.expanduser('~/fr_config/test_current'))

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

    now_time = datetime.now(EASTERN)
    session_start = now_time.replace(hour=START_SESSION_HOUR, minute=START_SESSION_MIN, second=0)
    points = (now_time - session_start).seconds + 60

    if parameters['Frequency'] == 'minute':
        bar_size_setting = '1 min'
        duration_str_suffix = " S"
        localize = True
    else:
        print('unsupported current data frequency')
        return 1

    what_to_show = parameters['Field']
    client_id = parameters['Client_id']

    requests = {}
    with IBLayer(client_id=client_id, host=host, port=port) as ib:
        for symbol in parameters['Symbols']:
            requests[symbol] = (points, stocks_contract(symbol))
        collected_data = ib.retrieve_ib_historical_data_general(requests, duration_str_suffix, bar_size_setting, what_to_show, localize=localize)

    print("collected_data", collected_data.keys(), flush=True)

    for symbol in collected_data:
        if not collected_data[symbol] is None:
            collected_data[symbol].index = collected_data[symbol].index.tz_localize(None)
            to_write = collected_data[symbol]
            if len(to_write) == 0:
                continue

            to_write = to_write.between_time(START_SESSION, END_SESSION)
            date_for_file_name = now_time.strftime('%Y-%m-%d %H:%M:%S')

            if parameters['Destination']['Type'] == 'S3':
                print(parameters['Destination']['Bucket'] + '/' + folder + '/' + symbol + '_' + date_for_file_name + '.csv', len(to_write))
                with s3.open(parameters['Destination']['Bucket'] + '/' + folder + '/' + symbol + '_' + date_for_file_name + '.csv', 'w') as f:
                    to_write.to_csv(f)
            else:
                print(parameters['Destination']['Path'] + '/' + folder + '/' + symbol + '_' + date_for_file_name + '.csv', len(to_write))
                to_write.to_csv(parameters['Destination']['Path'] + '/' + folder + '/' + symbol + '_' + date_for_file_name + '.csv')
    return 0

if __name__ == '__main__':
    print("start current_data_slice", datetime.now())
    current_data_slice()
    print("finish current_data_slice", datetime.now())
