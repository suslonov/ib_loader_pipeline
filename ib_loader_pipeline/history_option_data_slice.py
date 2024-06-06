#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme dict-iter-missing-items.
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
        "Assets":
        "Metadata":
        "DepthDays":
        "Symbols": []
        "Field":
        "Direction": []

"""

import sys
import os
import json
from datetime import datetime
import pytz
import s3fs

from broker_matrix import IBLayer, option_contract
import history_data_utils

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
START_SESSION = "09:30"
END_SESSION = "15:59"

def history_option_data_slice():
    if len(sys.argv) < 2:
        sys.argv.append(os.path.expanduser('~/fr_config/ib_daily_options'))
        # sys.argv.append(os.path.expanduser('~/fr_config/test_daily_options'))

    with open(sys.argv[1], 'r') as f:
        parameters = json.load(f)

    host = parameters["HOST"]
    port = parameters["PORT"]
    folder = parameters['Destination']['Folder']
    metadata_folder = parameters['Destination']['Metadata']
    if parameters['Destination']['Type'] == 'S3':
        key = os.environ['AWS_ACCESS_KEY']
        secret = os.environ['AWS_SECRET_ACCESS_KEY']
        s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret)
    else:
        parameters['Destination']['Path'] = os.path.expanduser(parameters['Destination']['Path'])
        s3 = None
    points = parameters['DepthDays']

    option_symbols = {}
    for symbol in parameters['Symbols']:
        slice_files = history_data_utils.get_file_list(parameters['Destination'], metadata_folder, symbol, s3)
        max_file_date = None
        for slice_file in slice_files:
            file_date = datetime.strptime(slice_file.split('/')[-1].split("_")[1].split(".")[0], "%Y-%m-%d").date()
            if not max_file_date or max_file_date < file_date:
                max_file_date = file_date

        if max_file_date:
            if parameters['Destination']['Type'] == 'S3':
                with s3.open(parameters['Destination']['Bucket'] + '/' + metadata_folder + '/' + symbol + '/' + symbol + '_' + str(max_file_date) + '.json', 'r') as f:
                    option_symbols[symbol] = json.load(f)
            else:
                with open(parameters['Destination']['Path'] + '/' + metadata_folder + '/' + symbol + '/' + symbol + '_' + str(max_file_date) + '.json', 'r') as f:
                    option_symbols[symbol] = json.load(f)
        else:
            option_symbols[symbol] = None

    requests = {}
    max_points = 0
    now_date = datetime.now(EASTERN).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
    for symbol in parameters['Symbols']:
        if option_symbols[symbol] and option_symbols[symbol]['strikes']:
            for strike in option_symbols[symbol]['strikes']:
                for option_direction in parameters['Direction']:
                    option_symbol_string = history_data_utils.make_option_symbol_string(symbol, str(strike), option_direction, option_symbols[symbol]['closest_expiration'][2:8])
                    slice_files = history_data_utils.get_file_list(parameters['Destination'], folder + '/' + symbol, option_symbol_string, s3)
                    max_file_date = None
                    for slice_file in slice_files:
                        file_date = datetime.strptime(slice_file.split('/')[-1].split("_")[1].split(".")[0], "%Y-%m-%d")
                        if not max_file_date or max_file_date < file_date:
                            max_file_date = file_date

                    if max_file_date:
                        if max_file_date < now_date:
                            file_date_points = (now_date - max_file_date).days - 1
                            if file_date_points > max_points:
                                max_points = file_date_points
                        elif max_file_date == now_date:
                            file_date_points = 1
                            if file_date_points > max_points:
                                max_points = file_date_points
                    else:
                        file_date_points = points
                        max_points = max([max_points, points])

                    max_points = min([max_points, points])
                    opt_contract = option_contract(symbol=symbol,
                                                       strike=strike,
                                                       expiration_str=option_symbols[symbol]['closest_expiration'],
                                                       multiplier=option_symbols[symbol]['multiplier'],
                                                       direction="C")
                    requests[(symbol, strike)] = (option_symbol_string, file_date_points + 1, opt_contract)


    if max_points == 0:
        return 0

    if parameters['Frequency'] == 'daily':
        bar_size_setting = '1 day'
        duration_str_suffix = " D"
        localize = False
    elif parameters['Frequency'] == 'minute':
        bar_size_setting = '1 min'
        duration_str_suffix = " D"
        localize = True
    what_to_show = parameters['Field']
    client_id = parameters['Client_id']

    print("requests=", len(requests))
    with IBLayer(client_id=client_id, host=host, port=port) as ib:
        collected_data = ib.retrieve_ib_historical_data_general(requests, duration_str_suffix, bar_size_setting, what_to_show, localize=localize, options=True)

    for (symbol, strike) in collected_data:
        if not collected_data[(symbol, strike)] is None:
            to_write = collected_data[(symbol, strike)]
            if len(to_write) == 0:
                continue
            to_write.index = to_write.index.tz_localize(None)

            if parameters['Frequency'] == 'daily':
                to_write.index = to_write.index.date
                to_write.index.name = 'Date'
                date_for_file_name = str(to_write.index[-1])
            else:
                to_write = to_write.between_time(START_SESSION, END_SESSION)
                if len(to_write) == 0:
                    continue
                date_for_file_name = str(to_write.index[-1].date())

            option_symbol_string = requests[(symbol, strike)][0]

            if parameters['Destination']['Type'] == 'S3':
                print(parameters['Destination']['Bucket'] + '/' + folder + '/' + symbol + '/' + option_symbol_string + '/' + option_symbol_string + '_' + date_for_file_name + '.csv', len(to_write))
                with s3.open(parameters['Destination']['Bucket'] + '/' + folder + '/' + symbol + '/' + option_symbol_string + '/' + option_symbol_string + '_' + date_for_file_name + '.csv', 'w') as f:
                    to_write.to_csv(f)
            else:
                print(parameters['Destination']['Path'] + '/' + folder + '/' + symbol + '/' + option_symbol_string + '/' + option_symbol_string + '_' + date_for_file_name + '.csv', len(to_write))
                if not os.path.isdir(parameters['Destination']['Path'] + '/' + folder + '/' + symbol):
                    os.mkdir(parameters['Destination']['Path'] + '/' + folder + '/' + symbol)
                if not os.path.isdir(parameters['Destination']['Path'] + '/' + folder + '/' + symbol + '/' + option_symbol_string):
                    os.mkdir(parameters['Destination']['Path'] + '/' + folder + '/' + symbol + '/' + option_symbol_string)
                to_write.to_csv(parameters['Destination']['Path'] + '/' + folder + '/' + symbol + '/' + option_symbol_string + '/' + option_symbol_string + '_' + date_for_file_name + '.csv')
    return 0

if __name__ == '__main__':
    print("start history_option_data_slice", datetime.now())
    history_option_data_slice()
    print("finish history_option_data_slice", datetime.now())
