#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
Automated option metadata loader
Parameters:
1 JSON description file
        "Client_id":
        "Frequency":
        "Destination":
                "Type":
                "Bucket":
                "Path":
                "Folder":
                "Assets":
                "Metadata":
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
import pandas as pd

from broker_matrix import IBLayer, option_contract
import history_data_utils

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
STEPS_FROM_AT_THE_MONEY = 20

def option_metadata():
    if len(sys.argv) < 2:
        sys.argv.append(os.path.expanduser('~/fr_config/test_option_data'))
        # sys.argv.append(os.path.expanduser('~/fr_config/ib_daily_options'))

    with open(sys.argv[1], 'r') as f:
        parameters = json.load(f)

    # folder = parameters['Folder'] + '/'
    assets_folder = parameters['Destination']['Assets']
    metadata_folder = parameters['Destination']['Metadata']
    client_id = parameters['Client_id']
    host = parameters["HOST"]
    port = parameters["PORT"]
    if "StepsFromAtTheMoney" in parameters:
        steps_from_at_the_money = parameters["StepsFromAtTheMoney"]

    if parameters['Destination']['Type'] == 'S3':
        key = os.environ['AWS_ACCESS_KEY']
        secret = os.environ['AWS_SECRET_ACCESS_KEY']
        s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret)
    else:
        parameters['Destination']['Path'] = os.path.expanduser(parameters['Destination']['Path'])
        s3 = None

    asset_prices = {}
    date_now = datetime.now(EASTERN).replace(tzinfo=None)
    # date_now = JERUSALEM.localize(datetime.now()).astimezone(EASTERN).replace(tzinfo=None)

    max_max_file_date = None
    for symbol in parameters['Symbols']:
        slice_files = history_data_utils.get_file_list(parameters['Destination'], assets_folder, symbol, s3)
        max_file_date = None
        for slice_file in slice_files:
            file_date = datetime.strptime(slice_file.split('/')[-1].split("_")[1].split(".")[0], "%Y-%m-%d").date()
            if file_date <= date_now.date() and (not max_file_date or max_file_date < file_date):
                max_file_date = file_date

        if max_file_date:
            if parameters['Destination']['Type'] == 'S3':
                with s3.open(parameters['Destination']['Bucket'] + '/' + assets_folder + '/' + symbol + '/' + symbol + '_' + str(max_file_date) + '.csv', 'r') as f:
                    asset_prices[symbol] = pd.read_csv(f, index_col=0)['close'].iloc[-1]
            else:
                asset_prices[symbol] = pd.read_csv(parameters['Destination']['Path'] + '/' + assets_folder + '/' + symbol + '/' + symbol + '_' + str(max_file_date) + '.csv', index_col=0)['close'].iloc[-1]
            if not max_max_file_date or max_max_file_date < max_file_date:
                max_max_file_date = max_file_date
        else:
            asset_prices[symbol] = None

    options = {}
    with IBLayer(client_id=client_id, host=host, port=port) as ib:
        volatility = ib.retrieve_ib_historical_data(parameters['Symbols'], '1 M', '1 month', 'HISTORICAL_VOLATILITY')

        for symbol in parameters['Symbols']:
            if symbol not in asset_prices:
                continue
            option_symbol = ib.retrieve_option_parameters(symbol)
            if option_symbol is None:
                continue
            option_symbol['symbol'] = symbol
            if not symbol in volatility or volatility[symbol] is None:
                continue
            option_symbol['volatility'] = volatility[symbol]['close'][-1]
            option_symbol['close_before'] = asset_prices[symbol]
            options[symbol] = option_symbol

        for symbol in options:
            option_symbol = options[symbol]
            closest_expiration = option_symbol['closest_expiration']
            if closest_expiration is None:
                continue
            closest_expiration_date = option_symbol['closest_expiration_date']
    
            if (closest_expiration_date - date_now).days > 7:
                continue
    
            strikes_filtered = []
            today_price = option_symbol['close_before']   # TODO current price would be better
            len_strikes = 0
            for strike in option_symbol['all_strikes']:
                if strike < today_price:
                    strikes_filtered.append(strike)
                    if len(strikes_filtered) >= steps_from_at_the_money:
                        del strikes_filtered[0]
                else:
                    strikes_filtered.append(strike)
                    if len_strikes >= steps_from_at_the_money:
                        break
                    len_strikes += 1
    
            for strike in strikes_filtered:
                if not strike in option_symbol['strikes']:
                    opt_contract = option_contract(symbol=symbol,
                                                       strike=strike,
                                                       expiration_str=closest_expiration,
                                                       multiplier=option_symbol['multiplier'],
                                                       direction="C")
                    if not ib.contract_details_check(opt_contract):
                        continue
                    option_symbol['strikes'].append(strike)
            print(symbol, "expiration", option_symbol["closest_expiration_date"].date())

    date_for_file_name = str(max_max_file_date)
    for symbol in options:
        del options[symbol]["closest_expiration_date"]
        if parameters['Destination']['Type'] == 'S3':
            print(parameters['Destination']['Bucket'] + '/' + metadata_folder + '/' +  symbol + '/' + symbol + '_' + date_for_file_name + '.json')
            with s3.open(parameters['Destination']['Bucket'] + '/' + metadata_folder + '/' +  symbol + '/' + symbol + '_' + date_for_file_name + '.json', 'w') as f:
                json.dump(options[symbol], f)
        else:
            print(parameters['Destination']['Path'] + '/' + metadata_folder + '/' + symbol + '/' + symbol + '_' + date_for_file_name + '.json')
            if not os.path.isdir(parameters['Destination']['Path'] + '/' + metadata_folder + '/' + symbol):
                os.mkdir(parameters['Destination']['Path'] + '/' + metadata_folder + '/' + symbol)
            with open(parameters['Destination']['Path'] + '/' + metadata_folder + '/' + symbol + '/' + symbol + '_' + date_for_file_name + '.json', 'w') as f:
                json.dump(options[symbol], f)
    return 0

if __name__ == '__main__':
    print("start option_metadata", datetime.now())
    option_metadata()
    print("finish option_metadata", datetime.now())
