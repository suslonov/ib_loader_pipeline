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
                "Options":
                "Assets":
                "AssetsDaily":
                "Metadata":
        "Destination": {"Type":
                "Bucket":
                "Path":
                "Folder":
        "Levels":
        "TrendDays":
        "Symbols": []
"""

import sys
import os
import json
from datetime import datetime
import pytz
import s3fs
import pandas as pd
import numpy as np
from scipy import stats

import history_data_utils

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
RISK_FREE = 0 # for very short expirations it is negligible
VOLATILITY_SHAPES = "~/git/option_trader/option_trader/pre_trained/volatility_shapes.csv"

def data_for_XL(end_date=None):
    if len(sys.argv) < 2:
        sys.argv.append(os.path.expanduser('~/fr_config/download_data_for_XL'))

    with open(sys.argv[1], 'r') as f:
        parameters = json.load(f)

    if end_date is None:
        if len(sys.argv) == 3:
            end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")
        else:
            end_date = datetime.now()
    end_date = end_date.replace(hour=23, minute=59, second=59)

    df_volatility_shapes = pd.read_csv(os.path.expanduser(VOLATILITY_SHAPES), index_col=0)
    source_folder = parameters['Source']['Options']
    assets_folder = parameters['Source']['Assets']
    assets_daily_folder = parameters['Source']["AssetsDaily"]
    metadata_folder = parameters['Source']['Metadata']
    destination_folder = parameters['Destination']['Folder']
    trend_days = parameters['TrendDays']
    trend_for_s = parameters["TREND_FOR_S"]

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
        if (max_date == 0 or max_date <= file_date) and file_date <= end_date:
            max_date = file_date
    date_for_file_name = str(max_date.date())

    df_metadata = pd.DataFrame(columns=["symbol", "volatility", "coeff_b", "coeff_a", "mean_before", "close_before", "closest_expiration"])
    df_assets = None
    if parameters['Source']['Type'] == 'S3':
        for symbol in parameters['Symbols']:
            metadata_file = parameters['Source']['Bucket'] + '/' + metadata_folder + '/' + symbol + '/' + symbol + '_' + date_for_file_name + '.json'
            if s3.exists(metadata_file):
                with s3.open(metadata_file, 'r') as f:
                    symbol_metadata = json.load(f)
                df_metadata.loc[len(df_metadata)] = [symbol, symbol_metadata['volatility'], np.nan, np.nan, np.nan, symbol_metadata['close_before'], symbol_metadata['closest_expiration']]

            asset_file = parameters['Source']['Path'] + '/' + assets_folder + '/' + symbol + '.csv'
            if s3.exists(asset_file):
                with s3.open(asset_file, 'r') as f:
                    df_asset = pd.read_csv(f, index_col=0, parse_dates=True)
                df_asset = df_asset.loc[(df_asset.index >= max_date) & (df_asset.index <= end_date)]["close"].to_frame()
                df_asset.columns = [symbol]
                if df_assets is None:
                    df_assets = df_asset
                else:
                    df_assets = pd.concat([df_assets, df_asset], axis=1)
    else:
        for symbol in parameters['Symbols']:
            metadata_file = parameters['Source']['Path'] + '/' + metadata_folder + '/' + symbol + '/' + symbol + '_' + date_for_file_name + '.json'
            if os.path.isfile(metadata_file):
                with open(metadata_file, 'r') as f:
                    symbol_metadata = json.load(f)
                df_metadata.loc[len(df_metadata)] = [symbol, symbol_metadata['volatility'], np.nan, np.nan, np.nan, symbol_metadata['close_before'], symbol_metadata['closest_expiration']]

            asset_file = parameters['Source']['Path'] + '/' + assets_folder + '/' + symbol + '.csv'
            if os.path.isfile(asset_file):
                df_asset = pd.read_csv(asset_file, index_col=0, parse_dates=True)
                df_asset = df_asset.loc[(df_asset.index >= max_date) & (df_asset.index <= end_date)]["close"].to_frame()
                df_asset.columns = [symbol]
                if df_assets is None:
                    df_assets = df_asset
                else:
                    df_assets = pd.concat([df_assets, df_asset], axis=1)

    for i in df_metadata.index:
        symbol = df_metadata.loc[i]['symbol']
        if parameters['Source']['Type'] == 'S3':
            symbol_daily_file = parameters['Source']['Bucket'] + '/' + assets_daily_folder + '/' + symbol + '.csv'
            if s3.exists(symbol_daily_file):
                with s3.open(symbol_daily_file, 'r') as f:
                    df_symbol_daily = pd.read_csv(f, index_col=0, parse_dates=[0])

        else:
            symbol_daily_file = parameters['Source']['Path'] + '/' + assets_daily_folder + '/' + symbol + '.csv'
            if os.path.isfile(symbol_daily_file):
                df_symbol_daily = pd.read_csv(symbol_daily_file, index_col=0, parse_dates=[0])
        df_symbol_daily = df_symbol_daily.loc[(df_symbol_daily.index <= max_date) & (df_symbol_daily.index <= end_date)]['close'][-trend_days:]
        coeffs = np.polyfit(np.arange(trend_days), df_symbol_daily, 1)
        coeffs1 = np.polyfit(np.arange(trend_for_s), df_symbol_daily[-trend_for_s:], 1)
        df_metadata.loc[i, "mean_before"] = df_symbol_daily.mean()
        df_metadata.loc[i, "coeff_b"] = coeffs[0]
        df_metadata.loc[i, "coeff_a"] = coeffs[1]
        df_metadata.loc[i, "trend_b"] = coeffs1[0]
        df_metadata.loc[i, "trend_a"] = coeffs1[1]
        if symbol in df_volatility_shapes.index:
            df_metadata.loc[i, "corrected_volatility"] = df_volatility_shapes.loc[symbol, "c"] * df_metadata.loc[i, "volatility"]
        else:
            df_metadata.loc[i, "corrected_volatility"] = df_metadata.loc[i, "volatility"]

    df_metadata['closest_expiration_date'] = pd.to_datetime(df_metadata['closest_expiration'], format="%Y%m%d")
    df_metadata['t'] = (df_metadata['closest_expiration_date'] - max_date).dt.days/365
    df_metadata['trend_correction'] = df_metadata["trend_b"] * df_metadata['t'] * 365

    if parameters['Source']['Type'] == 'S3':
        options_full_list = history_data_utils.get_dir_levels(1, [parameters['Source']['Bucket'] + '/' + source_folder], parameters['Source'], s3)
    else:
        options_full_list = history_data_utils.get_dir_levels(1, [parameters['Source']['Path'] + '/' + source_folder], parameters['Source'], s3)

    df_options = None
    metadata_symbols = set(df_metadata['symbol'])
    for option_file in options_full_list:
        option_file_name = option_file.split('/')[-1].split(".")[0]
        option_file_date_str = option_file_name[-15:-9]
        if not option_file_date_str:
            continue
        symbol = option_file_name[:-15]
        if not symbol in metadata_symbols:
            continue
        # direction = option_file_name[-9:-8]
        strike = int(option_file_name[-8:])/1000
        if not symbol in parameters['Symbols']:
            continue
        option_file_date = datetime.strptime(option_file_date_str, "%y%m%d")
        if option_file_date <= max_date or option_file_date != df_metadata.loc[df_metadata.symbol == symbol]["closest_expiration_date"].iloc[0]:
            continue
        if parameters['Source']['Type'] == 'S3':
            with s3.open(option_file, 'r') as f:
                df_option = pd.read_csv(f, index_col=0, parse_dates=True)
        else:
            df_option = pd.read_csv(option_file, index_col=0, parse_dates=True)
        df_option = df_option.loc[(df_option.index >= max_date) & (df_option.index <= end_date)]["close"].to_frame()

        df_option.columns = [(symbol, strike, 'close')]
        if df_options is None:
            df_options = df_option
        else:
            df_options = pd.concat([df_options, df_option], axis=1)

#TODO check what to do with many expirations per week - like SPY

    df_options.columns = pd.MultiIndex.from_tuples(df_options.columns)
    df_options.sort_index(axis=1, inplace=True)

    columns_list = df_options.columns.tolist()
    df_BS = df_options.copy()
    df_BS[columns_list] = np.nan
    df_BS1 = df_options.copy()
    df_BS1[columns_list] = np.nan
    df_BS2 = df_options.copy()
    df_BS2[columns_list] = np.nan
    df_BS_S = df_options.copy()
    df_BS_S[columns_list] = np.nan

    df_BS_S[df_BS_S.columns.levels[0]] = df_assets[df_BS_S.columns.levels[0]]
    df_BS_K = np.array([s[1] for s in columns_list])
    df_BS_t = np.array([df_metadata.loc[df_metadata.symbol == s[0]]['t'].tolist()[0] for s in columns_list])
    df_BS_trend_correction = np.array([df_metadata.loc[df_metadata.symbol == s[0]]['trend_correction'].tolist()[0] for s in columns_list])
    df_BS_v = np.array([df_metadata.loc[df_metadata.symbol == s[0]]['corrected_volatility'].tolist()[0] for s in columns_list])

    df_BS1 = (np.log((df_BS_S + df_BS_trend_correction)/df_BS_K) + (RISK_FREE + 0.5*df_BS_v**2) * df_BS_t)/(df_BS_v*np.sqrt(df_BS_t))
    df_BS2 = (np.log((df_BS_S + df_BS_trend_correction)/df_BS_K) + (RISK_FREE - 0.5*df_BS_v**2) * df_BS_t)/(df_BS_v*np.sqrt(df_BS_t))
    df_BS = (df_BS_S + df_BS_trend_correction)*stats.norm.cdf(df_BS1, 0.0, 1.0) - df_BS_K*np.exp(-RISK_FREE*df_BS_t)*stats.norm.cdf(df_BS2, 0.0, 1.0)
    df_BS.columns = pd.MultiIndex.from_tuples([(c[0], c[1], 'BS') for c in df_BS.columns.tolist()])

    print('metadata', df_metadata.shape)
    print('assets', df_assets.shape)
    print('options', df_options.shape)
    print('BS', df_BS.shape)
    if parameters['Destination']['Type'] == 'S3':
        with s3.open(parameters['Destination']['Bucket'] + '/' + destination_folder + '/parameters-' + date_for_file_name + '.csv', 'w') as f:
            df_metadata.to_csv(f)
        with s3.open(parameters['Destination']['Bucket'] + '/' + destination_folder + '/assets-' + date_for_file_name + '.csv', 'w') as f:
            df_assets.to_csv(f)
        with s3.open(parameters['Destination']['Bucket'] + '/' + destination_folder + '/options-' + date_for_file_name + '.csv', 'w') as f:
            df_options.to_csv(f)
        with s3.open(parameters['Destination']['Bucket'] + '/' + destination_folder + '/BS-' + date_for_file_name + '.csv', 'w') as f:
            df_BS.to_csv(f)
    else:
        df_metadata.to_csv(parameters['Destination']['Path'] + '/' + destination_folder + '/parameters-' + date_for_file_name + '.csv')
        df_assets.to_csv(parameters['Destination']['Path'] + '/' + destination_folder + '/assets-' + date_for_file_name + '.csv')
        df_options.to_csv(parameters['Destination']['Path'] + '/' + destination_folder + '/options-' + date_for_file_name + '.csv')
        df_BS.to_csv(parameters['Destination']['Path'] + '/' + destination_folder + '/BS-' + date_for_file_name + '.csv')

    return 0

def reload():
    A = pd.read_csv("/media/Data/.zipline/data_to_ingest/daily/AAPL.csv", index_col=0)
    # for sd in A.loc[(A.index >= '2021-06-10') & (A.index < '2021-07-01')].index:
    for sd in A.loc[A.index >= '2021-10-01'].index:
        da = datetime.strptime(sd, "%Y-%m-%d")
        print(da)
        data_for_XL(da)

if __name__ == '__main__':
    print("start data_for_XL", datetime.now())
    data_for_XL()
    print("finish data_for_XL", datetime.now())

# end_date = datetime.strptime('2022-01-18', "%Y-%m-%d")
# data_for_XL(end_date)
