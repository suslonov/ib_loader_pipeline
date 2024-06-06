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
        "Levels":
"""

import sys
import os
import json
from datetime import datetime
import pytz
import s3fs

import history_data_utils

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC

def option_metadata_updater():
    if len(sys.argv) < 2:
        sys.argv.append(os.path.expanduser('~/fr_config/download_options_metadata'))

    with open(sys.argv[1], 'r') as f:
        parameters = json.load(f)

    source_folder = parameters['Source']['Metadata']
    destination_folder = parameters['Destination']['Metadata']
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
        source_full_list = history_data_utils.get_dir(parameters['Source']['Bucket'] + '/' + source_folder, parameters['Source'], s3)
    else:
        source_full_list = history_data_utils.get_dir(parameters['Source']['Path'] + '/' + source_folder, parameters['Source'], s3)

    for source_symbol in source_full_list:
        asset_symbol = source_symbol.split('/')[-1].split('_')[0]
        slice_files = history_data_utils.get_dir(source_symbol, parameters['Source'], s3)
        max_date = 0
        for slice_file in slice_files:
            file_date = datetime.strptime(slice_file.split('/')[-1].split("_")[1].split(".")[0], "%Y-%m-%d").date()
            if max_date == 0 or max_date <= file_date:
                max_date = file_date
        date_for_file_name = str(max_date)

        if parameters['Source']['Type'] == 'S3':
            with s3.open(parameters['Source']['Bucket'] + '/' + source_folder + '/' +  asset_symbol + '/' + asset_symbol + '_' + date_for_file_name + '.json', 'r') as f:
                buffer = f.read()
        else:
            with open(parameters['Destination']['Path'] + '/' + source_folder + '/' + asset_symbol + '/' + asset_symbol + '_' + date_for_file_name + '.json', 'w') as f:
                buffer = f.read()

        if parameters['Destination']['Type'] == 'S3':
            print(parameters['Destination']['Bucket'] + '/' + destination_folder + '/' +  asset_symbol + '/' + asset_symbol + '_' + date_for_file_name + '.json')
            with s3.open(parameters['Destination']['Bucket'] + '/' + destination_folder + '/' +  asset_symbol + '/' + asset_symbol + '_' + date_for_file_name + '.json', 'w') as f:
                f.write(buffer)
        else:
            print(parameters['Destination']['Path'] + '/' + destination_folder + '/' + asset_symbol + '/' + asset_symbol + '_' + date_for_file_name + '.json')
            if not os.path.isdir(parameters['Destination']['Path'] + '/' + destination_folder + '/' + asset_symbol):
                os.mkdir(parameters['Destination']['Path'] + '/' + destination_folder + '/' + asset_symbol)
            with open(parameters['Destination']['Path'] + '/' + destination_folder + '/' + asset_symbol + '/' + asset_symbol + '_' + date_for_file_name + '.json', 'w') as f:
                f.write(buffer)

    return 0

if __name__ == '__main__':
    print("start option_metadata_updater", datetime.now())
    option_metadata_updater()
    print("finish option_metadata_updater", datetime.now())
