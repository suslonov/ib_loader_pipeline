#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring.
"""
Garbage collector to delete old intraday slices
Parameters:
1 JSON description file
        "Destination": {"Type":
                "Bucket":
                "Path":
                "Folder":
                }
"""
import sys
import os
import json
from datetime import datetime, timedelta
import pytz
import s3fs

import history_data_utils

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC

def current_data_clean():
    if len(sys.argv) < 2:
        # sys.argv.append(os.path.expanduser('~/fr_config/current_data_slice'))
        sys.argv.append(os.path.expanduser('~/fr_config/download_stocks_current'))

    with open(sys.argv[1], 'r') as f:
        parameters = json.load(f)

    now_time = datetime.now(EASTERN)
    date_limit = now_time.replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0) \
                - timedelta(days=parameters["DaysToKeep"])

    destination_folder = parameters['Destination']['Folder']
    if parameters['Destination']['Type'] == 'S3':
        key = os.environ['AWS_ACCESS_KEY']
        secret = os.environ['AWS_SECRET_ACCESS_KEY']
        s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret)
    else:
        s3 = None
    if 'Path' in parameters['Destination']:
        parameters['Destination']['Path'] = os.path.expanduser(parameters['Destination']['Path'])

    if parameters['Destination']['Type'] == 'S3':
        files_full_list = history_data_utils.get_dir(parameters['Destination']['Bucket'] + '/'
                                                     + destination_folder, parameters['Destination'], s3)
    else:
        files_full_list = history_data_utils.get_dir(parameters['Destination']['Path'] + '/'
                                                     + destination_folder, parameters['Destination'], s3)

    file_counter = 0
    for f in files_full_list:
        file_date_time = datetime.strptime(f.split('/')[-1].split('_')[1].split('.')[0], "%Y-%m-%d %H:%M:%S")
        if file_date_time < date_limit:
            if parameters['Destination']['Type'] == 'S3':
                s3.rm(f)
            else:
                os.remove(f)
            file_counter += 1
    print("delete", file_counter, "files")

    return 0

if __name__ == '__main__':
    print("start current_data_clean", datetime.now())
    current_data_clean()
    print("finish current_data_clean", datetime.now())
