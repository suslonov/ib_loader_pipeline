#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
file and directory functions
"""

import os

def get_file_list(root, folder, symbol, s3):
    if root['Type'] == 'S3':
        return s3.ls(root['Bucket'] + '/' + folder + '/' + symbol + '/')
    if os.path.isdir(root['Path'] + '/' + folder + '/' + symbol):
        return os.listdir(root['Path'] + '/' + folder + '/' + symbol + '/')
    return []

def get_dir(path, root, s3):
    if root['Type'] == 'S3':
        return s3.ls(path)
    if os.path.isdir(path):
        return [path + '/' + d for d in os.listdir(path)]
    return []

def get_dir_levels(level, dir_list, root, s3):
    level_list = [dd for d in dir_list for dd in get_dir(d, root, s3)]
    if level > 1:
        return [dd for d in level_list for dd in get_dir_levels(level-1, [d], root, s3)]
    return level_list

def is_file_exist(root, folder, symbol, s3):
    if root['Type'] == 'S3':
        return s3.exists(root['Bucket'] + '/' + folder + '/' + symbol + '.csv')
    return os.path.isfile(root['Path'] + '/' + folder + '/' + symbol + '.csv')

def make_option_symbol_string(symbol, strike, direction, expiration):
    integer_fractional = strike.split('.')
    fractional = f'{int(integer_fractional[1]):d}'
    if len(fractional) < 3:
        fractional = fractional + '0'*(3-len(fractional))
    strike_string = f'{int(integer_fractional[0]):05d}' + fractional
    return symbol + expiration + direction + strike_string
