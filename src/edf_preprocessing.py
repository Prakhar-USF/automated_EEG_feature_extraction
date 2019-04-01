# =========================================================================
# This script downloads all not-preprocessed edf files to local path,
# then it will extract time series data and meta data from each edf file.
# Both part of data for each channel in the signal will be put together
# in one json-format document and insert to MongoDB directly.
#
# Currently the package we use to read edf files can only take local file path,
# so we cannot directly read raw file into memory and process it.
# =========================================================================

import os
import re
import sys
import glob
import json
import types
import boto3
import shutil
import botocore
import pyedflib
import itertools
import subprocess
import numpy as np

from tqdm import tqdm
from pathlib import Path
from pprint import pprint
from pymongo import MongoClient
from collections import defaultdict

from user_definition import *

# Set mongodb info
client = MongoClient(f'mongodb://{mongos_ip}:{mongos_port}/')
db = client[db_name]

# Set S3 client
s3_client = boto3.client('s3')


def get_edf_filepaths(dir_name):
    """
    Returns a list of edf file paths under a local directory.

    :param dir_name: local directory for the edf files.
    :return: a list
    """
    path = str(Path(dir_name) / '*.edf')
    file_paths = glob.glob(path)
    return file_paths


def all_file_paths(path):
    """
    Returns a dictionary: {test_group: a list of edf file paths}.

    :param path: root directory of all edf files
    :return: dictionary
    """
    path_dict = defaultdict(list)

    for d in os.listdir(path):
        if d != '.DS_Store':
            dir_name = os.path.join(path, d)
            path_dict[d] = get_edf_filepaths(dir_name)
    return path_dict


def list_s3_bucket(bucket_name, bucket_prefix):
    """
    List objects in S3 bucket.

    :param bucket_name: S3 bucket name
    :param bucket_prefix: target folder name in S3 bucket
    :return: list
    """
    kwargs = {'Bucket': bucket_name, 'Prefix': bucket_prefix}
    response = s3_client.list_objects_v2(**kwargs)
    bucket_list = response['Contents']

    return bucket_list


def get_matching_s3_keys(bucket, prefix, target_key):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param target_key: object keys to look for.
    :return: a list of S3 object keys
    """
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    resp = s3_client.list_objects_v2(**kwargs)
    to_be_logged = []
    for obj in resp['Contents']:
        key = obj['Key']
        if key in target_key:
            to_be_logged.append(obj)

    return to_be_logged


def clean_tmp(download_mode=False):
    """
    Clean up the temporary directory for unpreprocessed edf files.

    :param download_mode: if True, it will create empty directories for incoming edf files.
    """
    if os.path.isdir('./s3_tmp/'):
        shutil.rmtree('./s3_tmp/')

    if download_mode:
        os.makedirs('./s3_tmp/06_month_EEG')
        os.makedirs('./s3_tmp/12_month_EEG')
        os.makedirs('./s3_tmp/24_month_EEG')


def download_file(object_key):
    """
    Download file from S3 bucket to ./s3_tmp/*

    :param object_key: Key (full path) for the S3 object to be downloaded
    """
    target_path = os.path.join('./s3_tmp', *object_key.split('/')[-2:])

    # download file
    try:
        s3_client.download_file(bucket_name, object_key, target_path)
        print(f'File {object_key} downloaded. Waiting to be processed.')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def make_log(files_to_be_logged):
    """
    Make log for preprocessed edf files.

    :param files_to_be_logged: output of get_matching_s3_keys()
    :return: a list of dictionaries
    """
    logs = []
    for d in files_to_be_logged:
        log_dict = dict()
        if d['Key'].endswith('.edf'):
            log_dict['file_name'] = d['Key']
            log_dict['last_modified_time'] = str(d['LastModified'])
            log_dict['preprocessing'] = ''
            logs.append(log_dict)

    return logs


def print_object_attrs(obj):
    """
    Extract signal metadata from each edf file.

    :param obj: file object opened by pyedflib.EdfReader()
    :return: a dictionary of attributes
    """
    skip_instances_tuple = (types.BuiltinFunctionType, types.MethodType,
                            types.BuiltinMethodType, types.FunctionType)
    output = defaultdict()
    object_attributes = [atr for atr in obj.__dir__() if not atr.startswith('__')]
    for atr in object_attributes:
        t = getattr(obj, atr)
        if not isinstance(t, skip_instances_tuple):
            value = getattr(obj, atr)
            if isinstance(value, (bytes, bytearray)):
                value = str(value, 'utf-8')
            output[atr] = value
    return output


def extract_edf(file_path):
    """
    Extract time series data from an edf file.

    :param file_path: local file path for a single edf file
    :return: time series data, channel names, info of each signal, headers, and signal metadata
    """
    f = pyedflib.EdfReader(file_path)

    # metadata
    meta = print_object_attrs(f)

    # time series data
    channelNames = f.getSignalLabels()
    signal_info = f.getSignalHeaders()
    headers = f.getHeader()
    headers = {key: str(value) for key, value in headers.items()}
    n = f.signals_in_file
    ts_data = np.zeros((n, f.getNSamples()[0]))
    for i in np.arange(n):
        ts_data[i, :] = f.readSignal(i)

    return ts_data, channelNames, signal_info, headers, meta


def preprocess(full_path):
    """
    Insert both time series data and metadata to MongoDB as one document.

    :param full_path: local file path for a single edf file
    :return: participant id, participant group
    """
    tp_list = []
    # Get two parts of data
    ts_data, channelNames, signal_info, headers, meta = extract_edf(full_path)
    # metadata_dict = extract_metadata(full_path)

    # Add shard key
    participant_group = full_path.split('/')[-2]
    participant_id = re.search(r'[A-Z]\d+-\d-\d', full_path).group()
    meta["participant_id"] = participant_id
    meta["participant_group"] = participant_group

    for i, chn in enumerate(channelNames):
        meta['raw'] = ts_data[i, :].tolist()
        out_dict = {**meta, **signal_info[i], **headers}
        channel_id = db.eeg_raw.insert_one(out_dict)
        tp_list += [{'channel_id': str(channel_id.inserted_id)}]

    return participant_id, participant_group, tp_list


if __name__ == "__main__":
    TEMP = './s3_tmp/'  # tmp directory consisting all edf files for preprocessing

    # List S3 bucket
    bucket_list = list_s3_bucket(bucket_name, bucket_prefix)

    # Check the current preprocessing log in MongoDB
    log = db.preprocess_log.find({}, {'file_name': 1, 'preprocessing': 1, '_id': 0})
    status_ls = dict()
    for r in log:
        status_ls[r['file_name']] = r['preprocessing']

    # Download file if it needs to be preprocessed
    clean_tmp(download_mode=True)
    for file in bucket_list:
        tmp_k = file['Key']  # .replace('test', 'Rutgers') # for test only
        if tmp_k not in status_ls.keys() and tmp_k.endswith('.edf'):
            download_file(file['Key'])

    # download_file('Rutgers/06_month_EEG/20170106141207_A4-1-1.edf')
    # Get file paths of all downloaded files
    path_dict = all_file_paths(TEMP)
    file_list = list(itertools.chain(*list(path_dict.values())))

    # Start preprocessing and update log
    if len(file_list) == 0:
        print('No unpreprocessed files found. Exit...')
    else:
        for file in tqdm(file_list):
            target_file = file.replace('s3_tmp/', bucket_prefix)
            # Initialize log
            to_be_logged = get_matching_s3_keys(bucket_name, bucket_prefix, [target_file])
            file_log = make_log(to_be_logged)[0]
            try:
                participant_id, participant_group, tp_list = preprocess(file)
                tqdm.write(f'File {file} is successfully processed.')
                file_log['preprocessing'] = 'completed'
                db.eeg_metadata.update_one({'participant_id': participant_id, 'participant_group': participant_group},
                                           {'$inc': {'num_recording': 1}}, upsert=True)
                x = db.eeg_metadata.find_one({'participant_id': participant_id, 'participant_group': participant_group})
                exp_id = str(x['_id'])
                tqdm.write('Metadata updated.')

                for item in tp_list:
                    item.update({"exp_id": exp_id})
                    item.update({"status_cpu": 0})               
                    item.update({"status_gpu": 0})
                    item.update({"n_attempts_cpu": 0})
                    item.update({"n_attempts_gpu": 0})      

                db.tracking_participant.insert_many(tp_list)
                
            except Exception as e:
                tp_list = []
                tqdm.write(f'Preprocessing of file {file} failed. Cannot open the file.')
                file_log['preprocessing'] = 'failed'
                print(repr(e))
            db.preprocess_log.insert_one(file_log)

    # Remove s3_tmp/*
    clean_tmp(download_mode=False)
