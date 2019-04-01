# ==============================================
# To run this script:
# $python extract_edf.py [edf root dir] [signal duration(sec)] [target dir for csv]
# ==============================================

import sys
import os
import re
import glob
import pyedflib
from pathlib import Path
import json
from collections import defaultdict
import numpy as np
from tqdm import tqdm


def get_edf_filepaths(dir_name:str):
    """Returns a list of edf file paths under a directory."""
    path = str(Path(dir_name)/'*.edf')
    file_paths = glob.glob(path)
    return file_paths


def all_file_paths(path:str):
    """Returns a dictionary: {test_group: a list of edf file paths}."""
    path_dict = defaultdict(list)

    for d in os.listdir(path):
        if d != '.DS_Store':
            dir_name = os.path.join(path, d)
            path_dict[d] = get_edf_filepaths(dir_name)
    return path_dict


def extract_metadata(file_path:str, test_group:str):
    """Extract metadata from an edf file.
    Output path: ROOT/meta_data/test_group/*.csv
    """
    f = pyedflib.EdfReader(file_path)  # open the edf file
    participant_id = re.search(
        '[A-Z]\d+-\d-\d', file_path.split('/')[-1]).group()
    signals_in_file = f.getSignalLabels()
    signal_duration = f.file_duration # in seconds
    headers = f.getHeader()
    start_dt = headers.pop('startdate', None)
    signal_info = f.getSignalHeaders()
    all_info = {'participant_id': participant_id,
                'participant_group': test_group,
                'start_datetime': str(start_dt),
                'signals_in_file': {'number_of_signals': len(signals_in_file),
                                    'signal_labels': signals_in_file,
                                    'signal_duration': signal_duration},
                'headers': headers,
                'signal_info': signal_info}
    output_fname = file_path.split('/')[-1].replace('.edf', '.json')
    output_dir = os.path.join(ROOT+'_meta_data/', test_group)
    
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    with open(os.path.join(output_dir, output_fname), 'w') as fn:
        json.dump(all_info, fn)


def extract_time_series(filepath, max_nt):
    """Extract time series data from an edf file."""
    f = pyedflib.EdfReader(filepath)         # read the edf file 

    channelNames = f.getSignalLabels()       # channelnames
    srate = f.getSampleFrequency(3)          # sampling rate
    n = f.signals_in_file                    # number of signals 
    data = np.zeros((n, f.getNSamples()[0])) 
    for i in np.arange(n):
        data[i, :] = f.readSignal(i)
    nt = int(max_nt) * srate                  # number of time periods
    if int(max_nt) > f.file_duration:
        tqdm.write(f'File {filepath} only has {f.file_duration} seconds in length. Extracting all.')
    m1 = 0                                    # start time                          
    m2 = m1 + nt                              # end time
    data = data[:, m1:m2]                     # truncating data to the max number of time periods (in s)

    return data, channelNames


def edf2csv(full_path, test_group, target_dir, signal_time=3000):
    """Write time series data to a csv file."""
    data, channelNames = extract_time_series(full_path, signal_time)
    new_dir = os.path.join(target_dir, test_group)
    
    if not os.path.exists(new_dir):
        os.makedirs(new_dir)
    
    new_path = os.path.join(new_dir, full_path.split('/')[-1].replace('edf', 'csv'))
    if not os.path.exists(new_path):
        np.savetxt(new_path, data, delimiter=',',
                    header=','.join(channelNames))


if __name__ == "__main__":
    ROOT = sys.argv[1]  # path of Rutgers/ folder consisting all edf files
    signal_duration = sys.argv[2]   # user specify the signal duration
    new_dir = sys.argv[3]   # user specify the target path
    path_dict = all_file_paths(ROOT)

    for key, value in path_dict.items():
        for file in tqdm(path_dict[key]):
            try:
                extract_metadata(file, key)
                edf2csv(file, key, new_dir, signal_duration)
            except:
                tqdm.write(f'File {file} cannot be open.')
