import os
import sys
import pywt
import json
import nolds

import numpy as np
from math import log2

from pyrqa.settings import Settings
from pyrqa.neighbourhood import FixedRadius
from pyrqa.computation import RQAComputation
from pyrqa.time_series import SingleTimeSeries

from pyspark import SparkContext

from pymongo import MongoClient
from bson.objectid import ObjectId


MAX_RUN = 5
DEBUG = False
no_of_partitions = 10

# MODE = sys.argv[1]
MODE = 'CPU'

if MODE == 'GPU':
    from pyrqa.opencl import OpenCL

# Pyspark mongo config
mongos_ip = '34.219.156.227'
mongos_port = 27017
raw_clxn = 'eeg.eeg_raw'
read_pref = 'readPreference=primaryPreferred'
req_cols = ['raw', 'participant_id', 'participant_group', 'label',
            'startdate', 'sample_rate', 'signals_in_file', '_id',
            'file_duration']
client = MongoClient(f'mongodb://{mongos_ip}:{mongos_port}/')

# Feature config.
nonrqa_features = ['power', 'sample_entropy', 'hurst_exponent', 'dfa', 'lyap0', 'lyap1', 'lyap2']
rqa_features = ['recurrence_rate', 'determinism', 'laminarity',
                'entropy_diagonal_lines', 'longest_diagonal_line',
                'average_diagonal_line', 'trapping_time']
all_features = nonrqa_features + rqa_features
embedding, tdelay, tau = 10, 2, 30
delete_cols = ['raw', 'n_raw', 't_raw']


def power(y):
    return np.sum(y ** 2) / y.size


def sample_entropy(y):
    # Sample Entropy
    return nolds.sampen(y)


def hurst_exponent(y):
    # Hurst exponent
    return nolds.hurst_rs(y)


def dfa(y):
    # Detrended fluctuation analysis
    return nolds.dfa(y)


# what is emb_dim ?
def lyap(y, emb_dim=10):
    # Lyapunov exponent
    return nolds.lyap_e(y, emb_dim)


function_dict = {"power": power, "sample_entropy": sample_entropy,
                 "hurst_exponent": hurst_exponent, "dfa": dfa, "lyap": lyap}


def get_rqa_features(x, f_label_i, is_fail=False):
    res = {f'{k}_{f_label_i}': np.nan for k in rqa_features}
    if not is_fail:
        for fe in rqa_features:
            res[f'{fe}_{f_label_i}'] = getattr(x, fe)
    return res


def trim_data(data, srate, max_nt=30):
    nt = max_nt * srate              # number of time periods
    if data.shape[0] > 60 * srate:
        m1 = 30 * srate
    else:
        m1 = 0                       # start time
    m2 = m1 + nt                     # end time
    trim_data = data[m1:m2]          # truncating data to the max number of time periods (in s)
    return trim_data


def features_settings(data, srate, wavelet='db4', mode='cpd'):

    w = pywt.Wavelet(wavelet)
    a_orig = data - np.mean(data)
    a = a_orig
    nbands = int(log2(srate)) - 1

    rec_a, rec_d = [], []                # all the approximations and details

    for i in range(nbands):
        (a, d) = pywt.dwt(a, w, mode)
        f = pow(np.sqrt(2.0), i + 1)
        rec_a.append(a / f)
        rec_d.append(d / f)

    f_labels, freqband = ['A0'], [a_orig]  # A0 is the original signal
    fs = [srate]
    f = fs[0]
    N = len(a_orig)

    for j, r in enumerate(rec_d):
        freq_name = 'D' + str(j + 1)
        f_labels.append(freq_name)
        freqband.append(r[0:N])          # wavelet details for this band
        fs.append(f)
        f = f / 2.0

    # We need one more
    f = f / 2.0
    fs.append(f)

    j = len(rec_d) - 1
    freq_name = 'A' + str(j + 1)
    f_labels.append(freq_name)
    freqband.append(rec_a[j])       # wavelet approximation for this band
    res = {}
    res['freqband'] = freqband
    res['f_labels'] = f_labels
    return res


def compute_non_rqa_features(freqband, f_labels, nonrqa_features=nonrqa_features):

    feature_calc = {}
    error_feet = {}

    for i, y in enumerate(freqband):
        if 'lyap' in [f[:-1] for f in nonrqa_features]:
            try:
                lyap = function_dict['lyap'](y, embedding)
                for j in range(0, 3):
                    feature_calc[f'lyap{j}' + '_' + f_labels[i]] = lyap[j]
            except Exception as e:
                for j in range(0, 3):
                    feature_calc[f'lyap{j}' + '_' + f_labels[i]] = np.nan
                error_feet = {**{str('lyap_' + f_labels[i]): repr(e)}, **error_feet}
        for feat in [f for f in nonrqa_features if not f.startswith('lyap')]:
            try:
                feature_calc[feat + "_" + f_labels[i]] = function_dict[feat](y)
            except Exception as e:
                feature_calc[feat + "_" + f_labels[i]] = np.nan
                error_feet = {**{str(feat + "_" + f_labels[i]): repr(e)}, **error_feet}

    feature_calc['error_nonrqa_feat'] = error_feet
    return feature_calc


def compute_rqa_features(freqband, f_labels):
    """
    pyopencl once done building the source likes to cache the build,
    it uses cache folder as returned by `user_cache_dir` function of `appdirs` module.
    `appdirs` module derives the cache dir path from the environment variable
    `XDG_CACHE_HOME` for linux based systems if it is set, otherwise redirects to
    /home/user/.cache directory, while the function resides in the spark in the runtime
    it doesn't have permission to write to disk file-system, causing this function to throw
    Permission Error 13, as a fix we set the environment variable to point to hdfs file-system
    path every time.
    """
    os.environ["XDG_CACHE_HOME"] = "hdfs://home/hadoop/.cache"

    opencl = OpenCL(platform_id=0, device_ids=(0,))

    feature_calc = {}
    error_rqa_feat = {}

    for i, y in enumerate(freqband):

        y = SingleTimeSeries(y, embedding_dimension=embedding, time_delay=tdelay)
        settings = Settings(y, neighbourhood=FixedRadius(tau))
        computation = RQAComputation.create(settings, verbose=True, opencl=opencl)
        try:
            result = computation.run()
            result = get_rqa_features(result, f_labels[i])
        except Exception as e:
            error_rqa_feat['error_' + f_labels[i]] = repr(e)
            result = get_rqa_features(None, f_labels[i], is_fail=True)

        feature_calc = {**feature_calc, **result}

    feature_calc = {**feature_calc, **error_rqa_feat}
    return feature_calc


def fix_dtypes(x):
    for key in delete_cols:
        del x[key]
    del x['freqband']
    x['unique_id'] = str(x.pop('_id'))
    for k, v in x.items():
        if isinstance(v, np.floating):
            x[k] = float(x[k])
        if isinstance(v, np.integer):
            x[k] = int(x[k])
    return x


def get_raw_data(id):
    client_t = MongoClient(f'mongodb://{mongos_ip}:{mongos_port}/')
    clxn_t = client_t.eeg.eeg_raw
    x = list(clxn_t.find({'_id': id}))
    client_t.close()
    return x[0]


def keep_req_cols(x):
    remove_keys = [k for k in x.keys() if k not in req_cols]
    for k in remove_keys:
        x.pop(k)
    return x


if MODE == "CPU":
    status_field = "status_cpu"
    attempts_field = "n_attempts_cpu"
if MODE == "GPU":
    status_field = "status_gpu"
    attempts_field = "n_attempts_gpu"

if __name__ == '__main__':

    run_number = 0
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")

    while run_number < MAX_RUN:

        clxn = client.eeg.tracking_participant
        attempts_range = list(range(0, MAX_RUN))

        ids_to_run = list(clxn.find({status_field: 0, attempts_field: {"$in": attempts_range}}))
        total_job = [ObjectId(id['channel_id']) for id in ids_to_run]

        if (len(total_job) > 0):
            clxn.update_many(filter={status_field: 0, attempts_field: {"$in": attempts_range}},
                             update={'$inc': {status_field: 1, attempts_field: 1}},
                             upsert=True)

            rdd = sc.parallelize(total_job)
            rdd = rdd.map(lambda x: get_raw_data(x))
            rdd = rdd.map(lambda x: keep_req_cols(x))
            rdd = rdd.map(lambda x: {**{'n_raw': np.array(x['raw'])}, **x})
            rdd = rdd.map(lambda x: {**{'t_raw': trim_data(x['n_raw'], x['sample_rate'])}, **x})
            rdd = rdd.map(lambda x: {**features_settings(x['t_raw'], x['sample_rate']), **x})
            if MODE == 'CPU':
                rdd = rdd.map(lambda x: {**compute_non_rqa_features(x['freqband'], x['f_labels']), **x})
            if MODE == 'GPU':
                rdd = rdd.map(lambda x: {**compute_rqa_features(x['freqband'], x['f_labels']), **x})
            rdd = rdd.map(lambda x: fix_dtypes(x))
            features = rdd.collect()

            error_list = [{'error_type': k, 'error_msg': v, 'unique_id': d['unique_id'],
                           'participant_group': d['participant_group'], 'participant_id': d['participant_id'],
                           'run_num': run_number + 1}
                          for d in features for k, v in d.items() if k.startswith('error') and len(v) > 0]
            if DEBUG:
                with open('test.json', 'w') as file:
                    for document in features:
                        file.write(json.dumps(document))
                        file.write("\n")

                print(features)
                print(error_list)

            if MODE == 'CPU':
                clxn_fe = client.eeg.eeg_features
            if MODE == 'GPU':
                clxn_fe = client.eeg.eeg_features_rqa

            clxn_fe.insert_many(features)

            clxn = client.eeg.tracking_participant

            update_ids = [str(job) for job in total_job]
            clxn.update_many(filter={'channel_id': {'$in': update_ids}},
                             update={'$inc': {status_field: 1}},
                             upsert=True)

            if len(error_list) > 0:
                for d in error_list:
                    clxn.update_one(filter={'channel_id': d["unique_id"]},
                                    update={'$inc': {status_field: -2}, '$set': {"error_list": d}},
                                    upsert=True)
                error_files = list(clxn.find({attempts_field:{"$in": list(range(1, MAX_RUN))}, status_field:0}))
                error_ids = [y["channel_id"] for y in error_files]
                clxn_fe.remove({"unique_id":{"$in":error_ids}})            
            
        run_number += 1
    client.close()
