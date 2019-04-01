#!/usr/bin/env python

import os
import pywt
import numpy as np
from pyrqa3.settings import Settings
from pyrqa3.neighbourhood import FixedRadius
from pyrqa3.computation import RQAComputation
import pyedflib
import nolds
import multiprocessing as mp
from tqdm import tqdm
import boto3
import sys
import glob
import re

import warnings
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")
warnings.filterwarnings(action="ignore", module="nolds", message="^`rcond` parameter")

from pyrqa.opencl import OpenCL
opencl = OpenCL(platform_id=0,
                device_ids=(0,1,))

INPUT_PATH = '/tmp/data/'
#OUTPUT_PATH = '/tmp/output/'
UTPUT_PATH = '/Users/ncross/git/meddata/EEG_mod/'
_BUCKET_NAME = 'usfcaeeg'  # bucket name 
_PREFIX = 'Test' # subfolder name
FILENAME_PATTERN = '[\s,0-9,A-Z,a-z,_,-]*.edf$' #combination of alpha numeric, _, - and space,  ending with .edf

client = boto3.client('s3')

max_nt = 30 # length of time segment in seconds
all_features = ["Power", "SampE", "hurstrs","dfa", "lyap0", "lyap1", "lyap2", 
                "RR", "DET", "LAM", "Lentr", "Lmax", "Lmean", "TT"]
master_channel_list = ["Fp1","Fp2","F7","F3","Fz","F4","F8","T7","C3","Cz",
                       "C4","T8","P7","P3","Pz","P4","P8","O1","O2"]


def power(y): return np.sum(y**2)/y.size
def sampE(y): return nolds.sampen(y)
def hurstrs(y): return nolds.hurst_rs(y)
def dfa(y): return nolds.dfa(y)
def lyap(y,emb_dim): return nolds.lyap_e(y, emb_dim)
def RR(result): return result.recurrence_rate
def DET(result): return result.determinism
def LAM(result): return result.laminarity
def Lentr(result): return result.entropy_diagonal_lines
def Lmax(result): return result.longest_diagonal_line
def Lmean(result): return result.average_diagonal_line
def TT(result): return result.trapping_time

function_dict = {"Power": power,"SampE":sampE, "hurstrs":hurstrs,
                 "dfa": dfa,"lyap":lyap,"RR": RR, "DET": DET, 
                 "LAM": LAM, "Lentr": Lentr,"Lmax":Lmax,
                 "Lmean":Lmean,"TT":TT }



class Data:
    
    def __init__(self,filepath,max_nt):
        self.wavelet, self.mode = 'db4','cpd'
        self.D = {}
        self.data,self.channelNames,self.srate = self.extract_time_series(filepath,max_nt)
        self.levels = self._set_levels()
        self.nbands = self.levels+1
        self.f_limit,self.f_labels = self._set_f_limit(),[]
        self.freqband = []
        self.embedding, self.tdelay, self.tau = 10, 2, 30 # RQA parameters: embed_dim, time_delay, threshold
        self.write_headers = True
        
    def extract_time_series(self,filepath,max_nt):
        print(f'Processing {filepath}')
        f = pyedflib.EdfReader(filepath)         # read the edf file 

        channelNames = f.getSignalLabels()       # channelnames
        srate = f.getSampleFrequency(3)          # sampling rate
        n = f.signals_in_file                    # number of signals 
        data = np.zeros((n, f.getNSamples()[0])) 
        for i in np.arange(n):
            data[i, :] = f.readSignal(i)         # shitty implementation of pyedflib 
        nt = max_nt*srate                        # number of time periods
        m1 = 0                                   # start time                          
        m2 = m1 + nt                             # end time
        data = data[:,m1:m2]                     # truncating data to the max number of time periods (in s)
        return data,channelNames,srate
        
    def _set_levels(self):
        print(f'Setting levels...')
        # Determine the number of levels required so that the lowest level approximation is roughly the
        # delta band (freq range 0-4 Hz)
        if   self.srate <= 128:  levels = 4
        elif self.srate <= 256:  levels = 5
        elif self.srate <= 512:  levels = 6
        elif self.srate <= 1024: levels = 7
        return levels
        
    def _set_f_limit(self):
        # We also keep track of the frequency bands represented in the array f_limits
        # We don't actually do anything with this - it's just here for testing
        # when I want to print out frequency band limits.
        print(f'Setting frequency bands...')
        f_limit = np.ones(self.nbands+1) * self.srate / (2.0 **(self.nbands+1))
        f_limit[self.nbands] = self.srate / 2.0
        for i in range(1,self.nbands):
            f_limit[i] = f_limit[i-1] * 2.0
        f_limit[0] = 0.0
        return f_limit
    
    def _features_settings(self,chnls,all_features):
        print('Setting features....')
        w = pywt.Wavelet(self.wavelet)
        for c, ch in enumerate(self.channelNames):
            
            if ch in chnls:
                self.D[ch] = {}
                m = np.mean(self.data[c])
                a_orig = self.data[c]-m           # the original signal, initially
                a = a_orig

                rec_a,rec_d = [] ,[]               # all the approximations and details

                for i in range(self.nbands):
                    (a, d) = pywt.dwt(a, w, self.mode)
                    f = pow(np.sqrt(2.0), i+1)
                    rec_a.append(a/f)
                    rec_d.append(d/f)

                # Use the details and last approximation to create all the power-of-2 freq bands
                self.f_labels,self.freqband = ['A0'],[a_orig] # A0 is the original signal
                fs = [self.srate]
                f = fs[0] 
                N = len(a_orig)
                
                for j,r in enumerate(rec_d):
                    freq_name = 'D' + str(j+1)
                    self.f_labels.append(freq_name)
                    self.freqband.append(r[0:N])          # wavelet details for this band
                    fs.append(f)
                    f = f/2.0

                # We need one more
                f = f/2.0
                fs.append(f)

                # Keep only the last approximation
                j = len(rec_d)-1
                freq_name = 'A' + str(j+1)
                self.f_labels.append(freq_name)
                self.freqband.append(rec_a[j])       # wavelet approximation for this band
                
                for f in all_features:
                    self.D[ch][f] = {}

    def _compute_nonrqa_features(self,all_features,chnls,function_dict):
        #--------------------------------------------------------------------
        # Compute features such as Power, Sample Entropy, Hurst parameter, DFA, Lyapunov exponents on each of the frequency bands
        #--------------------------------------------------------------------
        nonrqa_features = ['Power','SampE','hurstrs','dfa','lyap0','lyap1','lyap2']
        for c, ch in enumerate(tqdm(self.channelNames)):
            if ch in chnls:
                for i, y in enumerate(self.freqband):
                    for feat in nonrqa_features:
                        if feat in all_features:
                            print(feat, i,y,ch)
                            if feat.startswith('lyap'):
                                lyap = function_dict['lyap'](y,self.embedding)
                                num = int(feat[-1])
                                self.D[ch][feat][self.f_labels[i]] = lyap[num]
                            else:
                                self.D[ch][feat][self.f_labels[i]] = function_dict[feat](y)

    def _compute_rqa_features(self,all_features,chnls,function_dict):
        #----------------------
        # Feature set 3: Recurrence Quantitative Analysis (RQA)
        # 'recurrence_rate', 'determinism', 'laminarity', 'entropy_diagonal_lines',
        #'longest_diagonal_line','average_diagonal_line', 'trapping_time'  
        
        rqa_features = ["RR", "DET", "LAM", "Lentr", "Lmax", "Lmean", "TT"]
        
        compute_RQA = False
        for r in rqa_features:
            if r in all_features:
                compute_RQA = True
                break
        # First check to see if RQA values are needed at all
        if compute_RQA:
            for c, ch in enumerate(self.channelNames):
                if ch in chnls:
                    for i, y in enumerate(self.freqband):
                        print('aaaa', i,y)
                        print(self.embedding)
                        settings = Settings(y, embedding_dimension=self.embedding,time_delay=self.tdelay,
                                            neighbourhood=FixedRadius(self.tau))
                        computation = RQAComputation.create(settings, verbose=True, opencl=opencl)
                        result = computation.run()
                        
                        for feat in rqa_features:
                            if feat in all_features:
                                self.D[ch][feat][self.f_labels[i]] = function_dict[feat](result)
                        self._screen_feedback(all_features,ch,i)

    def _screen_feedback(self,all_features,ch,freqbandno):
        # Write results from first channel to the screen, to give
        # visual feedback that the code is running
        lab = self.f_labels[freqbandno]
        sys.stdout.write( "%10s %6s " %(ch, lab ) )
        for f in all_features:
            print('\n\nasdf')
            print(ch,f,lab)
            print(self.D[ch])
            v = self.D[ch][f][lab]
            sys.stdout.write( " %8.3f " %(v) )
        sys.stdout.write("\n")

    def compute_features(self,all_features,chnls,function_dict):
        self._features_settings(chnls,all_features)
        print('Computing Non RQA features....')
        self._compute_nonrqa_features(all_features,chnls,function_dict)
        print('Computing RQA features....')
        self._compute_rqa_features(all_features,chnls,function_dict)
        
    def write_features(self,outputfilename):
        if self.write_headers:
            fout = open(outputfilename, 'w')
            for ch in list(self.D.keys()):                   # all_channels:  # D.keys():
                for f in list(self.D[ch].keys()):            # all_features:  # D[ch].keys():
                    for lab in list(self.D[ch][f].keys()):
                        h = ch + ':' + f + ':' + lab
                        fout.write(", %s" % (h))
            fout.write("\n")
        for ch in list(self.D.keys()):                       # all_channels:  # D.keys():
            for f in list(self.D[ch].keys()):                # all_features:  # D[ch].keys():
                for lab in list(self.D[ch][f].keys()):
                    fout.write(", %7.3e " % (self.D[ch][f][lab]))
        fout.write("\n")  
        self.write_headers = False  
        
# List files in S3 
def ListFiles(client):
    """List files in specific S3 URL"""
    response = client.list_objects(Bucket=_BUCKET_NAME, Prefix=_PREFIX)
    for content in response.get('Contents', []):
        yield content.get('Key')
        
def process(filename):
    filename = re.search(FILENAME_PATTERN, filename.lower()).group(0)
    local_file_name = _PREFIX.replace("/","__") + "__" + filename

    os.chdir(INPUT_PATH)
    client.download_file(_BUCKET_NAME, _PREFIX + '/' + filename,  local_file_name)
    data_obj = Data(local_file_name,max_nt)
    print("before compute_feautres")
    data_obj.compute_features(all_features,master_channel_list,function_dict)
    print("after compute_features")
    filename = f'{OUTPUT_PATH}out_{local_file_name}'
    print(filename)
    data_obj.write_features(filename)

def processLcl(filename):
    #os.chdir(INPUT_PATH)    
    #print(re.match(FILENAME_PATTERN, filename))
    OUTPUT_PATH='/Users/ncross/git/meddata/EEG_mod'
    data_obj = Data(filename,max_nt)
    data_obj.compute_features(all_features,master_channel_list,function_dict)
    filename = f'{OUTPUT_PATH}out_{filename}'
    data_obj.write_features(filename)


#----------------------------------------------------------------------
# Main
#----------------------------------------------------------------------
# multiprocessing the functions

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ### If an argument is given, then assume it is a file list and run it locally, as opposed
        ### to using the S3 buckets
        file_list = sys.argv[1:]
        print(file_list)
        files = [f for f in file_list if f.lower().endswith('.edf') and os.path.getsize(f) > 0]   
        #pool = mp.Pool(processes=4)
        #results = pool.map(processLcl, files)
        processLcl(file_list[0])

    else:
        file_list = ListFiles(client)
        files = [f for f in file_list if f.lower().endswith('.edf')]
        pool = mp.Pool(processes=4)
        results = pool.map(process, files)

## EOF ##

