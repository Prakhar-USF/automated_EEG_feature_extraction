# =========================================================================
# This script reads the spreadsheets containing characteristics metadata about
#  participants and generate a json-format document to upload to MongoDB.
#
# Fields include:
# participant_id, Gender, Gestational_Age, Weight_gms, Maternal_age, 
# Delivery_type, Relative_size, Prematurity_Level, Multiple_births,
# participant_group, num_recording
#
# To run this script:
# $python read_metadata.py [metadata local directory]
# =========================================================================
import os
import re
import sys
import json
import subprocess
import pandas as pd

from tqdm import tqdm
from pathlib import Path
from pymongo import MongoClient

from user_definition import *


# Set mongodb info
client = MongoClient(f'mongodb://{mongos_ip}:{mongos_port}/')
db = client[db_name]

# Set metadata file names
cohort_1_fname = "Cohort_1_participant_characteristics.xlsx"
cohort_2_fname = "Cohort_2_participant_characteristics.xlsx"
characteristics_fname = "Characteristics_for_EEG_data.xlsx"


def list_s3_bucket(path):
    """Get file list from S3 bucket.
    Return: [participant group, filename]
    """
    out = subprocess.check_output(['aws', 's3', 'ls', path, '--recursive'])
    file_ls = out.decode('utf-8').strip().split('\n')

    fpath_ls = []
    for fpath in file_ls:
        if re.match(r'.*_month_EEG/.*\.edf$', fpath):
            fpath_ls.append(fpath.split('/')[-2:])

    return fpath_ls


def categorize(x):
    if isinstance(x, str):
        if x == 'extremely preterm':
            return 1
        elif x == 'very preterm' :
            return 2
        elif x == 'moderate/ late preterm' :
            return 3
        else:
            return -1
    else:
        return -1


def toFloatSafe(x):
    try:
        return float(x)
    except ValueError:
        return str(x) #if it is not a float type return as a string.


if __name__ == "__main__":

    bucket_path = 's3://'+bucket_name+'/'+bucket_prefix
    metadata_root = sys.argv[1]

    # Cohort 1
    p1 = os.path.join(metadata_root, cohort_1_fname)
    cohort1_meta = pd.read_excel(p1, header=1)
    # Rename inconsistent column names
    cohort1_meta = cohort1_meta.rename(columns={"Subject ID": "SubjectID", "Weight (gms)": "Weight_gms", "Gestational Age": "Gestational_Age","Active Diagnoses":"Active_Diagnoses",
                                  "Resolved Diagnoses": "Resolved_Diagnoses", "Medications (Baby)":"Medications_Baby"})
    # Remove rows with Nans
    cohort1_meta = cohort1_meta[~pd.isna(cohort1_meta.SubjectID)]

    
    # Cohort 2
    p2 = os.path.join(metadata_root, cohort_2_fname)
    cohort2_meta = pd.read_excel(p2, header=1)
    # Rename inconsistent column names
    cohort2_meta = cohort2_meta.rename(columns={"Subject ID": "SubjectID", "Birth Weight (grams)": "Weight_gms", "Sex":"Gender", "Gestational Age (weeks)": "Gestational_Age","Active Dx":"Active_Diagnoses",
                                  "Resolved Dx": "Resolved_Diagnoses", "Infant's medications at discharge":"Medications_Baby"})
    # Remove rows with Nans
    cohort2_meta = cohort2_meta[~pd.isna(cohort2_meta.SubjectID)]

    
    # Combine cohort 1 and 2
    cohort1_meta = cohort1_meta.set_index('SubjectID')
    cohort2_meta = cohort2_meta.set_index('SubjectID')
    cohort_all = pd.concat([cohort1_meta, cohort2_meta])


    # Information on preterm level, maternal age, etc
    p3 = os.path.join(metadata_root, characteristics_fname)
    cohort1_chars = pd.read_excel(p3, header=0, sheet_name="Cohort 1")
    cohort2_chars = pd.read_excel(p3, header=0, sheet_name="Cohort 2")
    # Rename inconsistent column names
    cohort1_chars = cohort1_chars.rename(columns={"Study ID":"SubjectID", "Maternal age": "Maternal_age",
                                   "Delivery type":"Delivery_type",
                                   'Relative size SGA/ AGA/ LGA ':"Relative_size",
                                   'Prematurity Level:  extremely preterm ( <28 weeks)\nvery preterm (28 to 32 weeks)\nmoderate/ late preterm (32 to 37 weeks)':"Prematurity_Level",
                                  "Multiple births (no = 1, twins = 2, triplets = 3)": "Multiple_births"})
    cohort2_chars = cohort2_chars.rename(columns={"Study ID":"SubjectID", "Maternal age": "Maternal_age",
                                   "Delivery type":"Delivery_type",
                                   'Relative size (AGA = 1, SGA = 2, LGA = 3) ':"Relative_size",
                                   'Prematurity Level:  extremely preterm ( <28 weeks)\nvery preterm (28 to 32 weeks)\nmoderate/ late preterm (32 to 37 weeks)':"Prematurity_Level",
                                  "Multiple births (no = 1, twins = 2, triplets = 3)": "Multiple_births"})
    
    cohort1_chars = cohort1_chars[['SubjectID','Sex', 'Gestational age', 'Birth Weight', 'Maternal_age','Delivery_type', 'Relative_size','Prematurity_Level','Multiple_births']]
    cohort2_chars = cohort2_chars[['SubjectID','Sex', 'Gestational age', 'Birth Weight', 'Maternal_age','Delivery_type', 'Relative_size','Prematurity_Level','Multiple_births']]
    # # Remove rows with NaNs
    cohort1_chars = cohort1_chars[~pd.isna(cohort1_chars.SubjectID)]
    cohort2_chars = cohort2_chars[~pd.isna(cohort2_chars.SubjectID)]

    # Different nomenclature between cohort 1 and 2. Convert to numbers in cohort 1
    cohort1_chars['Prematurity_Level'] = cohort1_chars['Prematurity_Level'].apply(lambda x: int(categorize(x)))

    cohortChars_all = pd.concat([cohort1_chars, cohort2_chars])
    cohortChars_all = cohortChars_all.set_index('SubjectID')
    cohortChars_all = cohortChars_all.iloc[:-1, :]

    numerical_columns = ['Gestational age', 'Birth Weight', 'Maternal_age', 'Prematurity_Level', 'Multiple_births']

    # Remove spaces in decimals
    for c in numerical_columns:
        cohortChars_all[c] = cohortChars_all[c].apply(lambda x: toFloatSafe(x))

    cohortChars_all = cohortChars_all.reset_index(drop=False)
    cohortChars_all.rename(columns={'SubjectID':'participant_id',
                             'Sex':'Gender', 
                             'Gestational age':'Gestational_Age',
                             'Birth Weight':'Weight_gms'}, inplace=True)


    # Check for participant_id and add test group
    cohortChars_all['participant_group'] = ''
    cohortChars_all['num_recording'] = 0

    # Check with files in S3 bucket
    fpath_ls = list_s3_bucket(bucket_path)
    for group, name in fpath_ls:
        p_id = re.search(r'[A-Z]\d+-\d-\d', name).group()
        if p_id in list(cohortChars_all['participant_id']):
            condition = (cohortChars_all.participant_id == p_id)
            # when there are multi-match of id
            if sum(condition) > 1:
                condition = condition.replace(condition[condition == 1].iloc[:-1], False)
            # check id and test group
            if (cohortChars_all.loc[condition, 'participant_group'] == group).values[-1]:
                cohortChars_all.loc[condition, 'num_recording'] += 1
            elif (cohortChars_all.loc[condition, 'participant_group'] == '').values:
                cohortChars_all.loc[condition, 'participant_group'] = group
                cohortChars_all.loc[condition, 'num_recording'] += 1
            else:
                cohortChars_all = cohortChars_all.append(cohortChars_all.loc[condition, :])
                cohortChars_all = cohortChars_all.reset_index(drop=True)
                cohortChars_all.loc[cohortChars_all.index[-1], 'num_recording'] = 1  # reset count for last row
                cohortChars_all.loc[cohortChars_all.index[-1], 'participant_group'] = group
    cohortChars_all['num_recording'] = 0 # clean count for preprocessing
    cohortChars_all = cohortChars_all.reset_index(drop=True)


    for i in tqdm(range(len(cohortChars_all))):
        if cohortChars_all['participant_group'][i] != '':
            # output_file = str(cohortChars_all['participant_id'][i])+'_'+str(cohortChars_all['participant_group'][i])+'_meta_data.json'
            # out_p = os.path.join(out_path, output_file)
            # f = open(out_p, 'w')
            # f.write(cohortChars_all.iloc[i, :].to_json())
            # f.write('\n')
            doc = json.loads(cohortChars_all.iloc[i, :].to_json())
            db.eeg_metadata.insert_one(doc)
        # For participants who currently hava missing signal recordings
        # Initialize test group value as 6 month for cohort1 and 12 month for cohort2
        elif cohortChars_all['participant_group'][i] == '':
            if cohortChars_all['participant_id'][i].startswith('A'):
                cohortChars_all.loc[i, 'participant_group'] = '06_month_EEG' 
            else:
                cohortChars_all.loc[i, 'participant_group'] = '12_month_EEG'
            doc = json.loads(cohortChars_all.iloc[i, :].to_json())
            db.eeg_metadata.insert_one(doc)