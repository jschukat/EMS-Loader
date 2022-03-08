import requests
import py7zr
import tarfile
from io import BytesIO
import zipfile
import gzip
import pandas as pd
from datetime import datetime
from EMS_cloud_module import cloud
import logging
from EMS_classes import ibc_file
import os

os.environ['NUMEXPR_MAX_THREADS'] = '16'
log = logging.getLogger(__name__)


    
def decider(indicator, value):
    if indicator == 'float':
        return flt(value)
    elif indicator == 'int':
        return inti(value)
    elif indicator == 'time':
        return time_casting(value)
    else:
        return dt(value)

    
def time_casting(df):
    try:
        df = '19700101'+df
        return dt(df)
    except Exception as e:
        log.error(f'casting {df} to time failed with {e}')

        
def dt(df):
    try:
        df = pd.to_datetime(df, errors='coerce')
        return df.astype('datetime64')
    except Exception as e:
        log.error(f'casting {df} to datetime failed with {e}')

        
def flt(df):
    try:
        mask = df.str.contains('-')
        df = df.str.replace('-', '').str.strip()
        df = pd.to_numeric(df, downcast='float', errors='coerce')
        #df = df.astype(float)
        return df.mask(mask, -df)
    except Exception as e:
        log.error(f'casting {df} to float failed with {e}')

        
def inti(df):
    try:
        mask = df.str.contains('-')
        df = df.str.replace('-', '').str.strip()
        df = pd.to_numeric(df, downcast='signed', errors='coerce')
        #df = df.str.replace('-', '').str.strip().astype('int64')
        #df = df.astype(int)
        return df.mask(mask, -df)
    except Exception as e:
        log.error(f'casting {df} to int failed with {e}')



def sap_load(lst, pwd=None):
    #zip(relevant_files, const(pre_url), const(header_json), const(header), const(jobhandle['id']))
    file = lst[0]
    pre_url = lst[1]
    header_json = lst[2]
    header = lst[3]
    job_id = lst[4]
    encoding = 'utf-8'
    df = lst[5]
    type_dict = lst[6]
    uppie = lst[7]
    
    try:
        log.info(f'uploading chunk: {file.file}')
        url = pre_url + file.file
        with requests.get(url, headers=header_json, stream=False) as r:
            r.raise_for_status()
            if 'HEADER' in file.file:
                return
            elif file.file.split('.')[-1] == 'zip':
                z = zipfile.ZipFile(BytesIO(r.content))
                fh = z.open(z.infolist()[0])
            elif file.file.split('.')[-1] == '7z':
                z = py7zr.SevenZipFile(BytesIO(r.content), password=pwd)
                filename = z.getnames()[0]
                fh = z.read(filename)[filename]
                log.info(fh)
            elif file.file.split('.')[-1] == 'gz':
                fh = gzip.GzipFile(fileobj=BytesIO(r.content), mode='rb')
            else:
                fh = BytesIO(r.content)
            df_up = pd.read_csv(fh, header=None, dtype=str, sep=';', names=list(df['names']), quotechar='"', encoding=encoding, escapechar='\\')
            if len(type_dict) > 0:
                for i in type_dict:
                    df_up[type_dict[i]] = df_up[type_dict[i]].apply(lambda x: decider(i, x), axis=0)
                    log.debug(f'conversion to {i} resulted in {df_up[type_dict[i]].dtypes.value_counts()}')
            uppie.push_new_chunk(pool_id=header.poolid, job_id=job_id, dataframe=df_up)
    except Exception as e:
        log.error(f'{file.file} failed with error: {e}')