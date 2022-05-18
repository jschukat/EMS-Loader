# Specify the target url and if you want to do a delta load
from cloud_upload_config import *

if as_string == 'all string':
    as_string = True
else:
    as_string = False

if delta == 'full':
    delta = False
else:
    delta = True

if exclude_loaded != 'skip':
    exclude_loaded = False
else:
    exclude_loaded = True

look_for_sap_files_globally = False
path_to_folder = ''
continue_from_last_time = False
# this determines how detailed the log is, where INFO is the standard. the list below is ordered from most detailed (DEBUG) to least detailled (CRITICAL)
# logging.DEBUG
# logging.INFO
# logging.WARNING
# logging.ERROR
# logging.CRITICAL
# log_level = logging.DEBUG


global compressed
compressed = ['.tar', '.gz', '.zip', '.7z']

global generic_file_type
generic_file_type = ['.csv', '.xlsx', '.xls', '.parquet']

global sap_file_type
sap_file_type = '(.*)_[0-9]{8}_[0-9]{6}\.'

global encrypted
encrypted = ['.gpg', '.pgp']

import logging
from datetime import datetime

# logname = f'IBC_Loader_log_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
FORMAT = '%(asctime)s %(levelname)s %(message)s'
formatter = logging.Formatter(FORMAT)
logging.basicConfig(format=FORMAT, filename=logname, level=logging.INFO)
print(logname)
logging.info('logging initialized')

try:
    import re
    import subprocess
    import json
    import requests
    import os
    import py7zr
    import tarfile
    from io import BytesIO
    import zipfile
    from itertools import repeat
    import gzip
    import pandas as pd
    from pandas.errors import EmptyDataError
    import numpy as np
    from multiprocessing import get_context
    from pycelonis import get_celonis
    from pycelonis.utils import parquet_utils
    from chardet.universaldetector import UniversalDetector
    import pycelonis
    import fastparquet as fp
    import pyarrow as pa
    import time
    from copy import deepcopy
    import csv
    from EMS_cloud_module import cloud
    from EMS_classes import ibc_team, bucket, folder, ibc_file
    import itertools
    from os import listdir
    from os.path import isfile, join
    from pathlib import Path
    from itertools import product
    import copy
    import sys
    import traceback as tb
    from mp_sap import sap_load
    # from lib.cloud_module import cloud
    # from lib.upload_module import import_sap_header
except ModuleNotFoundError as e:
    logging.error(e)
    logging.error('please install missing packages to use this program.')
    print('shutting down')
    quit()

if agreed != 'yes':
    logging.error('you need to read and accept the terms listed in disclaimer.md')
    quit()

os.environ['NUMEXPR_MAX_THREADS'] = '16'


def determine_chunk_size(number_of_columns):
    chunk_size = (-30 * number_of_columns + 5900) / 9
    if chunk_size < 100:
        chunk_size = 100
    elif chunk_size > 500:
        chunk_size = 500
    return int(chunk_size) * 1000


def determine_tables_loaded(ibc_team):
    # Create new table with the subset we're interested in
    data = None
    celonis = get_celonis(key_type="APP_KEY")
    logging.info('checking for tables that have already been loaded.')
    random_name = f'zzz___TEMP___{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}'

    if ibc_team.connectionid is None:
        create_table_from_query_statement = f'CREATE TABLE IF NOT EXISTS "{random_name}" AS (SELECT table_name FROM tables WHERE table_schema = \'{ibc_team.poolid}\');'
    else:
        create_table_from_query_statement = f'CREATE TABLE IF NOT EXISTS "{random_name}" AS (SELECT table_name FROM tables WHERE table_schema = \'{ibc_team.poolid}_{ibc_team.connectionid}\');'
    # table_name FROM tables where table_schema = \'\'

    # Create data job and run table creation script
    p = celonis.pools.find(ibc_team.poolid)
    counter = 0
    while counter < 4:
        counter += 1
        try:
            dj = p.create_data_job(random_name)
            transf = dj.create_transformation(random_name, create_table_from_query_statement)
            transf.execute()

            # Create temporary data model in pool and add recently created table, then reload
            dm = p.create_datamodel(random_name)
            try:
                dm.add_table_from_pool(random_name)
                dm.reload(from_cache=False, wait_for_reload=True)
                time.sleep(3)

                # Find table object in data model and download
                t = dm.tables.find(random_name)

                path = t._get_data_file(Path('.') / random_name)
                data = pd.read_parquet(path)
            except Exception as e:
                logging.error(f'determining what tables have been loaded failed with: {e}')
            finally:
                # Deleting temporary objects
                dm.delete()
                transf.statement = f'DROP TABLE IF EXISTS "{random_name}";'
                transf.execute()
                dj.delete()
                if sys.version_info > (3, 8):
                    path.unlink(missing_ok=True)
                else:
                    path.unlink()
            loaded_tables = pd.Series(data['table_name']).tolist()
            try:
                loaded_tables.remove(random_name)
            except:
                pass
            logging.info(f'these tables are already in the data pool: {loaded_tables}')
            break
        except:
            loaded_tables = []
        logging.warning(f'determine_tables_loaded failed for {counter}. time. Retrying.')
        time.sleep(1)
    return loaded_tables


def determine_line_count_of_loaded_tables(ibc_team):
    # Create new table with the subset we're interested in
    data = None
    celonis = get_celonis(key_type="APP_KEY")
    logging.info('counting lines of tables that have been loaded.')

    random_name = f'zzz___TEMP_LC___{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}'
    add_line_counts_statement = []
    add_line_counts_statement.append(
        f'CREATE TABLE IF NOT EXISTS "{random_name}" ("TABLE" VARCHAR(80), "COUNT" INTEGER);\n')

    tables = determine_tables_loaded(ibc_team)

    if ibc_team.connectionid is None:
        for t in tables:
            add_line_counts_statement.append(
                f'INSERT INTO "{random_name}" ("TABLE" ,"COUNT") SELECT \'{t}\', COUNT(1) FROM "{t}";\n')
    else:
        for t in tables:
            add_line_counts_statement.append(
                f'INSERT INTO "{random_name}" ("TABLE" ,"COUNT") SELECT \'{t}\', COUNT(1) FROM <%=DATASOURCE:JDBC%>."{t}";\n')
    add_line_counts_statement = ''.join(add_line_counts_statement)

    # table_name FROM tables where table_schema = \'\'
    # Create data job and run table creation script
    p = celonis.pools.find(ibc_team.poolid)
    counter = 0
    while counter < 4:
        counter += 1
        try:
            dj = p.create_data_job(random_name)
            transf = dj.create_transformation(random_name, add_line_counts_statement)
            transf.execute(wait_for_execution=True)
            # Create temporary data model in pool and add recently created table, then reload
            dm = p.create_datamodel(random_name)
            try:
                dm.add_tables_from_pool(random_name)
                dm.reload(from_cache=False, wait_for_reload=True)
                time.sleep(3)

                # Find table object in data model and download
                t = dm.tables.find(random_name)

                path = t._get_data_file(Path('.') / random_name)
                data = pd.read_parquet(path)
            except Exception as e:
                logging.error(f'determining line count per table failed with: {e}')
            finally:
                # Deleting temporary objects
                dm.delete()
                transf.statement = f'DROP TABLE IF EXISTS "{random_name}";'
                transf.execute()
                dj.delete()
                if sys.version_info > (3, 8):
                    path.unlink(missing_ok=True)
                else:
                    path.unlink()
            # logging.info(f'these tables are already in the data pool: {loaded_tables}')
            break
        except:
            loaded_tables = []
        logging.warning(f'determine_tables_loaded failed for {counter}. time. Retrying.')
        time.sleep(1)
    return data
    # data.to_excel('lines.xlsx')


def remove_file_endings(filename):
    endings = generic_file_type + compressed
    endings.extend(list(map(lambda x: x.upper(), endings)))
    for ending in endings:
        filename = filename.replace(ending, '')
    return filename


def clean_table_name(name):
    name = remove_file_endings(name)
    name = name.replace('.filepart', '')
    name = name.replace('.', '/')
    return re.sub('[^A-Za-z0-9_/]', '_', name)


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
        df = '19700101' + df
        return dt(df)
    except Exception as e:
        print(f'casting {df} to time failed with {e}')


def dt(df):
    try:
        df = pd.to_datetime(df, errors='coerce')
        return df.astype('datetime64')
    except Exception as e:
        print(f'casting {df} to datetime failed with {e}')


def flt(df):
    try:
        mask = df.str.contains('-')
        df = df.str.replace('-', '').str.strip()
        df = pd.to_numeric(df, downcast='float', errors='coerce')
        # df = df.astype(float)
        return df.mask(mask, -df)
    except Exception as e:
        logging.error(f'casting {df} to float failed with {e}')


def inti(df):
    try:
        mask = df.str.contains('-')
        df = df.str.replace('-', '').str.strip()
        df = pd.to_numeric(df, downcast='signed', errors='coerce')
        # df = df.str.replace('-', '').str.strip().astype('int64')
        # df = df.astype(int)
        return df.mask(mask, -df)
    except Exception as e:
        logging.error(f'casting {df} to int failed with {e}')


def type_determination(x):
    if x in ['CURR', 'QUAN', 'DEC', 'FLTP']:
        return 'float'
    elif x in ['INT1', 'INT2', 'INT4', 'PREC']:
        return 'int'
    elif x in ['DATS']:
        return 'date'
    elif x in ['TIMS']:
        return 'time'
    else:
        return 'str'


def test_member_part_of_object(obj: str, members: list) -> list:
    found_members = list()
    if isinstance(obj, str) is True:
        for member in members:
            if str(member) in obj:
                found_members.append(member)
    else:
        for member in members:
            if member in obj:
                found_members.append(member)
    return found_members


def import_sap_header(header, files, jobstatus, uppie, data, location_indicator='local', delta=False, pwd=None):
    encoding = 'utf-8'
    """
    team: {self.team}
                    realm: {self.realm}
                    poolid: {self.poolid}
                    connectionid: {self.connectionid}
                    appkey: {self.appkey}
                    apikey: {self.apikey}
                    url: {self.url}
    """
    logging.debug("header file: %s passed to import_sap_header", header.file)
    if location_indicator == 'local':
        ref = header.file.split('HEADER')
        ref[1] = ref[1].replace('.csv', '')
    elif location_indicator == 'global':
        ref = Path(header.file).name.split('HEADER')
        ref[1] = ref[1].replace('.csv', '')
    else:
        logging.error(f'location indicator {location_indicator} is invalid')
        raise ValueError(f'location indicator {location_indicator} is invalid')
    targetname = Path(ref[0][:-1]).name  # .replace(path_to_file, '')
    targetname = clean_table_name(targetname)
    if targetname in data:
        logging.warning(f'skipping {header.file} as table with {targetname} is already present in target pool.')
    else:
        jobhandle = uppie.create_job(pool_id=header.poolid,
                                     data_connection_id=header.connectionid,
                                     targetName=targetname,
                                     upsert=delta)
        logging.info(f'starting to upload {targetname}')
        logging.debug(jobhandle)
        jobstatus[jobhandle['id']] = False
        url = f'https://{header.team}.{header.realm}.celonis.cloud/storage-manager/api/buckets/{header.bucket_id}/files?path=/' + header.file
        header_json = {'Authorization': 'AppKey {}'.format(header.appkey), 'Accept': 'application/octet-stream'}
        with requests.get(url, headers=header_json, stream=False) as r:
            r.raise_for_status()
            df = pd.read_csv(BytesIO(r.content), header=None, dtype=str, sep=' ',
                             names=['names', 'type', 'length', 'declength'], encoding=encoding)

        df['type'] = df['type'].apply(lambda x: type_determination(x))
        type_dict = {}
        if len(df[df['type'] == 'float']) > 0:
            type_dict['float'] = list(df[df['type'] == 'float']['names'])
        if len(df[df['type'] == 'int']) > 0:
            type_dict['int'] = list(df[df['type'] == 'int']['names'])
        if len(df[df['type'] == 'date']) > 0:
            type_dict['date'] = list(df[df['type'] == 'date']['names'])
        if len(df[df['type'] == 'time']) > 0:
            type_dict['time'] = list(df[df['type'] == 'time']['names'])
        relevant_files = (f for f in files if ref[0] in f.file and ref[1] in f.file)
        # logging.info(f'{ref[0]}, {ref[1]}, {relevant_files}')

        header_json = {'Authorization': 'AppKey {}'.format(header.appkey), 'Accept': 'application/octet-stream'}
        pre_url = f'https://{header.team}.{header.realm}.celonis.cloud/storage-manager/api/buckets/{header.bucket_id}/files?path=/'
        with get_context("forkserver").Pool(5) as pool:
            logging.info('jumping into the pool')
            parallel_jobs = pool.imap_unordered(sap_load, zip(relevant_files, repeat(pre_url), repeat(header_json),
                                                              repeat(header), repeat(jobhandle['id']), repeat(df),
                                                              repeat(type_dict), repeat(uppie)))
            for parallel_job in parallel_jobs:
                pass
        logging.info('upload done, submitting job...')
        uppie.submit_job(pool_id=header.poolid, job_id=jobhandle['id'])
        data.append(targetname)
    return data


def test_float(strg: str) -> bool:
    try:
        float(strg)
        return True
    except ValueError:
        return False


def detect_encoding(non_sap_file, pwd):
    logging.info(f'starting encoding determination')
    detector = UniversalDetector()
    detector.reset()
    # counter = 0
    try:
        """
        with open(file, 'rb') as file_detect:
        """
        url = f'https://{non_sap_file.team}.{non_sap_file.realm}.celonis.cloud/storage-manager/api/buckets/{non_sap_file.bucket_id}/files?path=/' + non_sap_file.file
        header_json = {'Authorization': 'AppKey {}'.format(non_sap_file.appkey), 'Accept': 'application/octet-stream'}
        with requests.get(url, headers=header_json, stream=True) as file_detect:
            file_detect.raise_for_status()
            if ".csv" in non_sap_file.file and ".gz" not in non_sap_file.file:
                logging.info("starting csv detection")
                counter = 0
                for chunk in file_detect.iter_content(chunk_size=1024 * 1024 * 400, decode_unicode=False):
                    lines = chunk.split()[1:-1]
                    logging.info(f"{str(counter)}, {len(lines)}")
                    for line in lines:
                        detector.feed(line)
                        counter += 1
                        if counter % 1000 == 0:
                            logging.info(str(counter))
                        if detector.done or counter >= 50000:
                            break
                    if detector.done or counter >= 50000:
                        break
                logging.info("csv detection done")
            else:
                logging.info("starting other detection done")
                if non_sap_file.file.split('.')[-1] == 'zip':
                    z = zipfile.ZipFile(BytesIO(file_detect.content))
                    zip_content = z.infolist()
                    idx = 0
                    for c, zips in enumerate(zip_content):
                        if '.csv' in zips.filename:
                            idx = c
                            break
                    logging.debug(zip_content[idx])
                    fh = z.open(zip_content[idx])
                elif non_sap_file.file.split('.')[-1] == '7z':
                    z = py7zr.SevenZipFile(BytesIO(file_detect.content), password=pwd)
                    filename = z.getnames()[0]
                    fh = z.read(filename)[filename]
                    logging.debug(fh)
                elif non_sap_file.file.split('.')[-2] == 'tar':
                    fh = tarfile.open(fileobj=BytesIO(file_detect.content), mode='r:gz')
                elif non_sap_file.file.split('.')[-1] == 'gz':
                    fh = gzip.GzipFile(fileobj=BytesIO(file_detect.content), mode='rb')
                else:
                    raise NotImplementedError(f"decoding for file type: {non_sap_file.file} not implemented")
                for counter, line in enumerate(fh):
                    # counter += 1
                    detector.feed(line)
                    if detector.done:
                        break
                    elif counter > 50000:
                        break
                logging.info("other detection done")
        detector.close()
        enc = detector.result['encoding'].lower()
        logging.info(f'{non_sap_file.file} has encoding: {detector.result}')
    except Exception as e:
        logging.error(f'encoding detection failed with: {e}\nreverting to utf-8 as standard')
        enc = 'utf-8'
    finally:
        detector.reset()
    return enc


def get_url_and_headers(file):
    url = f'https://{file.team}.{file.realm}.celonis.cloud/storage-manager/api/buckets/{file.bucket_id}/files?path=/' + file.file
    header = {'Authorization': f'AppKey {file.appkey}', 'Accept': 'application/octet-stream'}
    return url, header


def download_and_unzip_file(file, password=None):
    downloaded_file = BytesIO(download_file(file))
    if '.zip' in file.file:
        z = zipfile.ZipFile(downloaded_file)
        zip_content = z.infolist()
        idx = 0
        for c, zips in enumerate(zip_content):
            if '.csv' in zips.filename:
                idx = c
                break
        logging.debug(zip_content[idx])
        fh = z.open(zip_content[idx])
    elif '.7z' in file.file:
        z = py7zr.SevenZipFile(downloaded_file, password=password)
        filename = z.getnames()[0]
        fh = z.read(filename)[filename]
        logging.debug(fh)
    elif '.tar' in file.file:
        fh = tarfile.open(fileobj=downloaded_file, mode='r:gz')
    elif '.gz' in file.file:
        fh = gzip.GzipFile(fileobj=downloaded_file, mode='rb')
    else:
        raise IOError("%s is not of a recognized compression format", file)
    return fh


def download_file(file):
    url, headers = get_url_and_headers(file)
    with requests.get(url, headers=headers, stream=True) as r:
        return r.content


def download_chunks(file, chunksize=None):
    url, headers = get_url_and_headers(file)
    if chunksize is None:
        chunksize = 100
    with requests.get(url, headers=headers, stream=True) as r:
        for chunk in r.iter_content(chunk_size=1024 * 1024 * chunksize, decode_unicode=False):
            yield chunk


def determine_dialect(non_sap_file, enc):
    logging.info('starting dialect determination')
    sniffer = csv.Sniffer()
    sniffer.preferred = [';', ',', '\t', '|', '~', ' ']
    dialect = ''
    data = []
    counter = 0
    try:
        url = f'https://{non_sap_file.team}.{non_sap_file.realm}.celonis.cloud/storage-manager/api/buckets/{non_sap_file.bucket_id}/files?path=/' + non_sap_file.file
        header_json = {'Authorization': 'AppKey {}'.format(non_sap_file.appkey), 'Accept': 'application/octet-stream'}
        with requests.get(url, headers=header_json, stream=True) as file_detect:
            file_detect.raise_for_status()
            if non_sap_file.file.split('.')[-1] == 'zip':
                z = zipfile.ZipFile(BytesIO(file_detect.content))
                zip_content = z.infolist()
                idx = 0
                for c, zips in enumerate(zip_content):
                    if '.csv' in zips.filename:
                        idx = c
                        break
                logging.debug(zip_content[idx])
                fh = z.open(zip_content[idx])
            elif non_sap_file.file.split('.')[-1] == '7z':
                z = py7zr.SevenZipFile(BytesIO(file_detect.content), password=pwd)
                filename = z.getnames()[0]
                fh = z.read(filename)[filename]
                logging.debug(fh)
            elif non_sap_file.file.split('.')[-2] == 'tar':
                fh = tarfile.open(fileobj=BytesIO(file_detect.content), mode='r:gz')
            elif non_sap_file.file.split('.')[-1] == 'gz':
                fh = gzip.GzipFile(fileobj=BytesIO(file_detect.content), mode='rb')
            else:
                for chunk in file_detect.iter_content(chunk_size=1024 * 1024 * 400, decode_unicode=False):
                    fh = BytesIO(chunk)
                    logging.info("chunk read")
                    break
                logging.info("left chunking")
            for counter, line in enumerate(fh):
                data.append(line.decode(enc))
                counter += 1
                if counter == 10:
                    break
    except Exception as e:
        logging.error(e)
    try:
        data_str = ''.join(data)
        dialect = sniffer.sniff(data_str)
        delimiter = dialect.delimiter
        quotechar = dialect.quotechar
        escapechar = dialect.escapechar
        header = sniffer.has_header(data_str)
    except:
        logging.warning('''sniffer was unsuccessful, using a simplistic approach
                        to determine the delimiter and existence of header.''')
        line1 = list_get(data, 0)
        delim = dict()
        for i in [';', ',', '\t', '|']:
            delim[i] = len(line1.split(i))
        delimiter = sorted(delim.items(), key=lambda kv: kv[1])[-1][0]
        quotechar = None
        escapechar = None
        if any(map(test_float, line1.split(delimiter))):
            header = False
        else:
            header = True
    if header is True:
        header = 0
    else:
        header = None
    logging.info(f'''delimiter: {delimiter}, quotechar: {quotechar},
                     escapechar: {escapechar}, header: {header}''')
    return {'delimiter': delimiter, 'quotechar': quotechar,
            'escapechar': escapechar, 'header': header}


def list_to_str(lst):
    return_list = list()
    for i in lst:
        return_list.append(str(i))
    return return_list


def remove_superfluous_spaces(lines, delimiter):
    lines = re.sub(" {2,}", " ", lines)
    lines = lines.replace(f"{delimiter} ", delimiter)
    lines = lines.replace(f" {delimiter}", delimiter)
    return lines


def list_get(lst, index):
    if len(lst) < index + 1:
        return ''
    else:
        return lst[index]


def determine_column_names(header, delimiter, quotechar):
    if quotechar is not None:
        header = header.replace(quotechar, "")
    header = header.split(delimiter)
    logging.info(f"number of columns: {len(header)}")
    names = []
    for i in header:
        counter = 0
        b = deepcopy(i)
        while b in names:
            b = deepcopy(i) + str(counter)
            counter += 1
        names.append(b.strip())
    logging.debug(names)
    return names


def process_non_sap_chunk(chunk, encoding, delimiter, first_line_visited, buffer, names, quotechar):
    lines = chunk.decode(encoding)
    lines = remove_superfluous_spaces(lines, delimiter)
    lines = lines.split("\n")
    logging.info("chunk cleaned")
    if first_line_visited is True:
        lines[0] = buffer + lines[0]
    else:
        header = lines.pop(0)
        names = determine_column_names(header, delimiter, quotechar)
        first_line_visited = True
    buffer = lines[-1]
    lines = "\n".join(lines[:-1])
    lines = BytesIO(lines.encode())
    return lines, first_line_visited, buffer, names


def delete_file_from_sftp(file):
    url = f'https://{file.team}.{file.realm}.celonis.cloud/storage-manager/api/buckets/{file.bucket_id}/files?path=/' + file.file
    header_json = {'Authorization': f'AppKey {file.appkey}', 'Accept': 'application/json'}
    r = requests.delete(url, headers=header_json)
    logging.warning(f'deletion status: {r.status_code}')


def import_non_sap_file(non_sap_file,
                        jobstatus,
                        uppie,
                        data,
                        delta,
                        as_string,
                        config,
                        pwd=None,
                        ):
    no_quoting = config["no_quoting"]
    encoding_list = config["encoding_list"]
    target_name = Path(non_sap_file.file).name
    target_name = clean_table_name(target_name)
    if target_name in data:
        logging.warning(f'skipping {non_sap_file.file} as table with {target_name} is already present in target pool.')
    else:
        job_handle = uppie.create_job(pool_id=non_sap_file.poolid,
                                      data_connection_id=non_sap_file.connectionid,
                                      targetName=target_name,
                                      upsert=delta)
        logging.debug(job_handle)
        try:
            jobstatus[job_handle['id']] = False
        except KeyError:
            logging.error(f'failed to get job id for file {non_sap_file} with job: {job_handle}')
            return data, None
        logging.info(f'starting to upload {target_name}')
        try:
            if len(test_member_part_of_object(non_sap_file.file, ["zip", "7z", "gz", "tar"])) > 0:
                fh = download_and_unzip_file(non_sap_file, pwd)
            else:
                fh = None  # BytesIO(r.content)
            if non_sap_file.file_type == '.parquet':
                tmp_parquet_file = Path('/home/jovyan/tmp.parquet')
                with open(tmp_parquet_file, 'wb') as out:
                    out.write(fh.getvalue())
                df = parquet_utils.read_parquet(tmp_parquet_file)
                tmp_parquet_file.unlink()
                uppie.push_new_chunk(pool_id=non_sap_file.poolid,
                                     job_id=job_handle['id'],
                                     dataframe=df,
                                     )
            elif non_sap_file.file_type == '.csv':
                if encoding_list[0] is None:
                    encoding = detect_encoding(non_sap_file, pwd)
                    if encoding == "ascii":
                        encoding = "utf-8"
                else:
                    encoding = encoding_list[0]
                dialect = determine_dialect(non_sap_file, encoding)
                delimiter = dialect['delimiter']
                quotechar = dialect['quotechar']
                escapechar = dialect['escapechar']
                if escapechar is None:
                    escapechar = '\\'
                header = dialect['header']
                if fh is not None:
                    logging.info("block way")
                    pd_config = {
                        'filepath_or_buffer': fh,
                        'encoding': encoding,
                        'sep': delimiter,
                        'parse_dates': False,
                        'on_bad_lines': 'warn',
                        'escapechar': escapechar,
                        # IDEA: Change chunksize to be dependend on # of cols (info can be taken from dialect determination)
                        'chunksize': 200000,
                        'engine': 'python',
                        'keep_default_na': False,
                        'header': header
                    }
                    if as_string is True:
                        pd_config['dtype'] = str
                    if no_quoting is True:
                        pd_config['quoting'] = 3
                    df_up = pd.read_csv(**pd_config)
                    for i in df_up:
                        logging.debug(i.head())
                        i.columns = list_to_str(list(i.columns))

                        uppie.push_new_chunk(pool_id=non_sap_file.poolid,
                                             job_id=job_handle['id'],
                                             dataframe=i,
                                             )
                else:
                    logging.info("chunked way")
                    buffer = ""
                    first_line_visited = False
                    names = None
                    pd_config = {
                        'encoding': encoding,
                        'sep': delimiter,
                        'parse_dates': False,
                        'on_bad_lines': 'warn',
                        'escapechar': escapechar,
                        'engine': 'python',
                        'keep_default_na': False,
                        'header': None,
                    }
                    if as_string is True:
                        logging.debug("setting all columns to string")
                        pd_config['dtype'] = str
                    if no_quoting is True:
                        pd_config['quoting'] = 3
                    else:
                        pd_config['quotechar'] = quotechar
                    for c, chunk in enumerate(download_chunks(file=non_sap_file, chunksize=400)):
                        logging.debug(f"processing chunk number: {c + 1}")
                        lines, first_line_visited, buffer, names = process_non_sap_chunk(chunk=chunk, encoding=encoding,
                                                                                         delimiter=delimiter,
                                                                                         first_line_visited=first_line_visited,
                                                                                         buffer=buffer, names=names,
                                                                                         quotechar=pd_config.get(
                                                                                             'quotechar', None))
                        pd_config['names'] = names
                        pd_config['filepath_or_buffer'] = lines
                        logging.info("reading csv")
                        df_up = pd.read_csv(**pd_config)
                        logging.debug(df_up.head())
                        df_up.columns = list_to_str(list(df_up.columns))
                        logging.debug(df_up.columns)
                        logging.debug(df_up.dtypes)
                        logging.info("pushing chunk")
                        uppie.push_new_chunk(pool_id=non_sap_file.poolid,
                                             job_id=job_handle['id'],
                                             dataframe=df_up,
                                             )
                        logging.info("chunk done")
            else:
                matches = {}
                pd_config = {
                    'io': fh,
                    'sheet_name': None,
                    'keep_default_na': False,
                }
                if as_string is True:
                    pd_config['dtype'] = str
                df = pd.read_excel(**pd_config)
                for a, b in product(df, df):
                    col_a = df[a].columns
                    col_b = df[b].columns
                    if (len(col_a) == len(col_b)
                            and (len([i for i, j in zip(col_a, col_b) if i == j])
                                 == len(col_b))):
                        matches[str(col_b)] = (matches.get(str(col_b), [a, b])
                                               + [a, b])
                for i in matches:
                    matches[i] = set(matches[i])
                if (len(matches) == 1
                        and len(df) == 1
                        and len(copy.deepcopy(matches).popitem()[1]) == len(df)):
                    for i in df:
                        df = df[i]
                elif (len(matches) == 1
                      and len(copy.deepcopy(matches).popitem()[1]) == len(df)):
                    dfs = []
                    for i in df:
                        dfs.append(df[i])
                    df = pd.concat(dfs, ignore_index=True)
                logging.debug(df.head())
                uppie.push_new_chunk(pool_id=non_sap_file.poolid,
                                     job_id=job_handle['id'],
                                     dataframe=df,
                                     )
            uppie.submit_job(pool_id=non_sap_file.poolid, job_id=job_handle['id'])
            logging.info(f'finished uploading {non_sap_file.file}')
            data.append(target_name)
            config["error"] = None
        except EmptyDataError:
            logging.warning(f'{non_sap_file} is empty and will be skipped')
            del jobstatus[job_handle['id']]
            config["error"] = EmptyDataError
        except Exception as e:
            logging.error(f'{non_sap_file} failed with error: {e}')
            logging.exception(''.join(tb.format_exception(None, e, e.__traceback__)))
            time.sleep(2)
            config["error"] = e
            if f"' expected after" in config["error"] and no_quoting is False:
                del jobstatus[job_handle['id']]
                logging.info(f'retrying {header} without quoting')
                config["no_quoting"] = no_quoting
            elif len(encoding_list) > 1 and "can't decode" in str(error):
                encoding_list.pop(0)
                del jobstatus[job_handle['id']]
                config["encoding_list"] = encoding_list
            else:
                config["error"] = None
    return data, config


def ibc_files_to_json(lst, name):
    temp = []
    for i in lst:
        temp.append(i.to_dict())
    with open(name, 'w') as out:
        out.write(json.dumps(temp, indent=4))


def json_to_ibc_files(jsn, url):
    with open(jsn, 'r') as inp:
        text = inp.read()
    dct_lst = json.loads(text)
    ibc_files = []
    for entry in dct_lst:
        entry_dct = entry
        entry_dct['url'] = url
        ibc_files.append(ibc_file(**entry_dct))
    return ibc_files


def check_permissions(c: pycelonis.celonis_api.celonis.Celonis):
    missing = list()
    for p in c.permissions:
        if p["serviceName"] == "storage-manager":
            for perm in ["GET", "LIST"]:
                if perm not in p["permissions"]:
                    missing.append(f"File Storage Manager is missing {perm}")
        elif p["serviceName"] == "event-collection":
            for perm in ["EDIT_ALL_DATA_POOLS"]:
                if perm not in p["permissions"]:
                    missing.append(f"Data Integration is missing {perm}")
        elif p["serviceName"] == "ml-workbench":
            for perm in ["EDIT_SCHEDULERS", "USE_ALL_SCHEDULERS", "CREATE_SCHEDULERS", "VIEW_CONFIGURATION"]:
                if perm not in p["permissions"]:
                    missing.append(f"Machine Learning is missing {perm}")
    if len(missing) > 0:
        e = ", ".join(missing)
        raise PermissionError(1, "The following permissions are missing", e)


def main():
    try:
        check_permissions(get_celonis(key_type="APP_KEY"))
    except Exception as e:
        logging.error("Permissions check failed with: %s", e)
        raise e

    c = ibc_team(url)

    if exclude_loaded is True:
        data = determine_tables_loaded(c)
    else:
        data = []
    # data.extend(["BSEG", "EKBE", "EKET", "MARC", "MKPF"])
    data.extend(["MKPF"])
    if continue_from_last_time is True and Path('./head.json').is_file():
        logging.info('getting ibc_files from ML Workbench')
        if Path('./body.json').is_file():
            body = json_to_ibc_files('body.json', url)
        else:
            body = []
        head = json_to_ibc_files('head.json', url)
    else:
        logging.info('getting ibc_files from SFTP')
        head, body = [], []
        buckets = c.find_buckets()
        for b in buckets:
            logging.info(f'started finding folders at: {datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}')
            f = b.find_folders(path_to_folder)
            logging.info(f'finished finding folders at: {datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}')
            try:
                logging.info(f'started classifying files at: {datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}')
                for i in f:
                    head_instance, body_instance = i.classify_files()
                    head.extend(head_instance)
                    body.extend(body_instance)
                logging.info(f'finished classifying files at: {datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}')
            except Exception as e:
                logging.error(f'encounterd error {e} while processing {i} in {f}')
            ibc_files_to_json(head, 'head.json')
            ibc_files_to_json(body, 'body.json')

    logging.info(
        f'finished classifying files. {len(head)} header and {len(body)} sub files were found.')  # {lenght_before - len(head)} files were skipped.')

    if look_for_sap_files_globally is True:
        location_indicator = 'global'
    else:
        location_indicator = 'local'
    jobstatus = {}
    uppie = cloud(tenant=c.team, realm=c.realm, api_key=c.apikey)
    for header in head:
        if header.file_type == 'sap':
            data = import_sap_header(header=header, files=body, jobstatus=jobstatus, uppie=uppie, data=data,
                                     location_indicator=location_indicator, delta=delta, )
        else:
            non_sap_config = {"error": "tmp", "encoding_list": [None, "utf-8", "ascii", "cp1252", "latin_1", "iso-8859-1", ], "no_quoting": False, }
            while non_sap_config["error"] is not None:
                data, non_sap_config["error"] = import_non_sap_file(non_sap_file=header, jobstatus=jobstatus, uppie=uppie, data=data, delta=delta, as_string=as_string, config=non_sap_config)
    logging.info('upload done.')
    error_flag = False
    failed_tables = []
    running = True
    while running:
        jobs = uppie.list_jobs(c.poolid)
        for jobids in jobstatus:
            for i in jobs:
                try:
                    if i['id'] == jobids:
                        if i['status'] == 'QUEUED':
                            pass
                        elif jobstatus[jobids] is True:
                            pass
                        elif i['status'] == 'DONE':
                            jobstatus[jobids] = True
                        elif i['status'] != 'RUNNING':
                            jobstatus[jobids] = True
                        else:
                            pass
                        break
                except (KeyboardInterrupt, SystemExit):
                    logging.error('terminating program\n')
                    quit()
                except:
                    pass
        if all(status is True for status in jobstatus.values()):
            running = False
            for i in jobs:
                if i['id'] in jobstatus:
                    if i['status'] == 'DONE':
                        logging.info(f"{i['targetName']} was successfully installed in the database")
                    else:
                        error_flag = True
                        failed_tables.append(i['targetName'])
                        logging.error(f"{i['targetName']} failed with: {i}")
        else:
            time.sleep(15)
    if error_flag is True:
        raise ValueError(f'the loading of the following tables failed: {failed_tables}')
    logging.info('all done.')


if __name__ == '__main__':
    main()
