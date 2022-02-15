import requests
import re
from pathlib import Path
from EMS_cloud_module import cloud
import subprocess
import logging
import os

os.environ['NUMEXPR_MAX_THREADS'] = '16'
log = logging.getLogger(__name__)


global compressed
compressed = ['.tar', '.gz', '.zip', '.7z']

global generic_file_type
generic_file_type = ['.csv', '.xlsx', '.xls', '.parquet']

global sap_file_type
sap_file_type = '(.*)_[0-9]{8}_[0-9]{6}.'

global encrypted
encrypted = ['.gpg', '.pgp']


class ibc_team():
    def parse_url(self, url):
        parts = []
        connectionflag = 1
        try:
            parts.append(re.search('https://([a-z0-9-]+)\.', url).groups()[0])
            parts.append(re.search('\.([a-z0-9-]+)\.celonis', url).groups()[0])
            parts.append(re.search('ui/pools/([a-z0-9-]+)', url).groups()[0])
            try:
                parts.append(re.search('data-connections/[a-z-]+/([a-z0-9-]+)', url)
                             .groups()[0])
            except AttributeError:
                connectionflag = 0
        except AttributeError:
            log.error(f'{url} this is an unvalid url.')
        log.debug(f'url has the following parts: {parts} and connectionflag: {connectionflag}')
        return parts, connectionflag
    
    def determine_appkey(self, cmd='printenv | grep CELONIS_API_TOKEN'):
        appkey = subprocess.run(cmd, shell=True, capture_output=True)
        return appkey.stdout.decode('utf-8').split('=')[1].strip()
    
    def __str__(self):
        return f'''team: {self.team}
                    realm: {self.realm}
                    poolid: {self.poolid}
                    connectionid: {self.connectionid}
                    appkey: {self.appkey}
                    apikey: {self.apikey}
                    url: {self.url}'''.replace('                    ', '')
    
    def __repr__(self):
        return f'ibc-team {self.team} in {self.realm}.'
    
    def __init__(self, url):
        parts, connectionflag = self.parse_url(url)
        self.url = url
        self.team = parts[0]
        self.realm = parts[1]
        self.poolid = parts[2]
        if connectionflag == 1:
            self.connectionid = parts[3]
        else:
            self.connectionid = None
        self.appkey = self.determine_appkey()
        self.apikey = self.appkey

    def get_values(self):
        return {'team': self.team,
                'realm': self.realm,
                'poolid': self.poolid,
                'connectionid': self.connectionid,
                'appkey': self.appkey,
                'apikey': self.apikey,
                'url': self.url,
               }

    def find_buckets(self, name=None, id=None):
        url = f'https://{self.team}.{self.realm}.celonis.cloud/storage-manager/api/buckets?feature=SFTP'
        header_json = {'Authorization': f'AppKey {self.appkey}', 'Accept': 'application/json'}
        file_response = requests.get(url, headers=header_json)
        log.debug(str(file_response.json()))
        if name is None and id is None:
            return [bucket(self.url, i['id'], i['name']) for i in file_response.json()]
        else:
            return [bucket(self.url, i['id'], i['name']) for i in file_response.json() if i['id'] == id]
        
    def validate_bucket(self, name=None, id=None):
        if name is None and id is None:
            raise ValueError("either name or bucket id need to be specified.")
        url = f'https://{self.team}.{self.realm}.celonis.cloud/storage-manager/api/buckets?feature=SFTP'
        header_json = {'Authorization': f'AppKey {self.appkey}', 'Accept': 'application/json'}
        file_response = requests.get(url, headers=header_json)
        log.debug(str(file_response.json()))
        result = [{'bucket_id': i['id'], 'bucket_name': i['name']} for i in file_response.json() if (i['id'] == id or i['name']==name)]
        if len(result) == 1:
            pass
        elif len(result) == 0:
            raise NameError('invalid bucket id.')
        else:
            raise ValueError('provided bucket identification is not unique.')

class bucket(ibc_team):
    def __init__(self, url, bucket_id, bucket_name=None):
        super().__init__(url)
        super().validate_bucket(id=bucket_id)
        self.bucket_id = bucket_id
        self.bucket_name = bucket_name
    
    def __str__(self):
        return f'{super().__str__()}\nbucket_id: {self.bucket_id}'
    
    def __repr__(self):
        return f'bucket {self.bucket_id} in {super().__repr__()}'
    
    def find_folders(self, path_to_folder=''):
        url = f'https://{self.team}.{self.realm}.celonis.cloud/storage-manager/api/buckets/{self.bucket_id}/files?path=/' + path_to_folder
        header_json = {'Authorization': f'AppKey {self.appkey}', 'Accept': 'application/json'}
        file_response = requests.get(url, headers=header_json)
        files = []
        folders = []
        return_folders = []
        try:
            log.debug(file_response.json()['children'])
            for i in file_response.json()['children']:
                if i['type'] == 'FILE':
                    files.append({'size': i['size'], 'file': (path_to_folder + i['filename'])})
                elif i['type'] == 'DIRECTORY':
                    folders.append(path_to_folder + i['filename'] + '/')
            log.debug(f'{files}\n\n{folders}\n')
            if len(folders) > 0:
                for f in folders:
                    return_folders.extend(self.find_folders(f))
            if len(files) > 0:
                return_folders.append(folder(self.url, self.bucket_id, path_to_folder, files))
            return return_folders
        except Exception as e:
            log.error(f'{e}\n{file_response}')

class folder(bucket):
    def __init__(self, url, bucket_id, folder, files):
        super().__init__(url, bucket_id)
        self.folder = folder
        self.files = files
    
    def __str__(self):
        return f'{super().__str__()}\nfolder: {self.folder}'
    
    def __repr__(self):
        return f'{self.folder} in {self.bucket_id}'
    
    def classify_files(self):
        log.debug(f'{self.files[0]} is the first file from {len(self.files)} to be classified.')
        head, body = [], []
        for file in self.files:
            file_characteristics = {
                'url': self.url,
                'bucket_id': self.bucket_id,
                'folder': self.folder,
                'file': file['file'],
                'size': file['size'],
                'file_type': None,
                'header': False,
                'encryption': None,
                'compression': None,
            }
            p = Path(file['file'])
            log.debug(f'file {p} has suffixes {p.suffixes}')
            
            if len(re.findall(sap_file_type, p.name)):
                file_characteristics['file_type'] = 'sap'
                if '_HEADER_' in p.name:
                    file_characteristics['header'] = True
            else:
                for s in p.suffixes:
                    if s.lower() in generic_file_type:
                        file_characteristics['file_type'] = s.lower()
                    elif s.lower() in compressed:
                        if file_characteristics['compression'] is None:
                            file_characteristics['compression'] = s.lower()
                        else:
                            file_characteristics['compression'] += s.lower()
                    elif s.lower() in encrypted:
                        if file_characteristics['encryption'] is None:
                            file_characteristics['encryption'] = s.lower()
                        else:
                            file_characteristics['encryption'] += s.lower()
            log.debug(f'file {file["file"]} has the following traits: {file_characteristics}')
            if file_characteristics['file_type'] is None and file_characteristics['compression'] is None:
                log.warning(f'{file["file"]} with traits: {file_characteristics} is of wrong file type.')
            elif file_characteristics['file_type'] != 'sap' or file_characteristics['header'] is True:
                file_tmp = ibc_file(**file_characteristics)
                log.debug(file_tmp)
                head.append(file_tmp)
            else:
                file_tmp = ibc_file(**file_characteristics)
                log.debug(file_tmp)
                body.append(file_tmp)
        return head, body

class ibc_file(folder):
    # ABAP, Header, csv, gz, zip, 7z, pgp, gpg
    def __init__(self, url, bucket_id, folder, file, file_type, encryption, header, compression, size=None, files=None):
        super().__init__(url, bucket_id, folder, files)
        self.file = file
        self.file_type = file_type
        self.encryption = encryption
        self.header = header
        self.compression = compression
        self.bucket_id = bucket_id
        self.folder = folder
        self.size = size

    def __str__(self):
        return f'{super().__str__()}\nfile: {self.file}'
    
    def __repr__(self):
        return f'{self.file} of {self.file_type} with header being {self.header}'
    
    def to_dict(self):
        return {'file': self.file,
                'file_type': self.file_type,
                'encryption': self.encryption,
                'header': self.header,
                'compression': self.compression,
                'bucket_id': self.bucket_id,
                'folder': self.folder,
                'size': self.size,
               }
