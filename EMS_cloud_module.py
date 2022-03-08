import requests
from pathlib import Path
from datetime import datetime
from pycelonis.utils import parquet_utils
import logging
import os

os.environ['NUMEXPR_MAX_THREADS'] = '16'
log = logging.getLogger(__name__)

class cloud:

    def get_api(self, path):
        return f"https://{self.tenant}.{self.realm}.celonis.cloud/{path}"

    def __init__(self, tenant, realm, api_key):
        self.tenant = tenant
        self.realm = realm
        self.api_key = api_key

    def get_jobs_api(self, pool_id):
        return self.get_api(f"integration/api/v1/data-push/{pool_id}/jobs/")

    def get_auth(self):
        return {'authorization': f"AppKey {self.api_key}"}

    def list_jobs(self, pool_id):
        api = self.get_jobs_api(pool_id)
        return requests.get(api, headers=self.get_auth()).json()

    def delete_job(self, pool_id, job_id):
        api = self.get_jobs_api(pool_id) + f"/{job_id}"
        return requests.delete(api, headers=self.get_auth())

    def create_job(self, pool_id, targetName, data_connection_id,
                   upsert=False):
        api = self.get_jobs_api(pool_id)
        job_type = "REPLACE"
        if upsert:
            job_type = "DELTA"
        if not data_connection_id:
            payload = {'targetName': targetName, 'type': job_type,
                       'dataPoolId': pool_id}
        else:
            payload = {'targetName': targetName, 'type': job_type,
                       'dataPoolId': pool_id,
                       'connectionId': data_connection_id}
        r = requests.post(api, headers=self.get_auth(), json=payload)
        log.debug(f'created job with {r}')
        return r.json()

    def push_new_dir(self, pool_id, job_id, dir_path):
        files = [join(dir_path, f) for f in listdir(dir_path)
                 if isfile(join(dir_path, f))]
        parquet_files = list(filter(lambda f: f.endswith(".parquet"), files))
        for parquet_file in parquet_files:
            log.debug(f"Uploading chunk {parquet_file}")
            self.push_new_chunk(pool_id, job_id, parquet_file)

    def push_new_chunk(self, pool_id, job_id, file_path=None, dataframe=None):
        api = self.get_jobs_api(pool_id) + f"/{job_id}/chunks/upserted"
        try:
            if dataframe is not None:
                file_path = f'/home/jovyan/tmp_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")}.parquet'
                parquet_utils.write_parquet(dataframe, file_path)
            upload_file = {"file": open(file_path, "rb")}
            r = requests.post(api, files=upload_file, headers=self.get_auth())
            log.debug(f'pushed new chunk with {r}')
            Path(file_path).unlink()
        except Exception as e:
            log.error(f'failed pushing chunk {file_path} with error: {e}\n')
        return r

    def submit_job(self, pool_id, job_id):
        api = self.get_jobs_api(pool_id) + f"/{job_id}"
        r = requests.post(api, headers=self.get_auth())
        log.debug(f'submitted job {r}')
        return r


class cloudC:
    def get_api(self, path):
        return "https://{}.{}.celonis.cloud/{}".format(self.tenant, self.realm,
                                                       path)

    def __init__(self, tenant, realm, api_key, client_id, connection_id):
        self.tenant = tenant
        self.realm = realm
        self.api_key = api_key
        self.client_id = client_id
        self.connection_id = connection_id

    def get_jobs_api(self, dataPoolId):
        return self.get_api("continuous-batch-processing/api/v1/{}"
                            .format(dataPoolId))

    def get_auth(self):
        # if you want to use an api key change use Bearer instead of AppKey
        return {'Authorization': 'AppKey ' + self.api_key}

    def list_jobs(self, dataPoolId):
        api = self.get_jobs_api(dataPoolId)
        return requests.get(api, headers=self.get_auth()).json()

    def push_new_chunk(self, dataPoolId, tablename, file_path, fallbackVarcharLength=None):
        api = self.get_jobs_api(dataPoolId) + "/items"
        params = {'targetName': tablename, 'clientId': self.client_id, 'connectionId': self.connection_id, 'fallbackVarcharLength': fallbackVarcharLength, 'upsertStrategy': 'UPSERT_WITH_NULLIFICATION'}
        headers = self.get_auth()
        file = {'file': open(file_path, 'rb')}
        return requests.post(url=api, params=params, files=file, headers=headers)

    def get_status(self, dataPoolId, tablename):
        api = self.get_jobs_api(dataPoolId) + "/flushes"
        params = {'targetName': tablename, 'connectionId': self.connection_id}
        headers = self.get_auth()
        response = requests.get(url=api, params=params, headers=headers)
        if response.status_code != 200:
            print("Received non 200 code", response.status_code, response.content)
            return
        else:
            flushes = response.json(object_pairs_hook=collections.OrderedDict)
            return flushes