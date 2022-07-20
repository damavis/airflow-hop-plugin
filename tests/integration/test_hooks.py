# -*- coding: utf-8 -*-
# Copyright 2022 Aneior Studio, SL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
from unittest import TestCase

from airflow_hop.hooks import HopHook

SLEEP_TIME = 2

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 8081
DEFAULT_USERNAME = 'cluster'
DEFAULT_PASSWORD = 'cluster'
DEFAULT_LOG_LEVEL = 'Basic'
DEFAULT_HOP_CONFIGURATION = '../assets/hop-config.json'
DEFAULT_METASTORE_FILE = '../assets/metadata.json'
DEFAULT_PIPELINE_NAME = 'fake-data-generate-person-record.hpl'
DEFAULT_PIPELINE_PATH = f'../assets/{DEFAULT_PIPELINE_NAME}'
DEFAULT_WORKFLOW_NAME = 'workflow-executor-child.hwf'
DEFAULT_WORKFLOW_PATH = f'../assets/{DEFAULT_WORKFLOW_NAME}'

class TestHopHook(TestCase):
    """
    Perform tests regarding Hooks
    """

    def __get_client(self):
        return HopHook.HopServerConnection(
                                    DEFAULT_HOST,
                                    DEFAULT_PORT,
                                    DEFAULT_USERNAME,
                                    DEFAULT_PASSWORD,
                                    DEFAULT_LOG_LEVEL,
                                    DEFAULT_METASTORE_FILE,
                                    DEFAULT_HOP_CONFIGURATION)

    def test_client_constructor(self):
        client = self.__get_client()
        self.assertEqual(client.host, DEFAULT_HOST)
        self.assertEqual(client.port, DEFAULT_PORT)
        self.assertEqual(client.username, DEFAULT_USERNAME)
        self.assertEqual(client.password, DEFAULT_PASSWORD)
        self.assertEqual(client.log_level, DEFAULT_LOG_LEVEL)
        self.assertEqual(client.metastore_file, DEFAULT_METASTORE_FILE)
        self.assertEqual(client.config_file, DEFAULT_HOP_CONFIGURATION)

    def test_run_pipeline_and_wait(self):
        client = self.__get_client()
        result = client.register_pipeline(DEFAULT_PIPELINE_PATH)
        pipe_id = result['webresult']['id']
        self.assertEqual(result['webresult']['result'],'OK')

        result = client.prepare_pipeline_exec(DEFAULT_PIPELINE_NAME, pipe_id)
        self.assertEqual(result['webresult']['result'],'OK')

        result = client.start_pipeline_execution(DEFAULT_PIPELINE_NAME, pipe_id)
        self.assertEqual(result['webresult']['result'],'OK')

        result = {}
        while not result or result['pipeline-status']['status_desc'] != 'Finished':
            result = client.pipeline_status(DEFAULT_PIPELINE_NAME, pipe_id)
            time.sleep(SLEEP_TIME)
        self.assertTrue('result' in result['pipeline-status'])

    def test_run_workflow_and_wait(self):
        client = self.__get_client()
        result = client.register_workflow(DEFAULT_WORKFLOW_PATH)
        work_id = result['webresult']['id']
        self.assertEqual(result['webresult']['result'],'OK')

        result = client.start_workflow(DEFAULT_WORKFLOW_NAME, work_id)
        self.assertEqual(result['webresult']['result'],'OK')

        result = client.workflow_status(DEFAULT_WORKFLOW_NAME, work_id)
        self.assertEqual(result['webresult']['result'],'OK')

        result = {}
        while not result or result['workflow-status']['status_desc'] != 'Finished':
            result = client.workflow_status(DEFAULT_WORKFLOW_NAME, work_id)
            time.sleep(SLEEP_TIME)
        self.assertTrue('result' in result['workflow-status'])
