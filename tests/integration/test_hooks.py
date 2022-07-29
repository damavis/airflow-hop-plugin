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

from airflow import AirflowException

from airflow_hop.hooks import HopHook
from tests import TestBase
SLEEP_TIME = 2

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8081
DEFAULT_USERNAME = 'cluster'
DEFAULT_PASSWORD = 'cluster'
DEFAULT_ERR_PASSWORD = 'wrong'
DEFAULT_LOG_LEVEL = 'Basic'

DEFAULT_HOP_HOME = f'{TestBase.TESTS_PATH}/assets'
DEFAULT_PROJECT_NAME = 'default'
DEFAULT_PIPE_CONFIG_NAME = 'remote hop server'
DEFAULT_PIPELINE_NAME = 'pipelines/get_param.hpl'
FAKE_PIPE_ID = '123'
DEFAULT_WORKFLOW_NAME = 'workflows/workflowTest.hwf'
FAKE_WORKFLOW_ID = '456'

DEFAULT_ENVIRONMENT = 'Dev'


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
                                    DEFAULT_HOP_HOME,
                                    DEFAULT_PROJECT_NAME,
                                    DEFAULT_ENVIRONMENT)
    def __get_error_client(self):
        return HopHook.HopServerConnection(
                                    DEFAULT_HOST,
                                    DEFAULT_PORT,
                                    DEFAULT_USERNAME,
                                    DEFAULT_ERR_PASSWORD,
                                    DEFAULT_LOG_LEVEL,
                                    DEFAULT_HOP_HOME,
                                    DEFAULT_PROJECT_NAME,
                                    DEFAULT_ENVIRONMENT)

    def test_client_constructor(self):
        client = self.__get_client()
        self.assertEqual(client.host, DEFAULT_HOST)
        self.assertEqual(client.port, DEFAULT_PORT)
        self.assertEqual(client.username, DEFAULT_USERNAME)
        self.assertEqual(client.password, DEFAULT_PASSWORD)
        self.assertEqual(client.log_level, DEFAULT_LOG_LEVEL)
        self.assertEqual(client.hop_home, DEFAULT_HOP_HOME)
        self.assertEqual(client.project_name, DEFAULT_PROJECT_NAME)
        self.assertEqual(client.environment, DEFAULT_ENVIRONMENT)

    def test_run_pipeline_and_wait(self):
        client = self.__get_client()
        result = client.register_pipeline(DEFAULT_PIPELINE_NAME, DEFAULT_PIPE_CONFIG_NAME)
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

    def test_run_pipeline_and_stop_it(self):
        client = self.__get_client()
        result = client.register_pipeline(DEFAULT_PIPELINE_NAME, DEFAULT_PIPE_CONFIG_NAME)
        pipe_id = result['webresult']['id']
        self.assertEqual(result['webresult']['result'],'OK')

        result = client.prepare_pipeline_exec(DEFAULT_PIPELINE_NAME, pipe_id)
        self.assertEqual(result['webresult']['result'],'OK')

        result = client.start_pipeline_execution(DEFAULT_PIPELINE_NAME, pipe_id)
        self.assertEqual(result['webresult']['result'],'OK')

        result = client.stop_pipeline_execution(DEFAULT_PIPELINE_NAME, pipe_id)
        self.assertEqual(result['webresult']['result'],'OK')

        result = {}
        while not result or (result['pipeline-status']['status_desc'] != 'Finished' \
            and result['pipeline-status']['status_desc'] != 'Stopped'):
            result = client.pipeline_status(DEFAULT_PIPELINE_NAME, pipe_id)
            time.sleep(SLEEP_TIME)
        self.assertTrue('result' in result['pipeline-status'])

    def test_run_workflow_and_wait(self):
        client = self.__get_client()
        result = client.register_workflow(DEFAULT_WORKFLOW_NAME)
        work_id = result['webresult']['id']
        self.assertEqual(result['webresult']['result'],'OK')

        result = client.start_workflow(DEFAULT_WORKFLOW_NAME, work_id)
        self.assertEqual(result['webresult']['result'],'OK')

        result = {}
        while not result or result['workflow-status']['status_desc'] != 'Finished':
            result = client.workflow_status(DEFAULT_WORKFLOW_NAME, work_id)
            time.sleep(SLEEP_TIME)
        self.assertTrue('result' in result['workflow-status'])

    def test_run_workflow_and_stop_it(self):
        client = self.__get_client()
        result = client.register_workflow(DEFAULT_WORKFLOW_NAME)
        work_id = result['webresult']['id']
        self.assertEqual(result['webresult']['result'],'OK')

        result = client.start_workflow(DEFAULT_WORKFLOW_NAME, work_id)
        self.assertEqual(result['webresult']['result'],'OK')

        time.sleep(0.5)

        result = client.stop_workflow(DEFAULT_WORKFLOW_NAME, work_id)
        self.assertEqual(result['webresult']['result'],'OK')

        result = {}
        while not result or (result['workflow-status']['status_desc'] != 'Finished' \
            and result['workflow-status']['status_desc'] != 'Stopped'):
            result = client.workflow_status(DEFAULT_WORKFLOW_NAME, work_id)
            time.sleep(SLEEP_TIME)
        self.assertTrue('result' in result['workflow-status'])

    def test_run_pipeline_with_http_error(self):
        client = self.__get_error_client()
        with self.assertRaises(AirflowException) as context:
            client.register_pipeline(DEFAULT_PIPELINE_NAME, DEFAULT_PIPE_CONFIG_NAME)
        self.assertTrue('Error 401' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.prepare_pipeline_exec(DEFAULT_PIPELINE_NAME, FAKE_PIPE_ID)
        self.assertTrue('Error 401' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.start_pipeline_execution(DEFAULT_PIPELINE_NAME, FAKE_PIPE_ID)
        self.assertTrue('Error 401' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.pipeline_status(DEFAULT_PIPELINE_NAME, FAKE_PIPE_ID)
        self.assertTrue('Error 401' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.stop_pipeline_execution(DEFAULT_PIPELINE_NAME, FAKE_PIPE_ID)
        self.assertTrue('Error 401' in str(context.exception))

    def test_run_workflow_with_http_error(self):
        client = self.__get_error_client()
        with self.assertRaises(AirflowException) as context:
            client.register_workflow(DEFAULT_WORKFLOW_NAME)
        self.assertTrue('Error 401' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.start_workflow(DEFAULT_WORKFLOW_NAME, FAKE_WORKFLOW_ID)
        self.assertTrue('Error 401' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.workflow_status(DEFAULT_WORKFLOW_NAME, FAKE_WORKFLOW_ID)
        self.assertTrue('Error 401' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.stop_workflow(DEFAULT_WORKFLOW_NAME, FAKE_WORKFLOW_ID)
        self.assertTrue('Error 401' in str(context.exception))

    def test_pipeline_with_hop_error(self):
        client = self.__get_client()

        with self.assertRaises(AirflowException) as context:
            client.prepare_pipeline_exec(DEFAULT_PIPELINE_NAME, FAKE_PIPE_ID)
        self.assertTrue('ERROR' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.start_pipeline_execution(DEFAULT_PIPELINE_NAME, FAKE_PIPE_ID)
        self.assertTrue('ERROR' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.pipeline_status(DEFAULT_PIPELINE_NAME, FAKE_PIPE_ID)
        self.assertTrue('ERROR' in str(context.exception))

    def test_workflow_with_hop_error(self):
        client = self.__get_client()

        with self.assertRaises(AirflowException) as context:
            client.start_workflow(DEFAULT_WORKFLOW_NAME, FAKE_WORKFLOW_ID)
        self.assertTrue('ERROR' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.workflow_status(DEFAULT_WORKFLOW_NAME, FAKE_WORKFLOW_ID)
        self.assertTrue('ERROR' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            client.stop_workflow(DEFAULT_WORKFLOW_NAME, FAKE_WORKFLOW_ID)
        self.assertTrue('ERROR' in str(context.exception))
