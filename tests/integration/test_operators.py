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

from airflow import AirflowException

from airflow_hop.operators import HopPipelineOperator
from tests.operator_test_base import OperatorTestBase


class TestPipelineOperator(OperatorTestBase):
    """Perform tests regarding operators"""

    PROJECT = 'default'

    PIPELINE_ERR = 'whatever.hpl'
    PIPELINE_OK = f'{OperatorTestBase.TESTS_PATH}/assets/config/projects/'\
            'default/transforms/get_param.hpl'
    PIPELINE_CONFIGURATION = f'{OperatorTestBase.TESTS_PATH}/assets/config/' \
            'projects/default/metadata/pipeline-run-configuration/remote_hop_server.json'

    def test_execute(self):
        op = HopPipelineOperator(
            task_id = 'test_pipeline_operator',
            pipeline = self.PIPELINE_OK,
            project_name = self.PROJECT,
            log_level = 'Basic')

        try:
            op.execute(self.PIPELINE_CONFIGURATION,context = {})
        except Exception as ex:
            raise ex

    def test_execute_non_existent_pipeline(self):
        op = HopPipelineOperator(
            task_id = 'test_pipeline_operator',
            pipeline = self.PIPELINE_ERR,
            project_name = self.PROJECT,
            log_level = 'Basic')

        with self.assertRaises(AirflowException) as context:
            op.execute(self.PIPELINE_CONFIGURATION,context = {})

        self.assertTrue(f'ERROR: pipeline {self.PIPELINE_ERR} not found' in str(context.exception))