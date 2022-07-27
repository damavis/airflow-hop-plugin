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

from airflow_hop.operators import HopPipelineOperator, HopWorkflowOperator
from tests.operator_test_base import OperatorTestBase

PROJECT = 'default'

PIPELINE_ERR = 'whatever.hpl'
PIPELINE_OK = 'pipelines/get_param.hpl'
PIPELINE_CONFIGURATION = 'remote hop server'

WORKFLOW_OK = 'workflows/workflowTest.hwf'
WORKFLOW_ERR = 'whatever.hwf'

class TestPipelineOperator(OperatorTestBase):
    """Perform tests regarding pipeline operators"""


    def test_execute(self):
        op = HopPipelineOperator(
            task_id='test_pipeline_operator',
            pipeline=PIPELINE_OK,
            pipe_config=PIPELINE_CONFIGURATION,
            project_name=PROJECT,
            log_level='Basic',
            params={'DATE':'{{ ds }}'})

        try:
            op.execute(context = {})
        except Exception as ex:
            raise ex

    def test_execute_non_existent_pipeline(self):
        op = HopPipelineOperator(
            task_id = 'test_pipeline_operator',
            pipeline = PIPELINE_ERR,
            pipe_config=PIPELINE_CONFIGURATION,
            project_name = PROJECT,
            log_level = 'Basic',
            params={'DATE':'{{ ds }}'})

        with self.assertRaises(AirflowException) as context:
            op.execute(context = {})

        self.assertTrue(f'{PIPELINE_ERR} not found' in str(context.exception))

class TestWorkflowOperator(OperatorTestBase):
    """Perform tests regarding workflow operators"""

    def test_execute(self):
        op = HopWorkflowOperator(
            task_id='test_workflow_operator',
            workflow=WORKFLOW_OK,
            project_name=PROJECT,
            log_level='Basic',
            params={'DATE':'{{ ds }}'})

        try:
            op.execute(context={})
        except Exception as ex:
            raise ex

    def test_execute_non_existent_workflow(self):
        op = HopWorkflowOperator(
            task_id='test_workflow_operator',
            workflow=WORKFLOW_ERR,
            project_name=PROJECT,
            log_level='Basic',
            params={'DATE':'{{ ds }}'})

        with self.assertRaises(AirflowException) as context:
            op.execute(context = {})

        self.assertTrue(f'{WORKFLOW_ERR} not found' in str(context.exception))
