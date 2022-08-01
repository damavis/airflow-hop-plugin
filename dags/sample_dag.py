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

from airflow import DAG
from datetime import datetime

from airflow_hop.operators import HopPipelineOperator
from airflow_hop.operators import HopWorkflowOperator

with DAG('sample_dag', start_date=datetime(2022,7,26),
        schedule_interval='@daily', catchup=False) as dag:

    fake_users = HopPipelineOperator(
        task_id='fake_users',
        pipeline='pipelines/fake-data-generate-person-record.hpl',
        pipe_config='remote hop server',
        project_name='default',
        log_level='Basic')

    get_param = HopPipelineOperator(
        task_id='get_param',
        pipeline='pipelines/get_param.hpl',
        pipe_config='remote hop server',
        project_name='default',
        log_level='Basic',
        params={'DATE':'{{ ds }}'})

    work_test = HopWorkflowOperator(
        task_id='work_test',
        workflow='workflows/workflowTest.hwf',
        project_name='default',
        log_level='Basic',
        params={'DATE':'{{ ds }}'})

    fake_users >> get_param >> work_test
