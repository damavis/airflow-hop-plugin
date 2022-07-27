# -*- coding: utf-8 -*-
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
        params={'date':'{{ ds }}'})

    work_test = HopWorkflowOperator(
        task_id='work_test',
        workflow='workflows/workflowTest.hwf',
        project_name='default',
        log_level='Basic',
        params={'date':'{{ ds }}'})

    fake_users >> get_param >> work_test
