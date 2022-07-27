# -*- coding: utf-8 -*-
from airflow import DAG
from datetime import datetime

from airflow_hop.operators import HopPipelineOperator
from airflow_hop.operators import HopWorkflowOperator

with DAG('fake_users', start_date=datetime(2022,7,26),
        schedule_interval='@daily', catchup=False) as dag:

    fake_users = HopPipelineOperator(
        task_id='fake_users',
        pipeline='fake-data-generate-person-record.hpl',
        pipeline_config='remote_hop_server.json',
        project_name='default',
        log_level='Basic')

    get_param = HopPipelineOperator(
        task_id='get_param',
        pipeline='/home/pal7/Documents/airflow-hop-plugin/tests/assets/' \
            'config/projects/default/transforms/get_param.hpl',
        pipeline_config='/home/pal7/Documents/airflow-hop-plugin/tests/' \
            'assets/config/projects/default/metadata/pipeline-run-configuration' \
            '/remote_hop_server.json',
        project_name='default',
        log_level='Basic')

    work_test = HopWorkflowOperator(
        task_id='work_test',
        workflow='/home/pal7/Documents/airflow-hop-plugin/tests/assets/config/' \
            'projects/default/transforms/new_workflow.hwf',
        project_name='default',
        log_level='Basic',
        params={'ds':'{{ ds }}'})

    fake_users >> get_param >> work_test
