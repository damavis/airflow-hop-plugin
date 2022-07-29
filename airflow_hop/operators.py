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
import base64
import re
import zlib
import time
from typing import Any
from airflow import AirflowException

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow_hop.hooks import HopHook

class HopBaseOperator(BaseOperator):
    """Hop Base Operator"""

    LOG_TEMPLATE = '%s: %s, with id %s'
    FINISHED_STATUSES = ['Finished']
    ERROR_STATUSES = [
        'Stopped',
        'Finished (with errors)',
        'Stopped (with errors)'
    ]
    END_STATUSES = FINISHED_STATUSES + ERROR_STATUSES

    def _log_logging_string(self, raw_logging_string):
        logs = raw_logging_string
        cdata = re.match(r'\<\!\[CDATA\[([^\]]+)\]\]\>', logs)
        cdata = cdata.group(1) if cdata else raw_logging_string
        decoded_lines = zlib.decompress(base64.b64decode(cdata),
                                        16 + zlib.MAX_WBITS)
        if decoded_lines:
            for line in re.compile(r'\r\n|\n|\r').split(
                    decoded_lines.decode('utf-8')):
                self.log.info(line)

class HopWorkflowOperator(HopBaseOperator):
    """Hop Workflow Operator"""

    template_fields = ('task_params',)

    def __init__(self,
                 workflow,
                 project_name,
                 log_level,
                 *args,
                 environment=None,
                 params=None,
                 hop_conn_id='hop_default',
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.workflow = workflow
        self.project_name = project_name
        self.log_level = log_level
        self.task_params = params
        self.hop_conn_id = hop_conn_id
        self.environment = environment

    def __get_hop_client(self):
        return HopHook(
                self.project_name,
                self.environment,
                self.hop_conn_id,
                self.log_level).get_conn()

    def execute(self, context: Context) -> Any: # pylint: disable=unused-argument
        conn = self.__get_hop_client()
        register_rs = conn.register_workflow(self.workflow, self.task_params)
        message = register_rs['webresult']['message']
        work_id = register_rs['webresult']['id']
        self.log.info(f'{self.workflow}: {message}')

        start_rs = conn.start_workflow(self.workflow, work_id)
        result = start_rs['webresult']['result']
        self.log.info(f'{self.workflow}: Started {result}')

        work_status_rs = None
        status_desc = None
        while not work_status_rs or status_desc not in self.END_STATUSES:
            work_status_rs = conn.workflow_status(self.workflow, work_id)

            status = work_status_rs['workflow-status']
            status_desc = status['status_desc']
            self.log.info(self.LOG_TEMPLATE, status_desc, self.workflow, work_id)
            self._log_logging_string(status['logging_string'])

            if status_desc not in self.END_STATUSES:
                self.log.info('Sleeping 5 seconds before ask again')
                time.sleep(5)

        if 'error_desc' in status and status['error_desc']:
            self.log.error(self.LOG_TEMPLATE, status['error_desc'], self.workflow, work_id)

        if status_desc in self.ERROR_STATUSES:
            self.log.error(self.LOG_TEMPLATE, status_desc, self.workflow, work_id)
            raise AirflowException(status_desc)


class HopPipelineOperator(HopBaseOperator):
    """Hop Pipeline Operator"""

    template_fields = ('task_params',)

    def __init__(self,
                 pipeline,
                 pipe_config,
                 project_name,
                 log_level,
                 *args,
                 environment=None,
                 params=None,
                 hop_conn_id='hop_default',
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline
        self.pipe_config = pipe_config
        self.project_name = project_name
        self.log_level = log_level
        self.hop_conn_id = hop_conn_id
        self.task_params = params
        self.environment = environment

    def __get_hop_client(self):
        return HopHook(
                self.project_name,
                self.environment,
                self.hop_conn_id,
                self.log_level).get_conn()

    def execute(self, context: Context) -> Any: # pylint: disable=unused-argument
        conn = self.__get_hop_client()

        register_rs = conn.register_pipeline(self.pipeline, self.pipe_config, self.task_params)
        message = register_rs['webresult']['message']
        pipe_id = register_rs['webresult']['id']
        self.log.info(f'{self.pipeline}: {message}')

        prepare_exec_rs = conn.prepare_pipeline_exec(self.pipeline, pipe_id)
        result = prepare_exec_rs['webresult']['result']
        self.log.info(f'{self.pipeline}: Prepared {result}')

        start_exec_rs = conn.start_pipeline_execution(self.pipeline, pipe_id)
        result = start_exec_rs['webresult']['result']
        self.log.info(f'{self.pipeline}: Started {result}')

        pipe_status_rs = None
        status_desc = None
        while not pipe_status_rs or status_desc not in self.END_STATUSES:
            pipe_status_rs = conn.pipeline_status(self.pipeline, pipe_id)

            status = pipe_status_rs['pipeline-status']
            status_desc = status['status_desc']
            self.log.info(self.LOG_TEMPLATE, status_desc, self.pipeline, pipe_id)
            self._log_logging_string(status['logging_string'])

            if status_desc not in self.END_STATUSES:
                self.log.info('Sleeping 5 seconds before ask again')
                time.sleep(5)

        if 'error_desc' in status and status['error_desc']:
            self.log.error(self.LOG_TEMPLATE, status['error_desc'], self.pipeline, pipe_id)

        if status_desc in self.ERROR_STATUSES:
            self.log.error(self.LOG_TEMPLATE, status_desc, self.pipeline, pipe_id)
            raise AirflowException(status_desc)
