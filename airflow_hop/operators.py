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
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context


class HopWorkflowOperator(BaseOperator):

    template_fields = ('task_params',)

    def __init__(self,
                 workflow,
                 params=None,
                 hop_conn_id='hop_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.workflow = workflow
        self.hop_conn_id = hop_conn_id
        self.task_params = params

    def execute(self, context: Context) -> Any:
        pass  # TODO: Implement me


class HopPipelineOperator(BaseOperator):

    template_fields = ('task_params',)

    def __init__(self,
                 pipeline,
                 params=None,
                 hop_conn_id='hop_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline
        self.hop_conn_id = hop_conn_id
        self.task_params = params

    def execute(self, context: Context) -> Any:
        pass  # TODO: Implement me
