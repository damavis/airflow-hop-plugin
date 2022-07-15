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

from airflow.hooks.base import BaseHook


class HopHook(BaseHook):

    class HopServerConnection:  # TODO: Implement me

        def register_pipeline(self):
            pass

        def register_workflow(self):
            pass

        def pipeline_status(self):
            pass

        def workflow_status(self):
            pass

        def prepare_exec(self):
            pass

        def start_execution(self):
            pass

        def stop_execution(self):
            pass

    def __init__(self, source, conn_id='hop_default'):
        super().__init__(source)
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson
        self.pentaho_cli = None

    def get_conn(self) -> Any:
        raise NotImplementedError()  # TODO: Implement me
