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

from unittest import mock

from airflow_hop.operators import HopPipelineOperator
from tests.operator_test_base import OperatorTestBase

DEFAULT_LOG_LEVEL = 'Basic'
DEFAULT_PROJECT_NAME = 'default'
DEFAULT_PIPELINE_NAME = 'get_param.hpl'
DEFAULT_PIPELINE = f'tests/assets/config/projects/{DEFAULT_PROJECT_NAME}' \
            f'/transforms/{DEFAULT_PIPELINE_NAME}'
DEFAULT_PIPE_CONFIG = f'tests/assets/config/projects/{DEFAULT_PROJECT_NAME}' \
            f'/metadata/pipeline-run-configuration/remote_hop_server.json'

class MockedPipelineRespone:
    """Create mocked responses"""

    def __init__(self, content, status_code):
        self.content = content
        self.status_code = status_code


def mock_requests(**kwargs) -> MockedPipelineRespone:
    if 'registerPipeline' in kwargs['url']:
        return MockedPipelineRespone("""
        <webresult>
            <result>OK</result>
            <message>Pipeline &#39;get_param&#39; was added to HopServer with id cae6cc35-f07a-4321-b211-bd884db655ac</message>
            <id>cae6cc35-f07a-4321-b211-bd884db655ac</id>
        </webresult>""", 200)

    if 'prepareExec' in kwargs['url']:
        return MockedPipelineRespone("""
        <webresult>
            <result>OK</result>
            <message/>
            <id/>
        </webresult>""", 200)
    if 'startExec' in kwargs['url']:
        return MockedPipelineRespone("""
        <webresult>
            <result>OK</result>
            <message/>
            <id/>
        </webresult>""", 200)
    if 'pipelineStatus' in kwargs['url']:
        return MockedPipelineRespone("""
        <pipeline-status>
            <pipeline_name>get_param</pipeline_name>
            <id>cae6cc35-f07a-4321-b211-bd884db655ac</id>
            <status_desc>Finished</status_desc>
            <error_desc/>
            <log_date>2022/07/22 12:36:18.971</log_date>
            <execution_start_date>2022/07/22 12:36:15.576</execution_start_date>
            <execution_end_date>2022/07/22 12:36:17.512</execution_end_date>
            <paused>N</paused>
            <transform_status_list>
                <transform_status><transformName>Get variables</transformName><copy>0</copy><linesRead>1</linesRead><linesWritten>1</linesWritten><linesInput>0</linesInput><linesOutput>0</linesOutput><linesUpdated>0</linesUpdated><linesRejected>0</linesRejected><errors>0</errors><input_buffer_size>0</input_buffer_size><output_buffer_size>0</output_buffer_size><statusDescription>Finished</statusDescription><seconds>0.1</seconds><speed> 14</speed><priority>-</priority><stopped>N</stopped><paused>N</paused></transform_status>
                <transform_status><transformName>Write to log</transformName><copy>0</copy><linesRead>1</linesRead><linesWritten>1</linesWritten><linesInput>0</linesInput><linesOutput>0</linesOutput><linesUpdated>0</linesUpdated><linesRejected>0</linesRejected><errors>0</errors><input_buffer_size>0</input_buffer_size><output_buffer_size>0</output_buffer_size><statusDescription>Finished</statusDescription><seconds>0.1</seconds><speed> 11</speed><priority>-</priority><stopped>N</stopped><paused>N</paused></transform_status>
            </transform_status_list>
            <first_log_line_nr>5</first_log_line_nr>
            <last_log_line_nr>10</last_log_line_nr>
            <result>
                <lines_input>0</lines_input>
                <lines_output>0</lines_output>
                <lines_read>1</lines_read>
                <lines_written>1</lines_written>
                <lines_updated>0</lines_updated>
                <lines_rejected>0</lines_rejected>
                <lines_deleted>0</lines_deleted>
                <nr_errors>0</nr_errors>
                <nr_files_retrieved>0</nr_files_retrieved>
                <entry_nr>0</entry_nr>
                <result>Y</result>
                <exit_status>0</exit_status>
                <is_stopped>N</is_stopped>
                <log_channel_id>e1cf42a3-0d19-4c10-898c-f56d61ec6125</log_channel_id>
                <log_text/>
                <elapsedTimeMillis>0</elapsedTimeMillis>
                <executionId/>
            </result>
            <logging_string>&lt;![CDATA[H4sIAAAAAAAAAK1RwUrDQBC99yseRaiFpk220GIgQg9RBEWplR5EZE2m24G4CbsbFcR/NxuJeCiS
            Q99hdmaYeTNvVoRCzMLlTAhEIp4v4miJAJfk8CYNy5eC7DRsMhes2e4pR2XKjKxlrXB6lYQT3Hqz
            TqIJtt48+DBNwvFAHKTeGnYEV6IoVcvcty74g3NcsyZtEAX/oi/33Wq9utmk9xskOPn8jb6OrSE5
            gL69R/sBRe65kka+euFcUdGcEnltpONSI0Y0PZsvYCkrdW7xiJ/EEE89+NIPyuqWZ9et2/iyEZHJ
            AlU3jbTyzzu7PUyt0YzasepWGLXVo8E3jYTssZwCAAA=
            ]]&gt;</logging_string>
        </pipeline-status>
        """, 200)


class TestPipelineOperator(OperatorTestBase):
    """Perform tests regarding operators"""

    @mock.patch('requests.get', side_effect = mock_requests)
    @mock.patch('requests.post', side_effect = mock_requests)
    def test_execute(self, mock_post, mock_get): # pylint: disable=unused-argument
        op = HopPipelineOperator(
            task_id = 'test_pipeline_operator',
            pipeline = DEFAULT_PIPELINE,
            project_name = DEFAULT_PROJECT_NAME,
            pipeline_config= DEFAULT_PIPE_CONFIG,
            log_level = DEFAULT_LOG_LEVEL)

        op.execute(context = {})
        self.assertEqual('cae6cc35-f07a-4321-b211-bd884db655ac',
            mock_get.call_args_list[0][1]['params']['id'])
        self.assertEqual('get_param.hpl',mock_get.call_args_list[0][1]['params']['name'])
        self.assertEqual('Y',mock_get.call_args_list[0][1]['params']['xml'])
