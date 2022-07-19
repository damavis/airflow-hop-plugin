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
import gzip
import json
from xml.etree import ElementTree
from xml.etree.ElementTree import Element

class XMLBuilder:
    """
    Builds an XML file to be sent through HTTP protocol
    """
    def __init__(self, metastore_file, hop_config):
        self.metastore_file = metastore_file
        self.hop_config = hop_config

    def get_workflow_xml(self, filename) -> str:
        root = Element('workflow_configuration')
        workflow = ElementTree.parse(filename)
        root.append(workflow.getroot())
        root.append(self.__get_workflow_execuion_config())
        root.append(self.__generate_element('metastore_json'), self.__generate_metastore)
        return ElementTree.tostring(root, encoding='unicode')

    def __get_workflow_execuion_config(self) -> Element:
        root = Element('workflow_execution_configuration')
        root.append(self.__generate_element('parameters'))      # TODO: Implement parameters
        root.append(self.__get_variables())
        root.append(self.__generate_element('log_level','Basic'))
        root.append(self.__generate_element('clear_log','Y'))
        root.append(self.__generate_element('start_copy_name'))
        root.append(self.__generate_element('gather_metrics','Y'))
        root.append(self.__generate_element('expand_remote_workflow','N'))
        root.append(self.__generate_element('run_configuration','local'))
        return root


    def get_pipeline_xml(self, filename) -> str:
        root = Element('pipeline_configuration')
        pipeline = ElementTree.parse(filename)
        root.append(pipeline.getroot())
        root.append(self.__get_pipeline_execution_config())
        root.append(self.__generate_element('metastore_json'), self.__generate_metastore)
        return ElementTree.tostring(root, encoding='unicode')

    def __get_pipeline_execution_config(self) -> Element:
        root = Element('pipeline_execution_configuration')
        root.append(self.__generate_element('pass_export','N'))
        root.append(self.__generate_element('parameters'))      # TODO: Implement parameters
        root.append(self.__get_variables())
        root.append(self.__generate_element('log_level','Basic'))
        root.append(self.__generate_element('log_filename'))
        root.append(self.__generate_element('log_file_append','N'))
        root.append(self.__generate_element('create_parent_folder','N'))
        root.append(self.__generate_element('clear_log','Y'))
        root.append(self.__generate_element('show_subcomponents','Y'))
        root.append(self.__generate_element('run_configuration','local'))
        return root

    def __get_variables(self) -> Element:
        with open(self.hop_config, encoding='utf-8') as f:
            data = json.load(f)
        variables = data['variables']
        variables = sorted(variables, key = lambda x: x['name']) # TODO: Check if it's necessary
        root = Element('variables')
        for variable in variables:
            new_variable = Element('variable')
            new_variable.append(self.__generate_element('name', variable['name']))
            new_variable.append(self.__generate_element('value', variable['value']))
            root.append(new_variable)

        project_home_var = Element('variable')
        project_home_var.append(self.__generate_element('name','PROJECT_HOME'))
         # TODO: Ask directory to user
        project_home_var.append(self.__generate_element('value',
                                                        'config/projects/default'))
        root.append(project_home_var)

        jdk_debug = Element('variable')
        jdk_debug.append(self.__generate_element('name','jdk.debug'))
        jdk_debug.append(self.__generate_element('value','release'))
        root.append(jdk_debug)
        return root

    def __generate_metastore(self) -> str:
        file = open(self.metastore_file, mode='br')
        content = file.read()
        file.close()
        metastore = gzip.compress(content)
        return base64.b64encode(metastore).decode('utf-8')

    def __generate_element(self, name:str, text = None) -> Element:
        element = Element(name)
        if text is not None:
            element.text = text
        return element
