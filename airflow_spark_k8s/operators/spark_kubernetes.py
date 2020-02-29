#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from typing import Optional

import yaml
from kubernetes import client

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow_spark_k8s.hooks.kubernetes import KubernetesHook
from airflow.utils.decorators import apply_defaults


class SparkKubernetesOperator(BaseOperator):
    """
    Creates sparkApplication object in kubernetes cluster:

       .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/api-docs.md#sparkapplication

    :param application_file: filepath to kubernetes custom_resource_definition of sparkApplication
    :type application_file:  str
    :param namespace: kubernetes namespace to put sparkApplication
    :type namespace: str
    :param kubernetes_conn_id: the connection to Kubernetes cluster
    :type conn_id: str
    """

    template_fields = ['application_file', 'namespace']
    template_ext = ('yaml', 'yml', 'json')
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 application_file: str,
                 namespace: Optional[str] = None,
                 conn_id: str = 'kubernetes_default',
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.conn_id = conn_id

    def execute(self, context):
        self.log.info("Creating sparkApplication")
        hook = KubernetesHook(conn_id=self.conn_id)
        api_client = hook.get_conn()
        api = client.CustomObjectsApi(api_client)
        application_dict = self._load_application_to_dict()
        if self.namespace is None:
            namespace = hook.get_namespace()
        else:
            namespace = self.namespace
        try:
            response = api.create_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                body=application_dict
            )
            self.log.debug("Response: %s", response)
            return response
        except client.rest.ApiException as e:
            raise AirflowException("Exception when calling -> create_custom_resource_definition: %s\n" % e)

    def _load_application_to_dict(self):
        try:
            application_dict = yaml.safe_load(self.application_file)
        except yaml.YAMLError as e:
            raise AirflowException("Exception when loading application_file: %s\n" % e)
        return application_dict
