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
from typing import Dict, Optional

from kubernetes import client

from airflow.exceptions import AirflowException
from airflow_spark_k8s.hooks.kubernetes import KubernetesHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class SparkKubernetesSensor(BaseSensorOperator):
    """
    Checks sparkApplication object in kubernetes cluster:
       .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/api-docs.md#sparkapplication

    :param application_name: spark Application resource name
    :type application_name:  str
    :param namespace: the kubernetes namespace where the sparkApplication reside in
    :type namespace: str
    :param kubernetes_conn_id: the connection to Kubernetes cluster
    :type conn_id: str
    """

    template_fields = ('application_name', 'namespace')
    INTERMEDIATE_STATES = ('SUBMITTED', 'RUNNING', 'PENDING_RERUN', '')
    FAILURE_STATES = ('FAILED', 'UNKNOWN')
    SUCCESS_STATES = 'COMPLETED'

    @apply_defaults
    def __init__(self,
                 application_name: str,
                 namespace: Optional[str] = None,
                 conn_id: str = 'kubernetes_default',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.application_name = application_name
        self.namespace = namespace
        self.conn_id = conn_id

    def poke(self, context: Dict):
        self.log.info("Poking: %s", self.application_name)
        hook = KubernetesHook(conn_id=self.conn_id)
        api_client = hook.get_conn()
        custom_resource_definition_api = client.CustomObjectsApi(api_client)
        if self.namespace is None:
            namespace = hook.get_namespace()
        else:
            namespace = self.namespace
        try:
            response = custom_resource_definition_api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                name=self.application_name
            )
            try:
                application_state = response['status']['applicationState']['state']
            except KeyError:
                return False
            if application_state in self.FAILURE_STATES:
                raise AirflowException("Spark application failed with state: %s" % application_state)
            if application_state in self.INTERMEDIATE_STATES:
                self.log.info("Spark application is still in state: %s", application_state)
                return False
            if application_state in self.SUCCESS_STATES:
                self.log.info("Spark application ended successfully")
                return True
            raise AirflowException("Unknown spark application state: %s" % application_state)
        except client.rest.ApiException as e:
            raise AirflowException("Exception when calling -> get_custom_resource_definition: %s\n" % e)
