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
import tempfile

from kubernetes import client, config

from airflow.hooks.base_hook import BaseHook


class KubernetesHook(BaseHook):
    """
    Creates Kubernetes API connection.

    :param conn_id: the connection to Kubernetes cluster
    """

    def __init__(
        self,
        conn_id="kubernetes_default"
    ):
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson

    def get_conn(self):
        """
        Returns kubernetes api session for use with requests
        """
        if self._get_field(("in_cluster")):
            self.log.debug("loading kube_config from: in_cluster configuration")
            config.load_incluster_config()
        elif self._get_field("kube_config") is None or self._get_field("kube_config") == '':
            self.log.debug("loading kube_config from: default file")
            config.load_kube_config()
        else:
            with tempfile.NamedTemporaryFile() as temp_config:
                self.log.debug("loading kube_config from: connection kube_config")
                temp_config.write(self._get_field("kube_config").encode())
                temp_config.flush()
                config.load_kube_config(temp_config.name)
                temp_config.close()
        return client.ApiClient()

    def get_namespace(self):
        """
        Returns the namespace that defined in the connection
        """
        return self._get_field("namespace", default="default")

    def _get_field(self, field_name, default=None):
        """
        Fetches a field from extras, and returns it. This is some Airflow
        magic. The kubernetes hook type adds custom UI elements
        to the hook page, which allow admins to specify in_cluster configutation, kube_config, namespace etc.
        They get formatted as shown below.
        """
        full_field_name = 'extra__kubernetes__{}'.format(field_name)
        if full_field_name in self.extras:
            return self.extras[full_field_name]
        else:
            return default
