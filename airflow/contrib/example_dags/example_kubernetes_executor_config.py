# -*- coding: utf-8 -*-
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
"""
This is an example dag for using a Kubernetes Executor Configuration.
"""
import os

import airflow
from airflow.contrib.example_dags.libs.helper import print_stuff
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'fs_group': 2000,
    'run_as_user': 1000
}

with DAG(
    dag_id='example_kubernetes_executor_config4',
    default_args=default_args,
    schedule_interval=None
) as dag:

    def test_volume_mount():
        """
        Tests whether the volume has been mounted.
        """
        with open('/foo/volume_mount_test.txt', 'w') as foo:
            foo.write('Hello')

        return_code = os.system("cat /foo/volume_mount_test.txt")
        if return_code != 0:
            raise ValueError(f"Error when checking volume mount. Return code {return_code}")

    # You can use annotations on your kubernetes pods!
    start_task = PythonOperator(
        task_id="start_task",
        python_callable=print_stuff,
        executor_config={
            "KubernetesExecutor": {
                "volumes": [
                    {
                        "name": "example-kubernetes-test-volume",
                        "emptyDir": {},
                    },
                    {
                        "name": "git-volume",
                        "emptyDir": {},
                    }
                ],
                "volume_mounts": [
                    {
                        "mountPath": "/usr/local/airflow/",
                        "name": "example-kubernetes-test-volume",
                    },
                    {
                        "mountPath": "/tmp/",
                        "name": "git-volume",
                    }

                ]
            }
        }
    )

    # You can mount volume or secret to the worker pod
    second_task = PythonOperator(
        task_id="four_task",
        python_callable=test_volume_mount,
        executor_config={
            "KubernetesExecutor": {
                "volumes": [
                    {
                        "name": "example-kubernetes-test-volume",
                        "emptyDir": {},
                    },
                    {
                        "name": "git-volume",
                        "emptyDir": {},
                    }
                ],
                "volume_mounts": [
                    {
                        "mountPath": "/usr/local/airflow/",
                        "name": "example-kubernetes-test-volume",
                    },
                    {
                        "mountPath": "/tmp/",
                        "name": "git-volume",
                    }

                ]
            }
        }
    )

    # Test that we can run tasks as a normal user
    third_task = PythonOperator(
        task_id="non_root_task",
        python_callable=print_stuff,
        executor_config={
            "KubernetesExecutor": {
                "volumes": [
                    {
                        "name": "example-kubernetes-test-volume",
                        "emptyDir": {},
                    },
                    {
                        "name": "git-volume",
                        "emptyDir": {},
                    }
                ],
                "volume_mounts": [
                    {
                        "mountPath": "/usr/local/airflow/",
                        "name": "example-kubernetes-test-volume",
                    },
                    {
                        "mountPath": "/tmp/",
                        "name": "git-volume",
                    }

                ]
            }
        }
    )

    start_task >> second_task >> third_task
