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
from typing import Tuple

from superset.ace.util_class import Node
from superset.ace.util_class import NodeType
from superset.ace.util_class import Dependency
from superset.ace.ds_state_manager import DashStateManager
from superset.ace.scheduler import Scheduler

from superset.models.dashboard import Dashboard
from superset.extensions import (
    ace_state_manager,
    ace_scheduler_manager,
)

DURATION = 1


def add_ds_state_manager(dashboard: Dashboard) -> None:
    dash_id = dashboard.id
    dependency_list = []
    for s in dashboard.slices:
        dep_node = Node(int(s.id), NodeType.VIZ)
        prec_node = Node(int(s.datasource_id), NodeType.BASE_TABLE)
        dependency_list.append(Dependency(prec_node, dep_node))

    ds_state_manager = DashStateManager(dependency_list)
    ace_state_manager[dash_id] = ds_state_manager


def remove_ds_state_manager(dash_id: int) -> None:
    if dash_id in ace_state_manager:
        del ace_state_manager[dash_id]


def config_ace(dash_id: int,
               mvc_properties: int,
               opt_viewport: bool,
               opt_exec_time: bool,
               opt_skip_write: bool,
               db_name: str,
               username: str,
               password: str,
               host: str,
               port: str) -> None:
    if dash_id in ace_state_manager:
        ds_manager = ace_state_manager[dash_id]
        ds_manager.config_state_manager(mvc_properties,
                                        opt_viewport,
                                        opt_exec_time,
                                        opt_skip_write,
                                        db_name,
                                        username,
                                        password,
                                        host,
                                        port)


def read_view_port(dash_id: int, node_id_set: set) -> dict:
    return ace_state_manager[dash_id].read_view_port(node_id_set, DURATION)


def submit_one_txn(dash_id: int, node_id_set: set,
                   node_ids_in_view_port: set) -> Tuple[int, list]:
    return ace_state_manager[dash_id].submit_one_txn(
        node_id_set,
        node_ids_in_view_port, DURATION)


def get_or_create_one_scheduler(dash_id, app) -> Scheduler:
    if dash_id not in ace_scheduler_manager:
        ace_scheduler_manager[dash_id] = Scheduler(dash_id, app)
        ace_scheduler_manager[dash_id].start()
    return ace_scheduler_manager[dash_id]


def shut_down_one_scheduler(dash_id) -> None:
    if dash_id in ace_scheduler_manager:
        ace_scheduler_manager[dash_id].shut_down()
        ace_scheduler_manager[dash_id].join(1)
        del ace_scheduler_manager[dash_id]
