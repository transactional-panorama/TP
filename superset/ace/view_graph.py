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

from superset.ace.util_class import *
from queue import SimpleQueue


class ViewGraph:
    def __init__(self) -> None:
        self.id_to_node = {}
        self.prec_to_dep = {}

    def insert(self, dependency: Dependency) -> None:
        dep = dependency.dep
        prec = dependency.prec

        self.id_to_node[dep.node_id] = dep
        self.id_to_node[prec.node_id] = prec

        dep_list = self.prec_to_dep.get(prec.node_id, list())
        dep_list.append(dep.node_id)
        self.prec_to_dep[prec.node_id] = dep_list

    def create_initial_snapshot(self, ts: int) -> None:
        for node_id in self.id_to_node:
            result = {RESPONSE_CODE: 400,
                      RESPONSE: "Not initialized yet"}
            version = Version(ts, result)
            node = self.id_to_node[node_id]
            node.add_version(version)

    def create_snapshot_placeholder(self,
                                    node_id_set: set, ts: int) -> list:
        impacted_set = set()
        base_table_set = set()
        filter_set = set()
        viz_set = set()

        pending_queue = SimpleQueue()
        for node_id in node_id_set:
            pending_queue.put(node_id)

        while not pending_queue.empty():
            cur_node_id = pending_queue.get()
            cur_node = self.id_to_node[cur_node_id]

            impacted_set.add(cur_node_id)
            result = {
                RESPONSE_CODE: 200,
                RESPONSE: "Done"
            }

            if cur_node.node_type == NodeType.BASE_TABLE:
                cur_node.add_version(Version(ts, result))
                base_table_set.add(cur_node.node_id)

            elif cur_node.node_type == NodeType.FILTER:
                cur_node.add_version(Version(ts, result))
                filter_set.add(cur_node.node_id)

            else:
                cur_node.add_iv(IV(ts))
                viz_set.add(cur_node.node_id)

            if cur_node.node_id in self.prec_to_dep:
                dep_id_list = self.prec_to_dep[cur_node.node_id]
                for dep_node_id in dep_id_list:
                    if dep_node_id not in impacted_set:
                        pending_queue.put(dep_node_id)

        ret_list = list()
        ret_list.append(base_table_set)
        ret_list.append(filter_set)
        ret_list.append(viz_set)

        return ret_list

    def read_snapshot(self, ts: int, node_id_set: set) -> dict:
        snapshot = {}
        for node_id in node_id_set:
            node = self.id_to_node[node_id]
            version = node.get_version_by_snapshot(ts)
            snapshot[node_id] = version
        return snapshot

    def read_visible_versions(self, node_id_set: set) -> dict:
        res = {}
        for node_id in node_id_set:
            node = self.id_to_node[node_id]
            version = node.get_visible_version()
            res[node_id] = version
        return res

    def add_version(self, node_id: int, ts: int, result: dict):
        self.id_to_node[node_id].add_version(Version(ts, result))

    def clean_unused_versions(self, ts: int):
        for node in self.id_to_node.values():
            node.clean_unused_versions(ts)
