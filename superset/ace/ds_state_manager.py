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

import sys
import random
from typing import Tuple

from superset.ace.view_graph import *


def compute_iv_num(snapshot: dict) -> int:
    return sum(map(lambda ver: isinstance(ver, IV), snapshot.values()))


def to_basic_types(snapshot: dict) -> dict:
    snapshot_with_basic_types = {}
    for node_id in snapshot:
        new_version = snapshot[node_id]
        basic_type_dict = new_version.to_basic_type_dict()
        snapshot_with_basic_types[node_id] = basic_type_dict
    return snapshot_with_basic_types


class DashStateManager:
    def __init__(self, dependency_list: list) -> None:
        self.prop = PropertyCombination(1)
        self.k_relaxed = 0
        self.opt_viewport = True
        self.opt_exec_time = True
        self.opt_metrics = True
        self.opt_skip_write = True
        self.enable_stats_cache = True
        self.db_name = ""
        self.username = ""
        self.password = ""
        self.host = ""
        self.port = ""

        self.last_read = {}
        self.last_submitted = START_TS
        self.last_committed = START_TS
        self.view_port_time = {}
        self.view_graph = ViewGraph()
        for dependency in dependency_list:
            self.view_graph.insert(dependency)
        self.view_graph.create_initial_snapshot(START_TS)
        self.num_ivs = {START_TS: 0}

        self.global_lock = Lock()
        self.meta_data_lock = Lock()
        self.cur_ts = START_TS
        self.node_metrics = {}

    # The following functions are used by WTXnManager
    def submit_one_txn(self, node_id_set: set,
                       node_ids_in_view_port: set,
                       duration: int) -> Tuple[int, list]:
        self.global_lock.acquire()
        self.cur_ts = self.cur_ts + 1
        ts = self.cur_ts
        self.last_submitted = ts
        self.num_ivs[ts] = len(node_id_set)
        ret_list = self.view_graph.create_snapshot_placeholder(node_id_set, ts)
        self.global_lock.release()

        self.meta_data_lock.acquire()
        # initialize the view_port_time
        self.view_port_time[ts] = {}
        for node_group in ret_list:
            for node_id in node_group:
                if node_id in node_ids_in_view_port:
                    self.view_port_time[ts][node_id] = duration
                else:
                    self.view_port_time[ts][node_id] = 0
        self.meta_data_lock.release()

        return ts, ret_list

    def finish_one_update(self, node_id: int, ts: int, result: dict) -> None:
        self.view_graph.add_version(node_id, ts, result)
        self.global_lock.acquire()
        self.num_ivs[ts] -= 1
        self.global_lock.release()

    def commit_one_txn(self, ts: int) -> None:
        self.last_committed = ts

    def get_top_priority_node(self, ts: int, node_ids: set,
                              chart_id_to_cost: dict) -> int:
        if self.opt_metrics:
            ret_node_ids = set()
            ret_node_metric = 0
            for node_id in node_ids:
                if node_id in self.node_metrics:
                    if self.node_metrics[node_id] > ret_node_metric:
                        ret_node_ids = set()
                    if self.node_metrics[node_id] >= ret_node_metric:
                        ret_node_ids.add(node_id)
                        ret_node_metric = self.node_metrics[node_id]
            if len(ret_node_ids) == 0:
                ret_node_ids = node_ids
            return random.choice(tuple(ret_node_ids))

        if not self.opt_viewport and not self.opt_exec_time:
            return random.choice(tuple(node_ids))
        self.meta_data_lock.acquire()
        max_priority = -1
        ret_node_id = -1
        for node_id in node_ids:
            if not self.opt_viewport:
                cur_view_time = 1
            else:
                cur_view_time = self.view_port_time[ts][node_id]
            if node_id not in chart_id_to_cost:
                cur_execute_cost = 1
            else:
                cur_execute_cost = chart_id_to_cost[node_id]
            cur_priority = float(cur_view_time) / float(cur_execute_cost)
            if cur_priority > max_priority:
                max_priority = cur_priority
                ret_node_id = node_id
        self.meta_data_lock.release()
        return ret_node_id

    # The following functions are used by RTxnManager
    def config_state_manager(self, prop_comb: int,
                             k_relaxed: int,
                             opt_viewport: bool,
                             opt_exec_time: bool,
                             opt_metrics: bool,
                             opt_skip_write: bool,
                             enable_stats_cache: bool,
                             db_name: str,
                             username: str,
                             password: str,
                             host: str,
                             port: str) -> None:
        self.prop = PropertyCombination(prop_comb)
        self.k_relaxed = k_relaxed
        self.opt_viewport = opt_viewport
        self.opt_exec_time = opt_exec_time
        self.opt_metrics = opt_metrics
        self.opt_skip_write = opt_skip_write
        self.enable_stats_cache = enable_stats_cache

        if db_name == "":
            self.opt_exec_time = False
        self.db_name = db_name
        self.username = username
        self.password = password
        self.host = host
        self.port = port

    def read_view_port(self, node_id_set: set, duration: int) -> dict:
        self.global_lock.acquire()
        last_committed = self.last_committed
        last_submitted = self.last_submitted
        ts_to_read = last_committed
        if self.prop == PropertyCombination.GCNB:
            for ts_to_read in reversed(range(last_committed, last_submitted + 1)):
                if self.num_ivs[ts_to_read] <= self.k_relaxed:
                    break
        self.global_lock.release()

        # update the view_port_time
        self.meta_data_lock.acquire()
        for ts_active in range(last_committed + 1, last_submitted + 1):
            for node_id in node_id_set:
                if node_id in self.view_port_time[ts_active]:
                    self.view_port_time[ts_active][node_id] += duration
        for node_id in node_id_set:
            if node_id in self.node_metrics:
                self.node_metrics[node_id] += duration
            else:
                self.node_metrics[node_id] = duration
        self.meta_data_lock.release()

        if self.prop == PropertyCombination.ICNB:
            snapshot = self.view_graph.read_visible_versions(node_id_set)
        elif self.prop == PropertyCombination.GCNB:
            snapshot = self.view_graph.read_snapshot(ts_to_read, node_id_set)
        elif self.prop == PropertyCombination.LCMB:
            ts_lower_bound = self._ts_from_last_read(node_id_set)
            ts_list = [ts for ts in range(ts_lower_bound, last_submitted + 1)]
            ss_list = [self.view_graph.read_snapshot(ts, node_id_set) for ts in ts_list]
            ret_ss = {}
            ret_ts = START_TS - 1
            min_iv = min([compute_iv_num(cur_ss) for cur_ss in ss_list])
            for idx, cur_ss in enumerate(ss_list):
                cur_ts = ts_list[idx]
                cur_iv_num = compute_iv_num(cur_ss)
                if cur_iv_num <= min_iv + self.k_relaxed and cur_ts > ret_ts:
                    ret_ts = cur_ts
                    ret_ss = cur_ss
            snapshot = ret_ss
        elif self.prop == PropertyCombination.GCPB:
            snapshot = self.view_graph.read_snapshot(last_submitted, node_id_set)
        else:  # C-M_A
            ts_list = range(last_committed, last_submitted + 1)
            ss_list = [self.view_graph.read_snapshot(ts, node_id_set)
                       for ts in ts_list]
            ret_ss = {}
            ret_ts = START_TS - 1
            for idx, cur_ss in enumerate(ss_list):
                cur_ts = ts_list[idx]
                cur_iv_num = compute_iv_num(cur_ss)
                if cur_iv_num <= self.k_relaxed and cur_ts > ret_ts:
                    ret_ts = cur_ts
                    ret_ss = cur_ss
            snapshot = ret_ss

        return {"ts": last_committed,
                "snapshot": to_basic_types(self._update_last_read(snapshot))}

    def _ts_from_last_read(self, node_id_set: set) -> int:
        ts_lower_bound = START_TS
        for node_id in node_id_set:
            old_version = self.last_read.get(node_id, None)
            if old_version is not None and old_version.ts > ts_lower_bound:
                ts_lower_bound = old_version.ts
        return ts_lower_bound

    def _update_last_read(self, snapshot: dict) -> dict:
        new_snapshot = {}
        for node_id in snapshot:
            new_version = snapshot[node_id]
            old_version = self.last_read.get(node_id, None)
            if old_version is None or (not old_version.equal_ts(new_version)) \
                or (old_version.equal_ts(new_version) and
                    isinstance(old_version, IV) and isinstance(new_version, Version)):
                new_snapshot[node_id] = new_version
            self.last_read[node_id] = new_version
        return new_snapshot

    # The following functions are used by a garbage collector
    def clean_unused_versions(self) -> None:
        ls = self.last_submitted
        self.view_graph.clean_unused_versions(ls)
