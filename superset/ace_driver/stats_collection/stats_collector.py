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

import os
import json
from typing import Union, Dict


class StatsCollector:
    def __init__(self, stat_dir: str,
                 test_ts: int,
                 dash_title: str,
                 viewport_range: int,
                 shift_step: int,
                 explore_range: int,
                 read_behavior: str,
                 viewport_start: int,
                 write_behavior: str,
                 refresh_interval: int,
                 num_refresh: int,
                 mvc_properties: int,
                 k_relaxed: int,
                 opt_viewport: bool,
                 opt_exec_time: bool,
                 opt_skip_write: bool,
                 enable_stats_cache: bool,
                 enable_iv_sl_log: bool,
                 sf: int):
        self.stat_dir = stat_dir
        self.behavior_logs = []
        self.refresh_logs = []
        self.viewport_change_logs = []
        self.read_views_logs = []
        self.iv_sl_logs = []
        self.invisibility = 0.0
        self.staleness = 0.0
        self.weighted_staleness = 0.0
        self.total_time = 0.0
        self.log_file = "behavior.log"
        self.refresh_log_file = "refresh.log"
        self.viewport_log_file = "viewport.log"
        self.iv_sl_file = "iv_sl.log"
        self.read_log_file = "read.log"
        self.stat_file = "stat.out"
        self.enable_iv_sl_log = enable_iv_sl_log
        self.configs: Dict[str, Union[int, float, str, bool]] = \
            {"test_ts": test_ts,
             "dash_title": dash_title,
             "viewport_range": viewport_range,
             "shift_step": shift_step,
             "explore_range": explore_range,
             "read_behavior": read_behavior,
             "viewport_start": viewport_start,
             "write_behavior": write_behavior,
             "refresh_interval": refresh_interval,
             "num_refresh": num_refresh,
             "mvc_properties": mvc_properties,
             "k_relaxed": k_relaxed,
             "opt_viewport": opt_viewport,
             "opt_exec_time": opt_exec_time,
             "opt_skip_write": opt_skip_write,
             "enable_stats_cache": enable_stats_cache,
             "enable_iv_sl_log": enable_iv_sl_log,
             "sf": sf}

    def collect_refresh(self, log_time: int,
                        node_ids_to_refresh: list,
                        txn_id: int):
        log_dict = {"log_time": log_time,
                    "log_type": "refresh",
                    "node_ids": node_ids_to_refresh,
                    "txn_id": txn_id}
        self.behavior_logs.append(log_dict)
        self.refresh_logs.append(log_dict)

    def collect_commit(self, log_time: int,
                       txn_id: int):
        log_dict = {"log_time": log_time,
                    "log_type": "commit",
                    "txn_id": txn_id}
        self.behavior_logs.append(log_dict)
        self.refresh_logs.append(log_dict)

    def collect_viewport_change(self, log_time: int,
                                node_ids_in_viewport: list):
        log_dict = {"log_time": log_time,
                    "log_type": "viewport_change",
                    "node_ids": node_ids_in_viewport}
        self.behavior_logs.append(log_dict)
        self.viewport_change_logs.append(log_dict)

    def collect_read_views(self, log_time: int,
                           recent_refresh_id: int,
                           node_id_to_ts: dict,
                           ts_to_real_ts: dict,
                           new_read: dict,
                           read_snapshot: dict,
                           txn_duration: int):
        if new_read:
            log_dict = {"log_time": log_time,
                        "log_type": "read_views",
                        "recent_refresh_id": recent_refresh_id,
                        "snapshot": new_read}
            self.behavior_logs.append(log_dict)
            self.read_views_logs.append(log_dict)
        self.update_stats(log_time, node_id_to_ts, ts_to_real_ts,
                          read_snapshot, txn_duration)

    def update_stats(self, log_time: int,
                     node_id_to_ts: dict,
                     ts_to_real_ts: dict,
                     read_snapshot: dict,
                     txn_duration: int):
        num_iv = 0
        num_sl = 0
        for node_id in read_snapshot:
            node_result = read_snapshot[node_id]
            ts = int(node_result["ts"])
            version_result = node_result["version_result"]
            if version_result == "IV":
                self.invisibility += txn_duration
                num_iv += 1
            else:
                if node_id_to_ts[node_id] != ts:
                    self.staleness += txn_duration
                    num_sl += 1
                up_to_date_real_ts = ts_to_real_ts[node_id_to_ts[node_id]]
                cur_real_ts = ts_to_real_ts[ts]
                weighted_node_staleness = \
                    (up_to_date_real_ts - cur_real_ts) * txn_duration
                self.weighted_staleness += weighted_node_staleness
            self.total_time += txn_duration
        if self.enable_iv_sl_log:
            log_dict = {"log_time": log_time,
                        "iv": num_iv,
                        "sl": num_sl}
            self.iv_sl_logs.append(log_dict)

    def flush_logs(self, logs: list, log_file_name: str):
        log_file_path = os.path.join(self.stat_dir, log_file_name)
        with open(log_file_path, 'a') as f:
            f.write(json.dumps(self.configs, indent=4))
            for one_log in logs:
                f.write(json.dumps(one_log, indent=4))

    def write_stats(self, cur_time: int, end_to_end_time: int):
        self.flush_logs(self.behavior_logs, self.log_file)
        self.flush_logs(self.refresh_logs, self.refresh_log_file)
        self.flush_logs(self.viewport_change_logs, self.viewport_log_file)
        self.flush_logs(self.read_views_logs, self.read_log_file)
        if self.enable_iv_sl_log:
            self.flush_logs(self.iv_sl_logs, self.iv_sl_file)

        stat_file_path = os.path.join(self.stat_dir, self.stat_file)
        stat_dict = self.configs.copy()
        stat_dict["end_to_end_time"] = end_to_end_time
        stat_dict["test_time"] = cur_time
        stat_dict["invisibility"] = self.invisibility
        stat_dict["staleness"] = self.staleness
        stat_dict["weighted_staleness"] = self.weighted_staleness
        stat_dict["total_time"] = self.total_time
        with open(stat_file_path, 'a') as f:
            f.write(json.dumps(stat_dict, indent=4))
