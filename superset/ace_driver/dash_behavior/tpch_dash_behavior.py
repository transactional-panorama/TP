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

import requests
import json
import time
import psycopg2

from superset.ace_driver.dash_behavior.base_dash_behavior import BaseDashBehavior
from superset.ace_driver.stats_collection.stats_collector import StatsCollector


def get_cur_time() -> int:
    return int(time.time() * 1000)


def get_ids_in_viewport(chart_ids: list, viewport: dict) -> list:
    return chart_ids[viewport["start"]:viewport["end"]]


class TPCHDashBehavior(BaseDashBehavior):
    def __init__(self, server_addr: str,
                 username: str,
                 password: str,
                 dashboard_title: str,
                 read_behavior: str,
                 write_behavior: str,
                 refresh_interval: int,
                 num_refresh: int,
                 mvc_properties: int,
                 opt_viewport: bool,
                 opt_exec_time: bool,
                 opt_skip_write: bool,
                 stat_dir: str,
                 db_name: str,
                 db_username: str,
                 db_password: str,
                 db_host: str,
                 db_port):
        super().__init__(server_addr, username, password, mvc_properties, opt_viewport,
                         opt_exec_time, opt_skip_write)
        self.read_behavior = read_behavior
        self.write_behavior = write_behavior
        self.refresh_interval_ms = refresh_interval * 1000
        self.num_refresh = num_refresh
        self.stat_dir = stat_dir

        self.db_name = db_name
        self.db_username = db_username
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.conn = None
        if self.write_behavior != "filter_change":
            try:
                self.conn = psycopg2.connect(dbname=db_name,
                                             user=db_username,
                                             password=db_password,
                                             host=db_host,
                                             port=db_port)
                self.delete_tuples_from_base_tables()
            except psycopg2.Error as e:
                print("Creating connection error: " + e.pgerror)
                exit(-1)

        # Workload-specific data
        self.dash_title = dashboard_title
        self.dash_id = None
        self.data_source_to_node_ids = {}
        self.chart_id_to_form_data = {}
        self.chart_id_to_ts = {}
        self.chart_ids = []

        self.txn_interval = 0.1
        self.viewport_interval_ms = 1000
        self.viewport_range = 4
        self.viewport_shift = 2
        self.viewport = {"start": 0, "end": self.viewport_range}
        self.sf = 1
        self.order_card = 1500000
        self.new_data_percentage = 0.01

        self.predefined_filters = [[{'col': 'l_linestatus',
                                     'op': 'IN',
                                     'val': ['F']
                                     }],
                                   [{'col': 'l_linestatus',
                                     'op': 'IN',
                                     'val': ['O']
                                     }],
                                   []
                                   ]
        self.filter_impacted_charts = ['tpch_q1', 'tpch_q3', 'tpch_q4',
                                       'tpch_q5', 'tpch_q6', 'tpch_q7',
                                       'tpch_q8', 'tpch_q9', 'tpch_q10',
                                       'tpch_q12', 'tpch_q14', 'tpch_q15',
                                       'tpch_q17', 'tpch_q18', 'tpch_q19',
                                       'tpch_q20', 'tpch_q21']
        self.chart_title_to_id = {}

        self.test_start_ts = get_cur_time()
        self.stat_collector = StatsCollector(
            stat_dir, self.test_start_ts, self.dash_title,
            read_behavior, write_behavior, refresh_interval,
            num_refresh, mvc_properties, opt_viewport,
            opt_exec_time, opt_skip_write)

    def delete_tuples_from_base_tables(self):
        rows_to_delete = self.sf * self.order_card * \
                         self.new_data_percentage * self.num_refresh
        with self.conn.cursor() as cur:
            cur.execute(f"delete from lineitem where l_orderkey <= {rows_to_delete};")
            cur.execute(f"delete from orders where o_orderkey <= {rows_to_delete};")

    def insert_tuples_to_base_tables(self, num_refresh_done: int):
        rows_to_insert_start = self.sf * self.order_card * \
                               self.new_data_percentage * num_refresh_done
        rows_to_insert_end = self.sf * self.order_card * \
                             self.new_data_percentage * (num_refresh_done + 1)
        with self.conn.cursor() as cur:
            cur.execute(f"insert into lineitem select * from lineitem_full "
                        f"where l_orderkey > {rows_to_insert_start}"
                        f"and l_orderkey <= {rows_to_insert_end}")
            cur.execute(f"insert into orders select * from orders_full "
                        f"where o_orderkey > {rows_to_insert_start}"
                        f"and o_orderkey <= {rows_to_insert_end}")

    def get_charts_info(self) -> None:
        get_charts_url = f"{self.url_header}/dashboard/{self.dash_id}/charts"
        charts_result = requests.get(get_charts_url, headers=self.headers)
        charts_dict = json.loads(charts_result.text)
        for chart_data in charts_dict["result"]:
            form_data = chart_data["form_data"]
            data_source_id = int(form_data["datasource"].split("__")[0])
            slice_id = int(form_data["slice_id"])
            slice_id_list = self.data_source_to_node_ids.get(data_source_id, list())
            slice_id_list.append(slice_id)
            self.chart_id_to_form_data[slice_id] = form_data
            self.chart_id_to_ts[slice_id] = -1
            self.chart_ids.append(slice_id)
            self.chart_title_to_id[chart_data["slice_name"]] = slice_id
        self.chart_ids.sort()

    def setup(self) -> None:
        self.login()
        self.load_all_dashboards()

        self.dash_id = self.dash_title_to_id[self.dash_title]
        super().config_simulation(self.dash_id, self.db_name,
                                  self.db_username, self.db_password,
                                  self.db_host, self.db_port)

    def run_test(self) -> None:
        refresh_counter = 0
        cur_filter_idx = 0
        cur_time = get_cur_time()
        last_refresh_time = cur_time - self.refresh_interval_ms
        last_viewport_change = cur_time
        submit_ts = 0
        commit_ts = 0
        viewport_up_to_date = True
        self.stat_collector.collect_viewport_change(
            cur_time,
            get_ids_in_viewport(self.chart_ids, self.viewport))
        while self.num_refresh != refresh_counter or submit_ts != commit_ts:
            # simulate a refresh when necessary
            ts_temp = self.simulate_next_refresh(last_refresh_time, refresh_counter,
                                                 cur_filter_idx, cur_time, self.viewport)
            if ts_temp != -1:
                submit_ts = ts_temp
                last_refresh_time = cur_time
                refresh_counter += 1
                cur_filter_idx = (cur_filter_idx + 1) % len(self.predefined_filters)

            # simulate a read when necessary
            if submit_ts != commit_ts:
                read_result = self.simulate_next_read()
                commit_ts = int(read_result["ts"])
                snapshot = read_result["snapshot"]
                viewport_up_to_date = self.is_up_to_date(snapshot)
                self.stat_collector.collect_read_views(
                    cur_time - self.test_start_ts,
                    submit_ts, self.chart_id_to_ts,
                    snapshot, int(self.txn_interval * 1000))

            # simulate a viewport change when necessary
            viewport_change = self.simulate_viewport_change(last_viewport_change,
                                                            cur_time,
                                                            viewport_up_to_date)
            if viewport_change:
                last_viewport_change = cur_time
                self.stat_collector.collect_viewport_change(
                    cur_time - self.test_start_ts,
                    get_ids_in_viewport(self.chart_ids, self.viewport))

            time.sleep(self.txn_interval)
            cur_time = get_cur_time()

    def simulate_next_refresh(self, last_refresh_time: int,
                              cur_filter_idx: int,
                              num_refresh_done: int,
                              cur_time: int,
                              viewport: dict) -> int:
        if cur_time - last_refresh_time < self.refresh_interval_ms:
            return -1
        if self.write_behavior == "filter_change":
            # Build a filter
            cur_filter = self.predefined_filters[cur_filter_idx]

            # Build other info required for a refresh
            node_ids_to_refresh = [self.chart_title_to_id[chart_title]
                                   for chart_title in self.filter_impacted_charts]
        else:  # source_data_change
            cur_filter = []
            node_ids_to_refresh = list(self.chart_id_to_form_data.keys())
            try:
                self.insert_tuples_to_base_tables(num_refresh_done)
            except psycopg2.Error as e:
                print("Inserting new data error: " + e.pgerror)
                exit(-1)
        node_id_to_form_data = {node_id: self.chart_id_to_form_data[node_id]
                                for node_id in node_ids_to_refresh}
        node_ids_in_viewport = get_ids_in_viewport(self.chart_ids, viewport)
        submit_ts = super().post_refresh(
            self.dash_id, node_ids_to_refresh,
            node_ids_in_viewport, node_id_to_form_data, cur_filter)
        for node_id in node_ids_to_refresh:
            self.chart_id_to_ts[node_id] = submit_ts
        self.stat_collector.collect_refresh(cur_time - self.test_start_ts,
                                            node_ids_to_refresh, submit_ts)
        return submit_ts

    def simulate_next_read(self) -> dict:
        node_ids_in_viewport = get_ids_in_viewport(self.chart_ids, self.viewport)
        return super().read_refreshed_charts(self.dash_id, node_ids_in_viewport)

    def simulate_viewport_change(self, last_view_port_change: int,
                                 cur_time: int,
                                 viewport_up_to_date: bool) -> bool:
        if self.read_behavior == "not_change":
            pass
        elif self.read_behavior == "regular_change":
            if cur_time - last_view_port_change >= self.viewport_interval_ms:
                self.move_view_port()
                return True
        else:  # see_change
            if viewport_up_to_date and cur_time - last_view_port_change >= self.viewport_interval_ms:
                self.move_view_port()
                return True
        return False

    def move_view_port(self) -> None:
        view_port_start = self.viewport["start"]
        view_port_start = (view_port_start + self.viewport_shift) % len(self.chart_ids)
        view_port_end = min(view_port_start + self.viewport_range, len(self.chart_ids))
        self.viewport["start"] = view_port_start
        self.viewport["end"] = view_port_end

    def is_up_to_date(self, snapshot: dict) -> bool:
        up_to_date = True
        for node_id_str in snapshot:
            node_result = snapshot[node_id_str]
            node_id = int(node_id_str)
            ts = int(node_result["ts"])
            version_result = node_result["version_result"]
            if version_result == "IV" or ts < self.chart_id_to_ts[node_id]:
                up_to_date = False
        return up_to_date

    def write_report_results(self) -> None:
        self.stat_collector.write_stats()

    def clean_up_dashboard(self) -> None:
        super().clean_up(self.dash_id)
        if self.conn:
            self.conn.close()
