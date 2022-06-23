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

import random
import requests
import json
import time
import psycopg2
from requests import HTTPError

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
                 stat_dir: str,
                 db_name: str,
                 db_username: str,
                 db_password: str,
                 db_host: str,
                 db_port: str,
                 sf: int):
        super().__init__(server_addr, username, password, mvc_properties, k_relaxed,
                         opt_viewport, opt_exec_time, opt_skip_write,
                         enable_stats_cache)
        self.read_behavior = read_behavior
        self.write_behavior = write_behavior
        self.refresh_interval_ms = refresh_interval * 1000
        self.num_refresh = num_refresh
        self.stat_dir = stat_dir
        self.viewport_range = viewport_range
        self.viewport_shift_step = shift_step
        if explore_range < viewport_range:
            self.explore_range = viewport_range
        else:
            self.explore_range = explore_range
        self.explore_start = None
        self.explore_end = None

        # Workload-specific data
        self.dash_title = dashboard_title
        self.dash_id = None
        self.data_source_to_node_ids = {}
        self.chart_id_to_form_data = {}
        self.chart_ids = []

        self.chart_id_to_submit_ts = {}
        self.ts_to_real_ts = {}
        self.chart_id_to_vis_result = {}

        self.txn_interval = 0.1
        self.viewport_interval_ms = 1000
        if viewport_start % 2 == 1:
            self.viewport_start = viewport_start - 1
        else:
            self.viewport_start = viewport_start
        self.viewport = {}

        self.is_up = False
        self.sf = sf
        self.order_card = 1500000
        self.part_card = 200000
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
        self.chart_id_to_title = {}

        self.test_start_ts = 0
        self.test_init_ts = get_cur_time()
        self.stat_collector = StatsCollector(
            stat_dir, self.test_init_ts, self.dash_title,
            viewport_range, shift_step, explore_range,
            read_behavior, viewport_start, write_behavior, refresh_interval,
            num_refresh, mvc_properties, k_relaxed, opt_viewport,
            opt_exec_time, opt_skip_write, enable_stats_cache, enable_iv_sl_log, sf)

        # Global states during the test
        self.refresh_counter = 0
        self.cur_filter_idx = 0
        self.cur_time = 0
        self.last_refresh_time = 0
        self.last_viewport_change = 0
        self.submit_ts = -1
        self.commit_ts = -1
        self.viewport_up_to_date = False

        self.db_name = db_name
        self.db_username = db_username
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.conn = None
        self.filter_change = "filter_change"
        if self.write_behavior != self.filter_change:
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

    def init_viewport(self, viewport_start):
        if viewport_start > len(self.chart_ids) - self.viewport_range:
            self.viewport = {"start": len(self.chart_ids) - self.viewport_range,
                             "end": len(self.chart_ids)}
        else:
            self.viewport = {"start": viewport_start,
                             "end": viewport_start + self.viewport_range}

    def init_explore_range(self, explore_range: int):
        self.explore_end = min(len(self.chart_ids), self.viewport_start + explore_range)
        self.explore_start = max(0, self.explore_end - explore_range)

    def delete_tuples_from_base_tables(self):
        order_rows_to_delete = self.sf * self.order_card * \
                               self.new_data_percentage * self.num_refresh
        part_rows_to_delete = self.sf * self.part_card * \
                              self.new_data_percentage * self.num_refresh

        with self.conn.cursor() as cur:
            cur.execute(
                f"delete from lineitem where l_orderkey <= {order_rows_to_delete};")
            cur.execute(
                f"delete from orders where o_orderkey <= {order_rows_to_delete};")
            cur.execute(
                f"delete from partsupp where ps_partkey <= {part_rows_to_delete};")

    def insert_tuples_to_base_tables(self, num_refresh_done: int):
        order_rows_to_insert_start = self.sf * self.order_card * \
                                     self.new_data_percentage * num_refresh_done
        order_rows_to_insert_end = self.sf * self.order_card * \
                                   self.new_data_percentage * (num_refresh_done + 1)

        part_rows_to_insert_start = self.sf * self.part_card * \
                                    self.new_data_percentage * num_refresh_done
        part_rows_to_insert_end = self.sf * self.part_card * \
                                  self.new_data_percentage * (num_refresh_done + 1)
        with self.conn.cursor() as cur:
            cur.execute(f"insert into orders select * from orders_full "
                        f"where o_orderkey > {order_rows_to_insert_start}"
                        f"and o_orderkey <= {order_rows_to_insert_end}")
            cur.execute(f"insert into lineitem select * from lineitem_full "
                        f"where l_orderkey > {order_rows_to_insert_start}"
                        f"and l_orderkey <= {order_rows_to_insert_end}")
            cur.execute(f"insert into partsupp select * from partsupp_full "
                        f"where ps_partkey > {part_rows_to_insert_start}"
                        f"and ps_partkey <= {part_rows_to_insert_end}")

    def get_charts_info(self, init_submit_ts: int) -> None:
        get_charts_url = f"{self.url_header}/dashboard/{self.dash_id}/charts"
        charts_result = requests.get(get_charts_url, headers=self.headers)
        try:
            charts_result.raise_for_status()
            charts_dict = json.loads(charts_result.text)
            for chart_data in charts_dict["result"]:
                form_data = chart_data["form_data"]
                data_source_id = int(form_data["datasource"].split("__")[0])
                slice_id = int(form_data["slice_id"])
                slice_id_list = self.data_source_to_node_ids.get(data_source_id, list())
                slice_id_list.append(slice_id)
                self.chart_id_to_form_data[slice_id] = form_data
                self.chart_id_to_submit_ts[slice_id] = init_submit_ts
                self.chart_ids.append(slice_id)
                self.chart_title_to_id[chart_data["slice_name"]] = slice_id
                self.chart_id_to_title[slice_id] = chart_data["slice_name"]
            self.chart_ids.sort()
        except HTTPError as error:
            print("Getting Charts Error: " + str(error.response))
            exit(-1)

    def setup(self) -> None:
        self.login()
        self.load_all_dashboards()

        self.dash_id = self.dash_title_to_id[self.dash_title]
        super().config_simulation(self.dash_id, self.db_name,
                                  self.db_username, self.db_password,
                                  self.db_host, self.db_port)
        self.get_charts_info(-1)
        self.init_viewport(self.viewport_start)
        self.init_explore_range(self.explore_range)

    def run_test(self) -> None:
        self.initial_loading()

        self.cur_time = get_cur_time()
        self.test_start_ts = self.cur_time
        self.last_viewport_change = self.cur_time
        new_ids_in_viewport = get_ids_in_viewport(self.chart_ids, self.viewport)
        self.print("Init viewport: " + str(new_ids_in_viewport))

        self.stat_collector.collect_viewport_change(
            self.cur_time - self.test_start_ts, new_ids_in_viewport)
        self.last_refresh_time = self.cur_time - self.refresh_interval_ms

        while True:

            if (self.write_behavior == self.filter_change and
                self.refresh_counter == self.num_refresh and
                self.submit_ts == self.commit_ts):
                break

            if (self.write_behavior != self.filter_change and
                self.refresh_counter == self.num_refresh and
                self.submit_ts == self.commit_ts and
                self.cur_time - self.last_refresh_time > self.refresh_interval_ms):
                break

            # simulate a refresh when necessary
            self.simulate_one_refresh()

            # simulate a read when necessary
            node_ids_in_viewport = get_ids_in_viewport(self.chart_ids,
                                                       self.viewport)
            new_read = None
            if self.submit_ts != self.commit_ts:
                read_result = self.enhance_read_result(
                    super().read_refreshed_charts(self.dash_id,
                                                  node_ids_in_viewport))
                new_commit_ts = int(read_result["ts"])
                if new_commit_ts != self.commit_ts:
                    self.print(f"Refresh {new_commit_ts} committed;"
                               f"Submitted ts is {self.submit_ts}")
                    self.stat_collector.collect_commit(
                        self.cur_time - self.test_start_ts, new_commit_ts)
                self.commit_ts = new_commit_ts
                new_read = read_result["snapshot"]
                self.update_chart_results(new_read)
                if new_read:
                    self.print(json.dumps(read_result))
            # Update stats about reading views
            read_snapshot = self.create_read_snapshot(node_ids_in_viewport)
            self.viewport_up_to_date = self.is_up_to_date(read_snapshot)
            self.stat_collector.collect_read_views(
                self.cur_time - self.test_start_ts,
                self.submit_ts, self.chart_id_to_submit_ts, self.ts_to_real_ts,
                new_read, read_snapshot, int(self.txn_interval * 1000))

            # simulate a viewport change when necessary
            viewport_change = self.simulate_viewport_change()
            if viewport_change:
                self.last_viewport_change = self.cur_time
                new_ids_in_viewport = get_ids_in_viewport(self.chart_ids, self.viewport)
                self.stat_collector.collect_viewport_change(
                    self.cur_time - self.test_start_ts, new_ids_in_viewport)
                self.print("New viewport: " + str(new_ids_in_viewport))

            time.sleep(self.txn_interval)
            self.cur_time = get_cur_time()

    def initial_loading(self):
        self.print("Loading visualizations")
        # initially reading all of the visualizations
        node_ids_to_refresh = self.chart_ids
        node_id_to_form_data = {node_id: self.chart_id_to_form_data[node_id]
                                for node_id in node_ids_to_refresh}
        node_ids_in_viewport = get_ids_in_viewport(self.chart_ids, self.viewport)
        cur_filter = []

        self.submit_ts = super().post_refresh(
            self.dash_id, node_ids_to_refresh,
            node_ids_in_viewport, node_id_to_form_data, cur_filter)
        self.ts_to_real_ts[self.submit_ts] = get_cur_time()
        for node_id in node_ids_to_refresh:
            self.chart_id_to_submit_ts[node_id] = self.submit_ts

        while self.submit_ts != self.commit_ts:
            read_result = self.enhance_read_result(
                super().read_refreshed_charts(self.dash_id, self.chart_ids))
            self.commit_ts = read_result["ts"]
            new_read = read_result["snapshot"]
            self.update_chart_results(new_read)
            if new_read:
                self.print(json.dumps(read_result))
            time.sleep(self.txn_interval * 10)
        self.print("Finished initial loading")

    def enhance_read_result(self, read_result: dict) -> dict:
        for chart_id_str in read_result["snapshot"]:
            result = read_result["snapshot"][chart_id_str]
            chart_id = int(chart_id_str)
            result["title"] = self.chart_id_to_title[chart_id]
        return read_result

    def simulate_one_refresh(self) -> None:
        if self.refresh_counter >= self.num_refresh or \
            self.cur_time - self.last_refresh_time < self.refresh_interval_ms:
            return

        # Build necessary data structures
        if self.write_behavior == self.filter_change:
            # Build a filter
            cur_filter = self.predefined_filters[self.cur_filter_idx]

            # Build other info required for a refresh
            node_ids_to_refresh = [self.chart_title_to_id[chart_title]
                                   for chart_title in self.filter_impacted_charts]
        else:  # source_data_change
            cur_filter = []
            node_ids_to_refresh = list(self.chart_ids)
            try:
                self.insert_tuples_to_base_tables(self.refresh_counter)
            except psycopg2.Error as e:
                print("Inserting new data error: " + e.pgerror)
                exit(-1)
        node_id_to_form_data = {node_id: self.chart_id_to_form_data[node_id]
                                for node_id in node_ids_to_refresh}
        node_ids_in_viewport = get_ids_in_viewport(self.chart_ids, self.viewport)

        # Submit a refresh
        self.submit_ts = super().post_refresh(
            self.dash_id, node_ids_to_refresh,
            node_ids_in_viewport, node_id_to_form_data, cur_filter)
        self.ts_to_real_ts[self.submit_ts] = self.cur_time

        # Update related statistics and states
        for node_id in node_ids_to_refresh:
            self.chart_id_to_submit_ts[node_id] = self.submit_ts
        self.stat_collector.collect_refresh(self.cur_time - self.test_start_ts,
                                            node_ids_to_refresh, self.submit_ts)
        self.last_refresh_time = self.cur_time
        self.refresh_counter += 1
        self.cur_filter_idx = (self.cur_filter_idx + 1) % \
                              len(self.predefined_filters)
        self.print("Submitted refresh " + str(self.submit_ts))
        self.print("Charts to refresh " + str(node_ids_to_refresh))

    def simulate_next_read(self) -> dict:
        node_ids_in_viewport = get_ids_in_viewport(self.chart_ids, self.viewport)
        return super().read_refreshed_charts(self.dash_id, node_ids_in_viewport)

    def update_chart_results(self, snapshot: dict) -> None:
        for chart_id_str in snapshot:
            node_result = snapshot[chart_id_str]
            chart_id = int(chart_id_str)
            self.chart_id_to_vis_result[chart_id] = node_result

    def create_read_snapshot(self, chart_ids: list) -> dict:
        read_snapshot = {}
        for chart_id in chart_ids:
            read_snapshot[chart_id] = self.chart_id_to_vis_result[chart_id]
        return read_snapshot

    def simulate_viewport_change(self) -> bool:
        if self.read_behavior == "not_change":
            pass
        elif self.read_behavior == "regular_change":
            if self.cur_time - self.last_viewport_change >= self.viewport_interval_ms:
                self.is_up = self.move_viewport(self.is_up)
                return True
        elif self.read_behavior == "see_change":
            if self.viewport_up_to_date and \
                self.cur_time - self.last_viewport_change \
                >= self.viewport_interval_ms:
                self.is_up = self.move_viewport(self.is_up)
                return True
        else:  # random_regular_change
            if self.cur_time - self.last_viewport_change >= self.viewport_interval_ms:
                if random.randint(0, 1) == 0:
                    self.is_up = not self.is_up
                self.is_up = self.move_viewport(self.is_up)
                return True
        return False

    def move_viewport(self, is_up: bool) -> bool:
        if is_up:
            new_viewport_start = max(self.explore_start,
                                     self.viewport["start"] - self.viewport_shift_step)
            new_viewport_end = new_viewport_start + self.viewport_range
        else:
            new_viewport_end = min(self.explore_end,
                                   self.viewport["end"] + self.viewport_shift_step)
            new_viewport_start = new_viewport_end - self.viewport_range
        self.viewport["start"] = new_viewport_start
        self.viewport["end"] = new_viewport_end
        if new_viewport_start == self.explore_start:
            return False
        elif new_viewport_end == self.explore_end:
            return True
        else:
            return is_up

    def is_up_to_date(self, snapshot: dict) -> bool:
        up_to_date = True
        for node_id in snapshot:
            node_result = snapshot[node_id]
            ts = int(node_result["ts"])
            version_result = node_result["version_result"]
            if version_result == "IV" or ts < self.chart_id_to_submit_ts[node_id]:
                up_to_date = False
        return up_to_date

    def write_report_results(self) -> None:
        self.stat_collector.write_stats(self.cur_time - self.test_start_ts,
                                        self.cur_time - self.test_init_ts)

    def clean_up_dashboard(self) -> None:
        super().clean_up(self.dash_id)
        if self.conn:
            self.conn.close()
