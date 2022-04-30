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

from superset.ace_driver.dash_behavior.base_dash_behavior import BaseDashBehavior


class TPCHDashBehavior(BaseDashBehavior):
    def __init__(self, server_addr: str,
                 username: str,
                 password: str,
                 read_behavior: str,
                 write_behavior: str,
                 refresh_interval: int,
                 num_refresh: int,
                 mvc_properties: int,
                 opt_viewport: bool,
                 opt_exec_time: bool,
                 opt_skip_write: bool,
                 stat_dir: str):
        super().__init__(server_addr, username, password, mvc_properties, opt_viewport,
                         opt_exec_time, opt_skip_write)
        self.read_behavior = read_behavior
        self.write_behavior = write_behavior
        self.refresh_interval = refresh_interval
        self.num_refresh = num_refresh
        self.stat_dir = stat_dir

        # Workload-specific data
        self.dash_title = "TPCH"
        self.dash_id = None
        self.data_source_to_chart_list = {}
        self.chart_id_to_form_data = {}
        self.chart_ids = []

        self.predefined_filters = {}
        self.viewport_range = 4
        self.viewport_shift = 2
        self.viewport = {"start": 0, "end": self.viewport_range}

    def get_charts_info(self) -> None:
        get_charts_url = f"{self.url_header}/dashboard/{self.dash_id}/charts"
        charts_result = requests.get(get_charts_url, headers=self.headers)
        charts_dict = json.loads(charts_result.text)
        for chart_data in charts_dict["result"]:
            form_data = chart_data["form_data"]
            data_source_id = int(form_data["datasource"].split("__")[0])
            slice_id = int(form_data["slice_id"])
            slice_id_list = self.data_source_to_chart_list.get(data_source_id, list())
            slice_id_list.append(slice_id)
            self.chart_id_to_form_data[slice_id] = form_data
            self.chart_ids.append(slice_id)

    def setup(self) -> None:
        self.login()
        self.load_all_dashboards()

        self.dash_id = self.dash_title_to_id[self.dash_title]
        super().config_simulation(self.dash_id)

    def simulate_next_step(self) -> bool:
        pass

    def write_report_results(self) -> None:
        pass

    def clean_up_dashboard(self) -> None:
        super().clean_up(self.dash_id)
