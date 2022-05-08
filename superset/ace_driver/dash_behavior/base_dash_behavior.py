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

import pprint
import requests
import json
from requests import HTTPError


def clean_read_results(read_results: dict) -> dict:
    snapshot = read_results["snapshot"]
    for node_id_str in snapshot:
        result = snapshot[node_id_str]
        result["version_result"] = "response"
    return read_results


def gen_request_chart_form_data(form_data, filters: []) -> dict:
    data_source_str = form_data["datasource"]
    data_source_id = int(data_source_str.split("__")[0])
    data_source_type = data_source_str.split("__")[1]
    metrics = []
    if form_data.get("metric", None) is not None:
        metrics.append(form_data.get("metric"))
    if form_data.get("metrics", None) is not None:
        metrics.extend(form_data.get("metrics"))
    if len(form_data['groupby']) == 0:
        columns = form_data.get('all_columns', [])
    else:
        columns = form_data.get('groupby', [])
    order_by = [[metric, True] for metric in metrics]
    request_body = {
        "datasource": {"id": data_source_id, "type": data_source_type},
        "force": False,
        "result_format": "json",
        "result_type": "full",
        "queries": [{
            "time_range": form_data["time_range"],
            "filters": filters,
            "extra": {
                "time_range_endpoints": ["inclusive", "exclusive"],
                "having": "",
                "having_druid": [],
                "where": "",
            },
            "applied_time_extra": {},
            "columns": columns,
            "metrics": metrics,
            "orderby": order_by,
            "annotation_layers": [],
            "row_limit": 100,
            "timeseries_limit": 0,
            "order_desc": True,
            "url_params": {},
            "custom_params": {},
            "custom_form_data": {},
            "group_by": form_data.get('group_by', []),
        }]
    }
    return request_body


class BaseDashBehavior:

    def __init__(self, server_addr: str,
                 username: str,
                 password: str,
                 mvc_properties: int,
                 opt_viewport: bool,
                 opt_exec_time: bool,
                 opt_skip_write: bool):
        self.url_header = f"http://{server_addr}/api/v1"
        self.username = username
        self.password = password
        self.access_token = None
        self.refresh_token = None
        self.headers = None

        self.dash_title_to_id = {}

        self.mvc_properties = mvc_properties
        self.opt_viewport = opt_viewport
        self.opt_exec_time = opt_exec_time
        self.opt_skip_write = opt_skip_write

        self.pp = pprint.PrettyPrinter(indent=4)

    def print(self, to_print: str) -> None:
        self.pp.pprint(to_print)

    def login(self) -> None:
        login_url = f"{self.url_header}/security/login"
        login_request_body = {
            "username": self.username,
            "password": self.password,
            "provider": "db",
            "refresh": True,
        }
        login_result = requests.post(login_url, json=login_request_body)
        token_dict = json.loads(login_result.text)
        self.access_token = token_dict["access_token"]
        self.refresh_token = token_dict["refresh_token"]
        self.headers = {"Content-type": "application/json",
                        "Authorization": f"Bearer {self.access_token}"}

    def load_all_dashboards(self) -> None:
        try:
            get_dash_info_url = f"{self.url_header}/dashboard"
            dash_info_result = requests.get(get_dash_info_url, headers=self.headers)
            dash_info_result.raise_for_status()
            for dash_info in json.loads(dash_info_result.text)["result"]:
                dash_id = dash_info["id"]
                dash_title = dash_info["dashboard_title"]
                self.dash_title_to_id[dash_title] = dash_id
        except HTTPError as error:
            print("Loading Dashboards Error: " + str(error.response))
            exit(-1)

    def config_simulation(self, dash_id: str,
                          db_name: str,
                          db_username: str,
                          db_password: str,
                          db_host: str,
                          db_port: str) -> None:
        try:
            # create ace state
            create_dash_state_url = f"{self.url_header}/dashboard/ace/" \
                                    f"{dash_id}/create_ds_state"
            dash_state_result = requests.post(create_dash_state_url,
                                              headers=self.headers,
                                              json={})
            dash_state_result.raise_for_status()

            # config ace
            config_url = f"{self.url_header}/dashboard/ace/{dash_id}/config"
            json_body = {
                "mvc_properties": self.mvc_properties,
                "opt_viewport": self.opt_viewport,
                "opt_exec_time": self.opt_exec_time,
                "opt_skip_write": self.opt_skip_write,
                "db_name": db_name,
                "username": db_username,
                "password": db_password,
                "host": db_host,
                "port": db_port,
            }
            result = requests.post(config_url,
                                   headers=self.headers,
                                   json=json_body)
            result.raise_for_status()
        except HTTPError as error:
            print("Config Error: " + str(error.response))
            exit(-1)

    def post_refresh(self, dash_id: str,
                     node_ids_to_refresh: list,
                     node_ids_in_viewport: list,
                     chart_id_to_form_data: dict,
                     cur_filter: list) -> int:
        refresh_url = f"{self.url_header}/dashboard/ace/{dash_id}/refresh"
        processed_chart_id_to_form_data = {}
        for chart_id in chart_id_to_form_data:
            form_data = chart_id_to_form_data[chart_id]
            processed_chart_id_to_form_data[chart_id] =\
                gen_request_chart_form_data(form_data, cur_filter)
        json_body = {
            "node_ids_to_refresh": node_ids_to_refresh,
            "node_ids_in_viewport": node_ids_in_viewport,
            "charts_form_data": processed_chart_id_to_form_data,
        }
        result = requests.post(refresh_url,
                               headers=self.headers,
                               json=json_body)
        try:
            result.raise_for_status()
        except HTTPError as error:
            print("Refresh Error: " + str(error.response))
            exit(-1)
        return int(json.loads(result.text)["result"]["ts"])

    def read_refreshed_charts(self, dash_id: int,
                              node_ids_to_read: list) -> dict:
        read_charts_url = f"{self.url_header}/dashboard/ace/{dash_id}/charts"
        json_body ={
            "node_ids_to_read": node_ids_to_read
        }
        result = requests.post(read_charts_url,
                               headers=self.headers,
                               json=json_body)
        try:
            result.raise_for_status()
        except HTTPError as error:
            print("Read Charts Error: " + str(error.response))
            exit(-1)
        return clean_read_results(json.loads(result.text)["result"])

    def clean_up(self, dash_id: str) -> None:
        try:
            delete_dash_state_url = f"{self.url_header}/dashboard/" \
                                    f"ace/{dash_id}/delete_ds_state"
            dash_state_result = requests.post(delete_dash_state_url,
                                              headers=self.headers)
            dash_state_result.raise_for_status()
        except HTTPError as error:
            print("Cleanup Error: " + str(error.response))
            exit(-1)
