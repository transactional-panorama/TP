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
from urllib.error import HTTPError


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
        get_dash_info_url = f"{self.url_header}/dashboard"
        dash_info_result = requests.get(get_dash_info_url, headers=self.headers)
        for dash_info in json.loads(dash_info_result.text)["result"]:
            dash_id = dash_info["id"]
            dash_title = dash_info["dashboard_title"]
            self.dash_title_to_id[dash_title] = dash_id

    def config_simulation(self, dash_id: str) -> None:
        try:
            # create ace state
            create_dash_state_url = f"{self.url_header}/dashboard/ace/" \
                                    f"{dash_id}/create_ds_state "
            dash_state_result = requests.post(create_dash_state_url,
                                              headers=self.headers,
                                              json={})
            dash_state_result.raise_for_status()

            # set mvc properties
            mvc_properties_url = "{url_header}/dashboard/ace/{dash_id}/properties" \
                .format(url_header=self.url_header, dash_id=str(dash_id))
            json_body = {
                "mvc_properties": self.mvc_properties,
            }
            result = requests.post(mvc_properties_url,
                                   headers=self.headers,
                                   json=json_body)
            result.raise_for_status()
        except HTTPError as error:
            print("Config Error: " + str(error))
            exit(-1)

    def simulate_next_step(self) -> bool:
        return False

    def write_report_results(self) -> None:
        pass

    def clean_up(self, dash_id: str) -> None:
        try:
            delete_dash_state_url = f"{self.url_header}/dashboard/ace/{dash_id}/delete"
            dash_state_result = requests.post(delete_dash_state_url,
                                              headers=self.headers)
            dash_state_result.raise_for_status()
        except HTTPError as error:
            print("Cleanup Error: " + str(error))
            exit(-1)
