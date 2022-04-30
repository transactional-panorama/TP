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
import pprint


class TestACE:
    def __init__(self, server_addr: str,
                 username: str,
                 password: str,
                 dashboard: str,
                 read_behavior: str,
                 write_behavior: str,
                 refresh_interval: int,
                 num_refresh: int,
                 mvc_property: int,
                 opt_viewport: bool,
                 opt_exec_time: bool,
                 opt_skip_write: bool,
                 stat_dir: str):
        self.server_addr = server_addr
        self.username = username
        self.password = password
        self.dashboard = dashboard
        self.read_behavior = read_behavior
        self.write_behavior = write_behavior
        self.refresh_interval = refresh_interval
        self.num_refresh = num_refresh
        self.mvc_property = mvc_property
        self.opt_viewport = opt_viewport
        self.opt_exec_time = opt_exec_time
        self.opt_skip_write = opt_skip_write
        self.stat_dir = stat_dir

    def start_test(self):
        pass

    def report_results(self):
        pass
