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

from superset.ace_driver.dash_behavior.tpch_dash_behavior import TPCHDashBehavior


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
        self.tpch_behavior = TPCHDashBehavior(server_addr, username,
                                              password, read_behavior,
                                              write_behavior, refresh_interval,
                                              num_refresh, mvc_property,
                                              opt_viewport, opt_exec_time,
                                              opt_skip_write, stat_dir)
        self.tpch_behavior.setup()

    def start_test(self):
        self.tpch_behavior.run_test()

    def report_results(self):
        self.tpch_behavior.write_report_results()
        self.tpch_behavior.clean_up_dashboard()
