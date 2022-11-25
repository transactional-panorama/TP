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
                 dashboard_title: str,
                 viewport_range: int,
                 shift_step: int,
                 explore_range: int,
                 read_behavior: str,
                 viewport_start: int,
                 write_behavior: str,
                 refresh_interval: int,
                 num_refresh: int,
                 mvc_property: int,
                 k_relaxed: int,
                 opt_viewport: bool,
                 opt_exec_time: bool,
                 opt_metrics: bool,
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
        self.tpch_behavior = TPCHDashBehavior(server_addr, username,
                                              password, dashboard_title, viewport_range,
                                              shift_step, explore_range, read_behavior,
                                              viewport_start, write_behavior,
                                              refresh_interval, num_refresh,
                                              mvc_property, k_relaxed, opt_viewport,
                                              opt_exec_time, opt_metrics, opt_skip_write,
                                              enable_stats_cache, enable_iv_sl_log,
                                              stat_dir, db_name,
                                              db_username, db_password, db_host, db_port, sf)
        self.tpch_behavior.setup()

    def start_test(self):
        self.tpch_behavior.run_test()

    def report_results(self):
        self.tpch_behavior.write_report_results()
        self.tpch_behavior.clean_up_dashboard()
