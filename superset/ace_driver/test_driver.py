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

import argparse
from superset.ace_driver.test_ace import TestACE

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Test Driver for ACE")
    parser.add_argument('--server_addr',
                        help='the address of the superset server',
                        required=True)
    parser.add_argument('--username',
                        help='the user name',
                        required=True)
    parser.add_argument('--password',
                        help='the password',
                        required=True)
    parser.add_argument('--dashboard',
                        help='the dashboard',
                        required=True)
    parser.add_argument('--read_behavior',
                        help='simulated behavior of a user reading the dashboard',
                        required=True)
    parser.add_argument('--write_behavior',
                        help='simulated behavior of a user or'
                             'an external system modifying the dashboard',
                        required=True)
    parser.add_argument('--refresh_interval',
                        help='the number of seconds for'
                             'refreshing the dashboard',
                        action="store_const",
                        default=30)
    parser.add_argument('--num_refresh',
                        help='the number of refreshes a test involves',
                        action='store_const',
                        default=1)
    parser.add_argument('--mvc_property',
                        help='the mvc property we need to set',
                        action='store_const',
                        default=1)
    parser.add_argument('--opt_viewport',
                        help='whether we turn on the optimization that the scheduling'
                             'considers the different amount of time a user spends on'
                             'different views',
                        action='store_true',
                        default=False)
    parser.add_argument('--opt_exec_time',
                        help='whether we turn on the optimization that the scheduling'
                             'considers the different amount of execution time for'
                             'refreshing a view',
                        action='store_true',
                        default=False)
    parser.add_argument('--opt_skip_write',
                        help='whether we turn on the optimization that the scheduling'
                             'skips refreshing a view that will be refreshed'
                             'by later writes',
                        action='store_true',
                        default=False)
    parser.add_argument('--stat_dir',
                        help='the dir for reporting the test results',
                        required=True)
    parser.add_argument('--db_name',
                        help='database name',
                        required=True)
    parser.add_argument('--db_username',
                        help='the username for the database',
                        required=True)
    parser.add_argument('--db_password',
                        help='the password for the database user',
                        required=True)
    parser.add_argument('--db_host',
                        help='the database host address',
                        required=True)
    parser.add_argument('--db_port',
                        help='the database host port',
                        required=True)

    args = parser.parse_args()
    testACE = TestACE(server_addr=args.server_addr,
                      username=args.username,
                      password=args.password,
                      dashboard_title=args.dashboard,
                      read_behavior=args.read_behavior,
                      write_behavior=args.write_behavior,
                      refresh_interval=args.refresh_interval,
                      num_refresh=args.num_refresh,
                      mvc_property=args.mvc_property,
                      opt_viewport=args.opt_viewport,
                      opt_exec_time=args.opt_exec_time,
                      opt_skip_write=args.opt_skip_write,
                      stat_dir=args.stat_dir,
                      db_name=args.db_name,
                      db_username=args.db_username,
                      db_password=args.db_password,
                      db_host=args.db_host,
                      db_port=args.db_port
                      )
    testACE.start_test()
    testACE.report_results()
