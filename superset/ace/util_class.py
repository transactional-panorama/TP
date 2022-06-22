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

from enum import Enum
from typing import NamedTuple
from threading import Lock

START_TS = -1
RESPONSE_CODE = "response_code"
RESPONSE = "response"


class PropertyCombination(Enum):
    MV = 1
    MVCC = 2
    MCM = 3
    MCF = 4
    CMV = 5


class BaseVersion:
    def __init__(self, ts: int) -> None:
        self.ts = ts

    def equal_ts(self, other) -> bool:
        return self.ts == other.ts


class IV(BaseVersion):
    def __init__(self, ts: int) -> None:
        super().__init__(ts)
        self.code = 200

    def to_basic_type_dict(self) -> dict:
        return {"ts": self.ts,
                "version_result": "IV"}


class Version(BaseVersion):
    def __init__(self, ts: int, result: dict) -> None:
        super().__init__(ts)
        self.result = result

    def to_basic_type_dict(self) -> dict:
        return {"ts": self.ts,
                "version_result": self.result}


class NodeType(Enum):
    BASE_TABLE = 1
    FILTER = 2
    VIZ = 3


class Node:
    def __init__(self, node_id: int, node_type: NodeType):
        self.node_id = node_id
        self.node_type = node_type
        self.versions = list()
        self.local_lock = Lock()

    def add_iv(self, iv: IV):
        self.local_lock.acquire()
        self.versions.append(iv)
        self.local_lock.release()

    def add_version(self, version: Version):
        self.local_lock.acquire()
        iv_index = 0
        for iv_index in range(len(self.versions)):
            basic_version = self.versions[iv_index]
            if isinstance(basic_version, IV) and basic_version.ts == version.ts:
                break
        if iv_index < len(self.versions):
            del self.versions[iv_index]
            self.versions.insert(iv_index, version)
        else:
            self.versions.append(version)
        self.local_lock.release()

    def get_version_by_snapshot(self, ts: int) -> BaseVersion:
        self.local_lock.acquire()
        ret_version = None
        max_ts = START_TS - 1
        for version in self.versions:
            if max_ts < version.ts <= ts:
                ret_version = version
                max_ts = version.ts
        self.local_lock.release()
        return ret_version

    def get_visible_version(self) -> BaseVersion:
        self.local_lock.acquire()
        ret_version = None
        max_ts = START_TS - 1
        for version in self.versions:
            if version.ts > max_ts and isinstance(version, Version):
                ret_version = version
                max_ts = version.ts
        self.local_lock.release()
        return ret_version

    def clean_unused_versions(self, ts: int) -> None:
        self.local_lock.acquire()
        lower_bound = START_TS - 1
        for version in self.versions:
            if lower_bound < version.ts <= ts:
                lower_bound = version.ts
        self.versions = [ver for ver in self.versions if ver.ts <= lower_bound]
        self.local_lock.release()


class Dependency(NamedTuple):
    prec: Node
    dep: Node
