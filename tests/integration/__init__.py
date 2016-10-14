# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import sys
import time
import traceback
import warnings
from itertools import groupby
from subprocess import call

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from cassandra import (
    ConsistencyLevel,
    OperationTimedOut,
    ReadTimeout,
    ReadFailure,
    WriteTimeout,
    WriteFailure,
    AlreadyExists
)
from cassandra.cluster import Cluster
from cassandra.protocol import ConfigurationException
from cassandra.policies import RoundRobinPolicy

from cqlmapper import connection
from cqlmapper.management import (
    create_keyspace_simple,
    CQLENG_ALLOW_SCHEMA_MANAGEMENT,
)
import cassandra

try:
    from ccmlib.cluster import Cluster as CCMCluster
    from ccmlib.dse_cluster import DseCluster
    from ccmlib.cluster_factory import ClusterFactory as CCMClusterFactory
    from ccmlib import common
except ImportError as e:
    CCMClusterFactory = None

_connections = {}

DEFAULT_KEYSPACE = 'cqlengine_test'
SINGLE_NODE_CLUSTER_NAME = 'single_node'
CASSANDRA_VERSION = "2.2.7"
CQL_SKIP_EXECUTE = bool(os.getenv('CQL_SKIP_EXECUTE', False))
CCM_CLUSTER = None
CCM_KWARGS = {}
CCM_KWARGS['version'] = CASSANDRA_VERSION
PROTOCOL_VERSION = 4

path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ccm')
if not os.path.exists(path):
    os.mkdir(path)

cass_version = None
cql_version = None
log = logging.getLogger(__name__)


def get_server_versions():
    """
    Probe system.local table to determine Cassandra and CQL version.
    Returns a tuple of (cassandra_version, cql_version).
    """
    global cass_version, cql_version

    if cass_version is not None:
        return (cass_version, cql_version)

    c = Cluster()
    s = c.connect()
    row = s.execute(
        'SELECT cql_version, release_version FROM system.local'
    )[0]

    cass_version = _tuple_version(row.release_version)
    cql_version = _tuple_version(row.cql_version)

    c.shutdown()

    return (cass_version, cql_version)


def _tuple_version(version_string):
    if '-' in version_string:
        version_string = version_string[:version_string.index('-')]

    return tuple([int(p) for p in version_string.split('.')])


def setup_package():
    # for testing warnings, make sure all are let through
    warnings.simplefilter('always')
    os.environ[CQLENG_ALLOW_SCHEMA_MANAGEMENT] = '1'

    conn = get_connection(keyspace_name=None)
    create_keyspace_simple(conn, DEFAULT_KEYSPACE, 1)


def get_connection(keyspace_name=DEFAULT_KEYSPACE):
    global _connections
    if keyspace_name not in _connections:

        c = Cluster(
            contact_points=['127.0.0.1'],
            protocol_version=PROTOCOL_VERSION,
        )
        session = c.connect(keyspace_name)
        _connections[keyspace_name] = connection.Connection(
            conn=session,
            consistency=ConsistencyLevel.ONE
        )
    return _connections[keyspace_name]


def is_prepend_reversed():
    # do we have https://issues.apache.org/jira/browse/CASSANDRA-8733 ?
    ver, _ = get_server_versions()
    return not (ver >= (2, 0, 13) or ver >= (2, 1, 3))


class StatementCounter(object):
    """
    Simple python object used to hold a count of the number of times
    the wrapped method has been invoked
    """
    def __init__(self, patched_func):
        self.func = patched_func
        self.counter = 0

    def wrapped_execute(self, *args, **kwargs):
        self.counter += 1
        return self.func(*args, **kwargs)

    def get_counter(self):
        return self.counter


# def execute_count(expected):
#     """
#     A decorator used wrap cqlmapper.connection.execute. It counts
#     the number of times this method is invoked then compares it to the number
#     expected. If they don't match it throws an assertion error. This function
#     can be disabled by running the test harness with the env variable
#     CQL_SKIP_EXECUTE=1 set
#     """
#     def innerCounter(fn):
#         def wrapped_function(*args, **kwargs):
#             # Create a counter monkey patch into cqlmapper.connection.execute
#             count = StatementCounter(cqlmapper.connection.execute)
#             original_function = cqlmapper.connection.execute
#             # Monkey patch in our StatementCounter wrapper
#             cqlmapper.connection.execute = count.wrapped_execute
#             # Invoked the underlying unit test
#             to_return = fn(*args, **kwargs)
#             # Get the count from our monkey patched counter
#             count.get_counter()
#             # DeMonkey Patch our code
#             cqlmapper.connection.execute = original_function
#             # Check to see if we have a pre-existing test case to work from.
#             if len(args) is 0:
#                 test_case = unittest.TestCase("__init__")
#             else:
#                 test_case = args[0]
#             # Check to see if the count is what you expect
#             test_case.assertEqual(count.get_counter(), expected, msg="Expected number of cqlmapper.connection.execute calls ({0}) doesn't match actual number invoked ({1})".format(expected, count.get_counter()))
#             return to_return
#         # Name of the wrapped function must match the original or unittest will error out.
#         wrapped_function.__name__ = fn.__name__
#         wrapped_function.__doc__ = fn.__doc__
#         # Escape hatch
#         if(CQL_SKIP_EXECUTE):
#             return fn
#         else:
#             return wrapped_function

#     return innerCounter

