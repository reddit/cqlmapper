# Modifications Copyright 2016-2017 Reddit, Inc.
#
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
import platform
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
    OperationTimedOut,
    ReadTimeout,
    ReadFailure,
    WriteTimeout,
    WriteFailure,
    AlreadyExists,
)
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

from cqlmapper import connection
from cqlmapper.management import create_keyspace_simple, CQLENG_ALLOW_SCHEMA_MANAGEMENT
import cassandra


_connections = {}

DEFAULT_KEYSPACE = "cqlengine_test"
CQL_SKIP_EXECUTE = bool(os.getenv("CQL_SKIP_EXECUTE", False))
CASSANDRA_VERSION = "2.2.7"
PROTOCOL_VERSION = 4

cass_version = None
cql_version = None
log = logging.getLogger(__name__)

pypy = unittest.skipUnless(
    platform.python_implementation() == "PyPy", "Test is skipped unless it's on PyPy"
)


def get_server_versions():
    """
    Probe system.local table to determine Cassandra and CQL version.
    Returns a tuple of (cassandra_version, cql_version).
    """
    global cass_version, cql_version

    if cass_version is not None:
        return (cass_version, cql_version)

    c = Cluster(["cassandra"])
    s = c.connect()
    row = s.execute("SELECT cql_version, release_version FROM system.local")[0]

    cass_version = _tuple_version(row.release_version)
    cql_version = _tuple_version(row.cql_version)

    c.shutdown()

    return (cass_version, cql_version)


def _tuple_version(version_string):
    if "-" in version_string:
        version_string = version_string[: version_string.index("-")]

    return tuple([int(p) for p in version_string.split(".")])


def setup_package():
    # for testing warnings, make sure all are let through
    warnings.simplefilter("always")
    os.environ[CQLENG_ALLOW_SCHEMA_MANAGEMENT] = "1"

    conn = get_connection(keyspace_name=None)
    create_keyspace_simple(conn, DEFAULT_KEYSPACE, 1)


def get_connection(keyspace_name=DEFAULT_KEYSPACE, attempts=5):
    global _connections
    if keyspace_name not in _connections:

        c = Cluster(contact_points=["cassandra"], protocol_version=PROTOCOL_VERSION)
        for _ in range(attempts):
            try:
                session = c.connect(keyspace_name)
                _connections[keyspace_name] = connection.Connection(session)
            except Exception:
                time.sleep(1)
            else:
                break
        else:
            raise Exception("Could not connect to cassandra")
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


class MockLoggingHandler(logging.Handler):
    """Mock logging handler to check for expected logs."""

    def __init__(self, *args, **kwargs):
        self.reset()
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.messages[record.levelname.lower()].append(record.getMessage())

    def reset(self):
        self.messages = {"debug": [], "info": [], "warning": [], "error": [], "critical": []}

    def get_message_count(self, level, sub_string):
        count = 0
        for msg in self.messages.get(level):
            if sub_string in msg:
                count += 1
        return count


def execute_count(expected):
    """
    A decorator used wrap cqlmapper.connection.execute. It counts
    the number of times this method is invoked then compares it to the number
    expected. If they don't match it throws an assertion error. This function
    can be disabled by running the test harness with the env variable
    CQL_SKIP_EXECUTE=1 set
    """

    def innerCounter(fn):
        def wrapped_function(*args, **kwargs):
            self = args[0]
            # Create a counter monkey patch into cqlmapper.connection.execute
            count = StatementCounter(self.conn.session.execute)
            original_function = self.conn.session.execute
            # Monkey patch in our StatementCounter wrapper
            self.conn.session.execute = count.wrapped_execute
            try:
                # Invoked the underlying unit test
                to_return = fn(*args, **kwargs)
                # Get the count from our monkey patched counter
                count.get_counter()
            finally:
                # DeMonkey Patch our code
                self.conn.session.execute = original_function
            # Check to see if the count is what you expect
            self.assertEqual(
                count.get_counter(),
                expected,
                msg=(
                    "Expected number of self.conn.execute calls ({0}) "
                    "doesn't match actual number invoked ({1})".format(
                        expected, count.get_counter()
                    )
                ),
            )
            return to_return

        # Name of the wrapped function must match the original or unittest will error out.
        wrapped_function.__name__ = fn.__name__
        wrapped_function.__doc__ = fn.__doc__
        # Escape hatch
        if CQL_SKIP_EXECUTE:
            return fn
        else:
            return wrapped_function

    return innerCounter
