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

import six

from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.cluster import ExecutionProfile
from cassandra.cluster import UserTypeDoesNotExist
from cassandra.query import dict_factory
from cassandra.query import SimpleStatement

from cqlmapper import ConnectionInterface
from cqlmapper import CQLEngineException
from cqlmapper import LWTException
from cqlmapper import TIMEOUT_NOT_SET
from cqlmapper.query import DMLQuery
from cqlmapper.statements import BaseCQLStatement


log = logging.getLogger(__name__)

EXEC_PROFILE_CQLMAPPER = "_cqlmapper"


class UndefinedKeyspaceException(CQLEngineException):
    pass


def check_applied(result):
    """
    Raises LWTException if it looks like a failed LWT request.
    """
    try:
        applied = result.was_applied
    except Exception:
        applied = True  # result was not LWT form
    if not applied:
        raise LWTException(result[0])


class Connection(ConnectionInterface):
    """CQLEngine Connection"""

    def __init__(self, conn):
        self.cluster = conn.cluster
        self.session = conn
        self.keyspace = self.session.keyspace
        if EXEC_PROFILE_CQLMAPPER not in self.cluster.profile_manager.profiles:
            default_profile = self.session.get_execution_profile(EXEC_PROFILE_DEFAULT)
            profile = self.session.execution_profile_clone_update(
                default_profile, row_factory=dict_factory
            )
            self.cluster.add_execution_profile(EXEC_PROFILE_CQLMAPPER, profile)
        enc = self.session.encoder
        enc.mapping[tuple] = enc.cql_encode_tuple

    def _prepare_query_statement(self, query, query_statement):
        params = query_statement.get_context()
        statement = SimpleStatement(
            str(query_statement),
            consistency_level=query.consistency,
            fetch_size=query_statement.fetch_size,
        )
        if query.model._partition_key_index:
            key_values = query_statement.partition_key_values(query.model._partition_key_index)
            if not any(v is None for v in key_values):
                parts = query.model._routing_key_from_values(
                    key_values, self.cluster.protocol_version
                )
                statement.routing_key = parts
                statement.keyspace = self.keyspace
        return statement, params

    def _excecute_dml_query(self, query):
        result = None
        if query.statement:
            statement, params = self._prepare_query_statement(query, query.statement)
            result = self.execute(
                statement, params=params, timeout=query.timeout, verify_applied=query.check_applied
            )
        if query.cleanup_statement:
            c_statement, c_params = self._prepare_query_statement(query, query.cleanup_statement)
            self.execute(
                c_statement,
                params=c_params,
                timeout=query.timeout,
                verify_applied=query.check_applied,
            )
        return result

    def execute(
        self,
        statement_or_query,
        params=None,
        consistency_level=None,
        timeout=TIMEOUT_NOT_SET,
        verify_applied=False,
    ):
        if isinstance(statement_or_query, DMLQuery):
            return self._excecute_dml_query(statement_or_query)
        elif isinstance(statement_or_query, SimpleStatement):
            pass
        elif isinstance(statement_or_query, BaseCQLStatement):
            params = statement_or_query.get_context()
            statement_or_query = SimpleStatement(
                str(statement_or_query),
                consistency_level=consistency_level,
                fetch_size=statement_or_query.fetch_size,
            )
        elif isinstance(statement_or_query, six.string_types):
            statement_or_query = SimpleStatement(
                statement_or_query, consistency_level=consistency_level
            )
        else:
            raise ValueError("Unexpected query type %s", type(statement_or_query))

        result = self.session.execute(
            statement_or_query, params, timeout=timeout, execution_profile=EXEC_PROFILE_CQLMAPPER
        )
        if verify_applied:
            check_applied(result)
        return result

    def register_udt(self, type_name, klass):
        try:
            self.cluster.register_user_type(self.keyspace, type_name, klass)
        except UserTypeDoesNotExist:
            pass  # new types are covered in management sync functions
