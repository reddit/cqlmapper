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

from cassandra.query import SimpleStatement, dict_factory

from cqlmapper import (
    ConnectionInterface,
    CQLEngineException,
    LWTException,
    TIMEOUT_NOT_SET,
)
from cqlmapper.batch import Batch
from cqlmapper.query import DMLQuery
from cqlmapper.statements import BaseCQLStatement


log = logging.getLogger(__name__)


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

    name = None
    hosts = None

    consistency = None
    retry_connect = False
    lazy_connect_lock = None
    cluster_options = None

    def __init__(self, conn, consistency=None, retry_connect=False,
                 cluster_options=None):
        self.consistency = consistency
        self.retry_connect = retry_connect
        self.cluster_options = cluster_options if cluster_options else {}
        self.cluster = conn.cluster
        self.session = conn
        self.keyspace = self.session.keyspace
        if self.consistency is not None:
            self.session.default_consistency_level = self.consistency
        self.session.row_factory = dict_factory
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
            key_values = query_statement.partition_key_values(
                query.model._partition_key_index
            )
            if not any(v is None for v in key_values):
                parts = query.model._routing_key_from_values(
                    key_values,
                    self.cluster.protocol_version
                )
                statement.routing_key = parts
                statement.keyspace = self.keyspace
        return statement, params

    def _excecute_dml_query(self, query):
        result = None
        if query.statement:
            statement, params = self._prepare_query_statement(
                query,
                query.statement
            )
            result = self.execute(
                statement=statement,
                params=params,
                timeout=query.timeout,
                verify_applied=query.check_applied,
            )
        if query.cleanup_statement:
            c_statement, c_params = self._prepare_query_statement(
                query,
                query.cleanup_statement
            )
            self.execute(
                statement=c_statement,
                params=c_params,
                timeout=query.timeout,
                verify_applied=query.check_applied,
            )
        return result

    def _execute_batch_query(self, batch):
        res = None
        has_error = False
        try:
            batch_args = batch.prepare()
            if batch_args:
                statement, params, consistency, timeout = batch_args
                res = self.execute(
                    statement=statement,
                    params=params,
                    consistency_level=consistency,
                    timeout=timeout,
                    verify_applied=True,
                )
        except Exception as e:
            if batch._execute_on_exception:
                batch.cleanup()
            raise
        batch.cleanup()
        return res

    # def execute_query(self, query):
    #     if isinstance(query, cqlmapper_query.BatchQuery):
    #         return self._execute_batch_query(query)
    #     elif isinstance(query, cqlmapper_query.DMLQuery):
    #         return self._excecute_dml_query(query)
    #     else:
    #         raise ValueError("Unexpected query type %s", type(query))

    def execute(self, statement_or_query, params=None, consistency_level=None,
                timeout=TIMEOUT_NOT_SET, verify_applied=False):
        if isinstance(statement_or_query, Batch):
            return self._execute_batch_query(statement_or_query)
        elif isinstance(statement_or_query, DMLQuery):
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
                statement_or_query,
                consistency_level=consistency_level,
            )
        else:
            raise ValueError(
                "Unexpected query type %s", type(statement_or_query)
            )

        log.debug(statement_or_query.query_string)

        result = self.session.execute(
            statement_or_query,
            params,
            timeout=timeout,
        )
        if verify_applied:
            check_applied(result)
        return result

