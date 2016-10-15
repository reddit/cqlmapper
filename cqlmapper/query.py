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

from datetime import datetime, timedelta
import time

import six
from warnings import warn

from cqlmapper import TIMEOUT_NOT_SET
from cqlmapper import (
    columns,
    CQLEngineException,
    UnicodeMixin,
)
from cqlmapper.functions import QueryValue
from cqlmapper.operators import (
    InOperator,
    EqualsOperator,
    GreaterThanOperator,
    GreaterThanOrEqualOperator,
    LessThanOperator,
    LessThanOrEqualOperator,
    ContainsOperator,
)
from cqlmapper.statements import (
    WhereClause,
    DeleteStatement,
    UpdateStatement,
    InsertStatement,
    BaseCQLStatement,
    MapDeleteClause,
)


class QueryException(CQLEngineException):
    pass


class IfNotExistsWithCounterColumn(CQLEngineException):
    pass


class IfExistsWithCounterColumn(CQLEngineException):
    pass


class AbstractQueryableColumn(UnicodeMixin):
    """
    exposes cql query operators through pythons
    builtin comparator symbols
    """

    def _get_column(self):
        raise NotImplementedError

    def __unicode__(self):
        raise NotImplementedError

    def _to_database(self, val):
        if isinstance(val, QueryValue):
            return val
        else:
            return self._get_column().to_database(val)

    def in_(self, item):
        """
        Returns an in operator

        used where you'd typically want to use python's `in` operator
        """
        return WhereClause(six.text_type(self), InOperator(), item)

    def contains_(self, item):
        """
        Returns a CONTAINS operator
        """
        return WhereClause(six.text_type(self), ContainsOperator(), item)

    def __eq__(self, other):
        return WhereClause(
            six.text_type(self),
            EqualsOperator(),
            self._to_database(other),
        )

    def __gt__(self, other):
        return WhereClause(
            six.text_type(self),
            GreaterThanOperator(),
            self._to_database(other),
        )

    def __ge__(self, other):
        return WhereClause(
            six.text_type(self),
            GreaterThanOrEqualOperator(),
            self._to_database(other),
        )

    def __lt__(self, other):
        return WhereClause(
            six.text_type(self),
            LessThanOperator(),
            self._to_database(other),
        )

    def __le__(self, other):
        return WhereClause(
            six.text_type(self),
            LessThanOrEqualOperator(),
            self._to_database(other),
        )


class BatchType(object):
    Unlogged = 'UNLOGGED'
    Counter = 'COUNTER'


class BatchQuery(object):
    """
    Handles the batching of queries

    http://docs.datastax.com/en/cql/3.0/cql/cql_reference/batch_r.html

    See :doc:`/cqlengine/batches` for more details.
    """

    _consistency = None

    def __init__(self, batch_type=None, timestamp=None, consistency=None,
                 execute_on_exception=False, timeout=TIMEOUT_NOT_SET):
        """
        :param batch_type: (optional) One of batch type values available
            through BatchType enum
        :type batch_type: str or None
        :param timestamp: (optional) A datetime or timedelta object with
            desired timestamp to be applied to the batch conditional.
        :type timestamp: datetime or timedelta or None
        :param consistency: (optional) One of consistency values ("ANY",
            "ONE", "QUORUM" etc)
        :type consistency: The :class:`.ConsistencyLevel` to be used for the
            batch query, or None.
        :param execute_on_exception: (Defaults to False) Indicates that when
            the BatchQuery instance is used as a context manager the queries
            accumulated within the context must be executed despite
            encountering an error within the context. By default, any
            exception raised from within the context scope will cause the
            batched queries not to be executed.
        :type execute_on_exception: bool
        :param timeout: (optional) Timeout for the entire batch (in seconds),
            if not specified fallback to default session timeout
        :type timeout: float or None
        """
        self.prepared = False
        self.cleaned_up = False
        self.queries = []
        self.batch_type = batch_type
        if timestamp is not None and not isinstance(timestamp, (datetime, timedelta)):
            raise CQLEngineException(
                'timestamp object must be an instance of datetime'
            )
        self.timestamp = timestamp
        self.consistency = consistency
        self._execute_on_exception = execute_on_exception
        self.timeout = timeout
        self._callbacks = []

    def add(self, query):
        if not isinstance(query, BaseCQLStatement):
            raise CQLEngineException(
                'only BaseCQLStatements can be added to a batch query'
            )
        self.queries.append(query)

    def consistency(self, consistency):
        self.consistency = consistency

    def _execute_callbacks(self):
        for callback, args, kwargs in self._callbacks:
            callback(*args, **kwargs)

    def add_callback(self, fn, *args, **kwargs):
        """Add a function and arguments to be passed to it to be executed
        after the batch executes.

        A batch can support multiple callbacks.

        Note, that if the batch does not execute, the callbacks are not
        executed. A callback, thus, is an "on batch success" handler.

        :param fn: Callable object
        :type fn: callable
        :param \*args: Positional arguments to be passed to the callback at
            the time of execution
        :param \*\*kwargs: Named arguments to be passed to the callback at the
            time of execution
        """
        if not callable(fn):
            raise ValueError(
                "Value for argument 'fn' is {0} and is not a callable "
                "object.".format(type(fn))
            )
        self._callbacks.append((fn, args, kwargs))

    def prepare(self):
        if len(self.queries) == 0:
            # Empty batch is a no-op except for callbacks which will be called
            # by __exit__
            return
        self.prepared = True

        opener = 'BEGIN ' + (self.batch_type + ' ' if self.batch_type else '') + ' BATCH'
        if self.timestamp:
            if isinstance(self.timestamp, six.integer_types):
                ts = self.timestamp
            elif isinstance(self.timestamp, (datetime, timedelta)):
                ts = self.timestamp
                if isinstance(self.timestamp, timedelta):
                    ts += datetime.now()  # Apply timedelta
                ts = int(time.mktime(ts.timetuple()) * 1e+6 + ts.microsecond)
            else:
                raise ValueError("Batch expects a long, a timedelta, or a datetime")

            opener += ' USING TIMESTAMP {0}'.format(ts)

        query_list = [opener]
        parameters = {}
        ctx_counter = 0
        for query in self.queries:
            query.update_context_id(ctx_counter)
            ctx = query.get_context()
            ctx_counter += len(ctx)
            query_list.append('  ' + str(query))
            parameters.update(ctx)

        query_list.append('APPLY BATCH;')

        return (
            '\n'.join(query_list),
            parameters,
            self.consistency,
            self.timeout,
        )

    def cleanup(self):
        if self.cleaned_up:
            warn("Batch executed multiple times.")
        self.cleaned_up = True
        self.queries = []
        self._execute_callbacks()


class DMLQuery(object):
    """
    A query object used for queries performing inserts, updates, or deletes

    this is usually instantiated by the model instance to be modified

    unlike the read query object, this is mutable
    """
    _ttl = None
    _consistency = None
    _timestamp = None
    _if_not_exists = False
    _if_exists = False

    def __init__(self, model, instance=None, ttl=None,
                 consistency=None, timestamp=None, if_not_exists=False,
                 conditional=None, timeout=TIMEOUT_NOT_SET, if_exists=False):
        self.model = model
        self.column_family_name = self.model.column_family_name()
        self.instance = instance
        self.cleanup_statement = None
        self.statement = None
        self._ttl = ttl
        self.consistency = consistency
        self._timestamp = timestamp
        self._if_not_exists = if_not_exists
        self._if_exists = if_exists
        self._conditional = conditional
        self.timeout = timeout
        self.prepare()

    @property
    def check_applied(self):
        return self._if_not_exists or self._if_exists or self._conditional

    def prepare(self):
        raise NotImplementedError

    def set_delete_null_columns(self, conditionals=None):
        """Set self.cleanup_statement to delete query to remove columns that
        have changed to null.
        """
        ds = DeleteStatement(
            self.column_family_name,
            conditionals=conditionals,
            if_exists=self._if_exists,
        )
        deleted_fields = False
        static_only = True
        for _, v in self.instance._values.items():
            col = v.column
            if v.deleted:
                ds.add_field(col.db_field_name)
                deleted_fields = True
                static_only &= col.static
            elif isinstance(col, columns.Map):
                uc = MapDeleteClause(
                    col.db_field_name,
                    v.value,
                    v.previous_value,
                )
                if uc.get_context_size() > 0:
                    ds.add_field(uc)
                    deleted_fields = True
                    static_only |= col.static

        if deleted_fields:
            keys = self.model._partition_keys if static_only else self.model._primary_keys
            for name, col in keys.items():
                ds.add_where(
                    col,
                    EqualsOperator(),
                    getattr(self.instance, name),
                )
            self.cleanup_statement = ds


class UpdateDMLQuery(DMLQuery):
    """
    updates a row.
    This is a blind update call.
    All validation and cleaning needs to happen
    prior to creating this.
    """

    def prepare(self):
        if self.instance is None:
            raise CQLEngineException("DML Query intance attribute is None")

        assert type(self.instance) == self.model
        null_clustering_key = False if len(self.instance._clustering_keys) == 0 else True
        static_changed_only = True
        statement = UpdateStatement(
            self.column_family_name,
            ttl=self._ttl,
            timestamp=self._timestamp,
            conditionals=self._conditional,
            if_exists=self._if_exists,
        )
        for name, col in self.instance._clustering_keys.items():
            null_clustering_key = (
                null_clustering_key and
                col._val_is_null(getattr(self.instance, name, None))
            )

        updated_columns = set()
        # get defined fields and their column names
        for name, col in self.model._columns.items():
            # if clustering key is null, don't include non static columns
            if null_clustering_key and not col.static and not col.partition_key:
                continue
            if not col.is_primary_key:
                val = getattr(self.instance, name, None)
                val_mgr = self.instance._values[name]

                if val is None:
                    continue

                if not val_mgr.changed and not isinstance(col, columns.Counter):
                    continue

                static_changed_only = static_changed_only and col.static
                statement.add_update(col, val, previous=val_mgr.previous_value)
                updated_columns.add(col.db_field_name)

        if not null_clustering_key:
            # remove conditions on fields that have been updated
            delete_conditionals = [
                condition for condition in self._conditional if
                condition.field not in updated_columns
            ] if self._conditional else None
            self.set_delete_null_columns(delete_conditionals)

        if statement.assignments:
            for name, col in self.model._primary_keys.items():
                # only include clustering key if clustering key is not null, and non static columns are changed to avoid cql error
                if (null_clustering_key or static_changed_only) and (not col.partition_key):
                    continue
                statement.add_where(col, EqualsOperator(), getattr(self.instance, name))
            self.statement = statement


class SaveDMLQuery(DMLQuery):
    """
    Creates / updates a row.
    This is a blind insert call.
    All validation and cleaning needs to happen
    prior to creating this.
    """

    def prepare(self):
        if self.instance is None:
            raise CQLEngineException("DML Query intance attribute is None")
        assert type(self.instance) == self.model

        nulled_fields = set()
        if self.instance._has_counter:
            raise Exception(
                "'create' and 'save' actions on Counters is not supported. "
                "Use the 'update' mechanism instead."
            )

        insert = InsertStatement(
            self.column_family_name,
            ttl=self._ttl,
            timestamp=self._timestamp,
            if_not_exists=self._if_not_exists,
        )
        static_save_only = len(self.instance._clustering_keys) != 0
        for name, col in self.instance._clustering_keys.items():
            static_save_only = (
                static_save_only and
                col._val_is_null(getattr(self.instance, name, None))
            )
        for name, col in self.instance._columns.items():
            if static_save_only and not col.static and not col.partition_key:
                continue
            val = getattr(self.instance, name, None)
            if col._val_is_null(val):
                if self.instance._values[name].changed:
                    nulled_fields.add(col.db_field_name)
                continue
            insert.add_assignment(col, getattr(self.instance, name, None))

        # set cleanup to delete any nulled columns
        if not static_save_only:
            self.set_delete_null_columns()

        # skip query execution if it's empty
        # caused by pointless update queries
        if not insert.is_empty:
            self.statement = insert


class DeleteDMLQuery(DMLQuery):
    """ Deletes one instance """

    def prepare(self):
        if self.instance is None:
            raise CQLEngineException("DML Query instance attribute is None")

        ds = DeleteStatement(
            self.column_family_name,
            timestamp=self._timestamp,
            conditionals=self._conditional,
            if_exists=self._if_exists,
        )
        for name, col in self.model._primary_keys.items():
            val = getattr(self.instance, name)
            if val is None and not col.partition_key:
                continue
            ds.add_where(col, EqualsOperator(), val)
        self.statement = ds
