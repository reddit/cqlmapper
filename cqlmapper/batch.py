
import time
from datetime import datetime, timedelta

import six
from warnings import warn

from cqlmapper import TIMEOUT_NOT_SET, CQLEngineException, ConnectionInterface
from cqlmapper.statements import BaseCQLStatement


class Batch(ConnectionInterface):
    """
    Handles the batching of queries

    http://docs.datastax.com/en/cql/3.0/cql/cql_reference/batch_r.html

    See :doc:`/cqlengine/batches` for more details.
    """

    _consistency = None

    def __init__(self, conn, batch_type=None, timestamp=None, consistency=None,
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
        self.conn = conn
        self._executed = False
        self._context_entered = False
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

    def consistency(self, consistency):
        self.consistency = consistency

    def execute(self, query):
        if query.statement:
            self._add_query(query.statement)
        if query.cleanup_statement:
            self._add_query(query.cleanup_statement)

    def _add_query(self, query):
        if not isinstance(query, BaseCQLStatement):
            raise CQLEngineException(
                'only BaseCQLStatements can be added to a batch query'
            )
        self.queries.append(query)

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

    def _prepare(self):
        opener = 'BEGIN ' + (
            self.batch_type + ' ' if self.batch_type else ''
        ) + ' BATCH'
        if self.timestamp:
            if isinstance(self.timestamp, six.integer_types):
                ts = self.timestamp
            elif isinstance(self.timestamp, (datetime, timedelta)):
                ts = self.timestamp
                if isinstance(self.timestamp, timedelta):
                    ts += datetime.now()  # Apply timedelta
                ts = int(time.mktime(ts.timetuple()) * 1e+6 + ts.microsecond)
            else:
                raise ValueError(
                    "Batch expects a long, a timedelta, or a datetime"
                )

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

    def execute_batch(self):
        if self._executed:
            msg = "Batch executed multiple times."
            if self._context_entered:
                msg += (
                    " If using the batch as a context manager, there is no "
                    "need to call execute directly."
                )
            warn(msg)
        self._executed = True
        if len(self.queries) == 0:
            # Empty batch is a no-op except for callbacks which will be called
            # by __exit__
            if not self._context_entered:
                self._cleanup()
            return

        batch_args = self._prepare()
        if batch_args:
            statement, params, consistency, timeout = batch_args
            res = self.conn.execute(
                statement=statement,
                params=params,
                consistency_level=consistency,
                timeout=timeout,
                verify_applied=True,
            )

    def _execute_callbacks(self):
        for callback, args, kwargs in self._callbacks:
            callback(*args, **kwargs)

    def _cleanup(self):
        self.queries = []
        self._context_entered = False
        self._execute_callbacks()

    def __enter__(self):
        self._context_entered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # don't execute if there was an exception by default
        if exc_type is not None and not self._execute_on_exception:
            return
        self.execute_batch()
        self._cleanup()

