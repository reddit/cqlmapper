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
import copy

from functools import partial

import six

from cqlmapper import columns
from cqlmapper import CQLEngineException
from cqlmapper import TIMEOUT_NOT_SET
from cqlmapper import ValidationError
from cqlmapper.functions import BaseQueryFunction
from cqlmapper.functions import Token
from cqlmapper.operators import BaseWhereOperator
from cqlmapper.operators import ContainsOperator
from cqlmapper.operators import EqualsOperator
from cqlmapper.operators import InOperator
from cqlmapper.query import IfExistsWithCounterColumn
from cqlmapper.query import IfNotExistsWithCounterColumn
from cqlmapper.query import QueryException
from cqlmapper.statements import ConditionalClause
from cqlmapper.statements import DeleteStatement
from cqlmapper.statements import SelectStatement
from cqlmapper.statements import UpdateStatement
from cqlmapper.statements import WhereClause


class DoesNotExist(QueryException):
    pass


class MultipleObjectsReturned(QueryException):
    pass


class ModelQuerySet(object):
    def __init__(self, model):
        super(ModelQuerySet, self).__init__()
        self.model = model

        # Where clause filters
        self._where = []

        # Conditional clause filters
        self._conditional = []

        # ordering arguments
        self._order = []

        self._allow_filtering = False

        # CQL has a default limit of 10000, it's defined here
        # because explicit is better than implicit
        self._limit = 10000

        # see the defer and only methods
        self._defer_fields = set()
        self._deferred_values = {}
        self._only_fields = []

        self._values_list = False
        self._flat_values_list = False

        # results cache
        self._result_cache = None
        self._result_idx = None
        self._result_generator = None
        self._materialize_results = True

        self._distinct_fields = None

        self._count = None

        self._ttl = None
        self._consistency = None
        self._timestamp = None
        self._if_not_exists = False
        self._timeout = TIMEOUT_NOT_SET
        self._if_exists = False
        self._fetch_size = None
        self._connection = None

    @property
    def check_applied(self):
        """Should the current query check if it was applied"""
        return self._if_not_exists or self._if_exists or self._conditional

    @property
    def column_family_name(self):
        """Shortcut for the model column family name"""
        return self.model.column_family_name()

    def _execute_statement(self, conn, statement):
        """Execute the given CQL statement with the given connection.

        :param conn: Cassandra connection wrapper used to execute the CQL
            statement.
        :type: cqlengine.ConnectionInterface subclass
        :param: statement
        :type: DMLQuery or BaseCQLStatement or string
        """
        return conn.execute(
            statement,
            consistency_level=self._consistency,
            timeout=self._timeout,
            verify_applied=self.check_applied,
        )

    def __unicode__(self):
        return six.text_type(self._select_query())

    def __str__(self):
        return str(self.__unicode__())

    def __call__(self, **kwargs):
        return self.filter(**kwargs)

    def __deepcopy__(self, memo):
        clone = self.__class__(self.model)
        for k, v in self.__dict__.items():
            if k in [
                "_con",
                "_cur",
                "_result_cache",
                "_result_idx",
                "_result_generator",
                "_construct_result",
            ]:  # don't clone these, which are per-request-execution
                clone.__dict__[k] = None
            elif k == "_timeout":
                clone.__dict__[k] = self._timeout
            else:
                clone.__dict__[k] = copy.deepcopy(v, memo)

        return clone

    def _select_fields(self):
        """Returns the fields to select."""
        if self._defer_fields or self._only_fields:
            fields = self.model._columns.keys()
            if self._defer_fields:
                fields = [f for f in fields if f not in self._defer_fields]
                # select the partition keys if all model fields are set defer
                if not fields:
                    fields = self.model._partition_keys
            if self._only_fields:
                fields = [f for f in fields if f in self._only_fields]
            if not fields:
                raise QueryException(
                    'No fields in select query. Only fields: "{0}", defer '
                    'fields: "{1}"'.format(
                        ",".join(self._only_fields), ",".join(self._defer_fields)
                    )
                )
            return [self.model._columns[f].db_field_name for f in fields]
        return []

    def _validate_select_where(self):
        """Checks that a filterset will not create invalid select statement."""
        # check that there's either a =, a IN or a CONTAINS (collection)
        # relationship with a primary key or indexed field
        equal_ops = [
            self.model._get_column_by_db_name(w.field)
            for w in self._where
            if isinstance(w.operator, EqualsOperator) and not isinstance(w.value, Token)
        ]
        token_comparison = any([w for w in self._where if isinstance(w.value, Token)])
        has_pk_or_idx = any(w.primary_key or w.index for w in equal_ops)
        valid_clause = has_pk_or_idx or token_comparison or self._allow_filtering
        if not valid_clause:
            raise QueryException(
                "Where clauses require either =, a IN or a CONTAINS "
                "(collection) comparison with either a primary key or "
                "indexed field"
            )

        if not self._allow_filtering:
            # if the query is not on an indexed field
            using_index = any(w.index for w in equal_ops)
            if not using_index:
                valid_filtering = any([w.partition_key for w in equal_ops]) or token_comparison
                if not valid_filtering:
                    raise QueryException(
                        "Filtering on a clustering key without a partition "
                        "key is not allowed unless allow_filtering() is "
                        "called on the querset"
                    )

    def _select_query(self):
        """
        Returns a select clause based on the given filter args
        """
        if self._where:
            self._validate_select_where()
        return SelectStatement(
            self.column_family_name,
            fields=self._select_fields(),
            where=self._where,
            order_by=self._order,
            limit=self._limit,
            allow_filtering=self._allow_filtering,
            distinct_fields=self._distinct_fields,
            fetch_size=self._fetch_size,
        )

    def _execute_query(self, conn):
        """
        Initialize the result cache if needed, does nothing if the result
        cache was already initialized.

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass
        """
        if self._result_cache is None:
            self._result_generator = (
                i for i in self._execute_statement(conn, self._select_query())
            )
            self._result_cache = []
            self._construct_result = self._maybe_inject_deferred(self._get_result_constructor())

            # "DISTINCT COUNT()" is not supported in C* < 2.2, so we need to
            # materialize all results to get len() and count() working with
            # DISTINCT queries
            if self._materialize_results or self._distinct_fields:
                self._fill_result_cache(conn)

    def _fill_result_cache(self, conn):
        """
        Fill the result cache with all results.

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass
        """

        idx = 0
        try:
            while True:
                idx += 1000
                self._fill_result_cache_to_idx(conn, idx)
        except StopIteration:
            pass

        self._count = len(self._result_cache)

    def _fill_result_cache_to_idx(self, conn, idx):
        """
        Fill the result cache to the given index.

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass
        :param idx: Index value to fill the result cache to
        :type: int
        """
        self._execute_query(conn)
        if self._result_idx is None:
            self._result_idx = -1

        qty = idx - self._result_idx
        if qty < 1:
            return
        else:
            for idx in range(qty):
                self._result_idx += 1
                while True:
                    try:
                        result = self._construct_result(self._result_cache[self._result_idx])
                        self._result_cache[self._result_idx] = result
                        break
                    except IndexError:
                        self._result_cache.append(next(self._result_generator))

    def iter(self, conn):
        """ Return an iterator over all of the objects return by the query.

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass
        """
        self._execute_query(conn)

        idx = 0
        while True:
            if len(self._result_cache) <= idx:
                try:
                    self._result_cache.append(next(self._result_generator))
                except StopIteration:
                    break

            instance = self._result_cache[idx]
            if isinstance(instance, dict):
                self._fill_result_cache_to_idx(conn, idx)
            yield self._result_cache[idx]

            idx += 1

    def _get_result_constructor(self):
        """Returns a function that will be used to instantiate query results.
        """
        if not self._values_list:
            # we want models
            return self.model._construct_instance
        elif self._flat_values_list:
            # the user has requested flattened list (1 value per row)
            key = self._only_fields[0]
            return lambda row: row[key]
        else:
            return lambda row: [row[f] for f in self._only_fields]

    @staticmethod
    def _construct_with_deferred(f, deferred, row):
        """
        Update the row dict with the deferred values and return a new instance
        of class f
        """
        row.update(deferred)
        return f(row)

    def _maybe_inject_deferred(self, constructor):
        if not self._deferred_values:
            return constructor
        return partial(self._construct_with_deferred, constructor, self._deferred_values)

    def first(self, conn):
        try:
            return six.next(self.iter(conn))
        except StopIteration:
            return None

    def all(self):
        """Returns a queryset matching all rows.

        .. code-block:: python

            for user in User.objects().all():
                print(user)
        """
        return copy.deepcopy(self)

    def consistency(self, consistency):
        """Sets the consistency level for the operation.

        See :class:`.ConsistencyLevel`.

        .. code-block:: python

            for user in User.objects(id=3).consistency(CL.ONE):
                print(user)
        """
        clone = copy.deepcopy(self)
        clone._consistency = consistency
        return clone

    def _parse_filter_arg(self, arg):
        """
        Parses a filter arg in the format:
        <colname>__<op>
        :returns: colname, op tuple
        """
        statement = arg.rsplit("__", 1)
        if len(statement) == 1:
            return arg, None
        elif len(statement) == 2:
            if arg != "pk__token":
                return (statement[0], statement[1])
            else:
                return (arg, None)
        else:
            raise QueryException("Can't parse '{0}'".format(arg))

    def iff(self, *args, **kwargs):
        """Adds IF statements to queryset"""
        if len([x for x in kwargs.values() if x is None]):
            raise CQLEngineException("None values on iff are not allowed")

        clone = copy.deepcopy(self)
        for operator in args:
            if not isinstance(operator, ConditionalClause):
                raise QueryException("{0} is not a valid query operator".format(operator))
            clone._conditional.append(operator)

        for arg, val in kwargs.items():
            if isinstance(val, Token):
                raise QueryException("Token() values are not valid in conditionals")

            col_name, col_op = self._parse_filter_arg(arg)
            try:
                column = self.model._get_column(col_name)
            except KeyError:
                raise QueryException("Can't resolve column name: '{0}'".format(col_name))

            if isinstance(val, BaseQueryFunction):
                query_val = val
            else:
                query_val = column.to_database(val)

            operator_class = BaseWhereOperator.get_operator(col_op or "EQ")
            operator = operator_class()
            clone._conditional.append(WhereClause(column.db_field_name, operator, query_val))

        return clone

    def filter(self, **kwargs):
        """
        Adds WHERE arguments to the queryset, returning a new queryset

        See :ref:`retrieving-objects-with-filters`

        Returns a QuerySet filtered on the keyword arguments
        """
        # add arguments to the where clause filters
        if len([x for x in kwargs.values() if x is None]):
            raise CQLEngineException("None values on filter are not allowed")

        clone = copy.deepcopy(self)

        for arg, val in kwargs.items():
            col_name, col_op = self._parse_filter_arg(arg)
            quote_field = True

            if not isinstance(val, Token):
                try:
                    column = self.model._get_column(col_name)
                except KeyError:
                    raise QueryException("Can't resolve column name: '{0}'".format(col_name))
            else:
                if col_name != "pk__token":
                    raise QueryException(
                        "Token() values may only be compared to the " "'pk__token' virtual column"
                    )

                column = columns._PartitionKeysToken(self.model)
                quote_field = False

                partition_columns = column.partition_columns
                if len(partition_columns) != len(val.value):
                    raise QueryException(
                        "Token() received {0} arguments but model has {1} "
                        "partition keys".format(len(val.value), len(partition_columns))
                    )
                val.set_columns(partition_columns)

            # get query operator, or use equals if not supplied
            operator_class = BaseWhereOperator.get_operator(col_op or "EQ")
            operator = operator_class()

            if isinstance(operator, InOperator):
                if not isinstance(val, (list, tuple)):
                    raise QueryException("IN queries must use a list/tuple value")
                query_val = [column.to_database(v) for v in val]
            elif isinstance(val, BaseQueryFunction):
                query_val = val
            elif isinstance(operator, ContainsOperator) and isinstance(
                column, (columns.List, columns.Set, columns.Map)
            ):
                # For ContainsOperator and collections, we query using the
                # value, not the container
                query_val = val
            else:
                query_val = column.to_database(val)
                if not col_op:  # only equal values should be deferred
                    clone._defer_fields.add(col_name)
                    # map by db field name for substitution in results
                    clone._deferred_values[column.db_field_name] = val

            clone._where.append(
                WhereClause(column.db_field_name, operator, query_val, quote_field=quote_field)
            )

        return clone

    def find(self, conn, **kwargs):
        """ Returns an iterator on the results generated by running the current
        query along with any additional filters specified by kwargs.

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass
        :param kwargs: keyword arguments to be passed to self.filter
        """
        if kwargs:
            return self.filter(**kwargs).find(conn)

        self._execute_query(conn)
        return self.iter(conn)

    def find_all(self, conn, **kwargs):
        """ Returns a list of all the results generated by running the current
        query along with any additional filters specified by kwargs.

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass
        :param kwargs: keyword arguments to be passed to self.filter
        """
        return [x for x in self.find(conn, **kwargs)]

    def get(self, conn, **kwargs):
        """
        Returns a single instance matching this query, optionally with
        additional filter kwargs.

        See :ref:`retrieving-objects-with-filters`

        Returns a single object matching the QuerySet.

        .. code-block:: python

            user = User.get(id=1)

        If no objects are matched, a :class:`~.DoesNotExist` exception is
        raised.

        If more than one object is found, a :class:`~.MultipleObjectsReturned`
        exception is raised.

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass
        :param kwargs: keyword arguments to be passed to self.filter
        """
        if kwargs:
            return self.filter(**kwargs).get(conn)

        self._execute_query(conn)

        NOT_SET = object()
        obj = NOT_SET
        for result in self.iter(conn):
            if obj is NOT_SET:
                obj = result
            else:
                raise self.model.MultipleObjectsReturned("Multiple objects found")
        if obj is NOT_SET:
            raise self.model.DoesNotExist

        return obj

    def _get_ordering_condition(self, colname):
        order_type = "DESC" if colname.startswith("-") else "ASC"
        colname = colname.replace("-", "")

        column = self.model._columns.get(colname)
        if column is None:
            raise QueryException("Can't resolve the column name: '{0}'".format(colname))

        # validate the column selection
        if not column.primary_key:
            raise QueryException(
                "Can't order on '{0}', can only order on (clustered) primary "
                "keys".format(colname)
            )

        pks = [v for k, v in self.model._columns.items() if v.primary_key]
        if column == pks[0]:
            raise QueryException(
                "Can't order by the first primary key (partition key), "
                "clustering (secondary) keys only"
            )

        return column.db_field_name, order_type

    def order_by(self, *colnames):
        """Sets the column(s) to be used for ordering.

        Default order is ascending, prepend a '-' to any column name for
        descending.

        *Note: column names must be a clustering key*

        .. code-block:: python

            from uuid import uuid1,uuid4

            class Comment(Model):
                photo_id = UUID(primary_key=True)
                # second primary key component is a clustering key
                comment_id = TimeUUID(primary_key=True, default=uuid1)
                comment = Text()

            sync_table(Comment)

            u = uuid4()
            for x in range(5):
                Comment.create(photo_id=u, comment="test %d" % x)

            print("Normal")
            for comment in Comment.objects(photo_id=u):
                print comment.comment_id

            print("Reversed")
            for comment in Comment.objects(photo_id=u).order_by("-comment_id"):
                print comment.comment_id
        """
        if len(colnames) == 0:
            clone = copy.deepcopy(self)
            clone._order = []
            return clone

        conditions = []
        for colname in colnames:
            conditions.append('"{0}" {1}'.format(*self._get_ordering_condition(colname)))

        clone = copy.deepcopy(self)
        clone._order.extend(conditions)
        return clone

    def count(self, conn):
        """Returns the number of rows matched by this query.

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass

        *Note: This function executes a SELECT COUNT() and has a performance
        cost on large datasets*
        """
        if self._count is None:
            query = self._select_query()
            query.count = True
            result = self._execute_statement(conn, query)
            count_row = result[0].popitem()
            self._count = count_row[1]
        return self._count

    def distinct(self, distinct_fields=None):
        """Returns the DISTINCT rows matched by this query.

        distinct_fields default to the partition key fields if not specified.

        *Note: distinct_fields must be a partition key or a static column*

        .. code-block:: python

            class Automobile(Model):
                manufacturer = columns.Text(partition_key=True)
                year = columns.Integer(primary_key=True)
                model = columns.Text(primary_key=True)
                price = columns.Decimal()

            sync_table(Automobile)

            # create rows

            Automobile.objects.distinct()

            # or

            Automobile.objects.distinct(['manufacturer'])

        """

        clone = copy.deepcopy(self)
        if distinct_fields:
            clone._distinct_fields = distinct_fields
        else:
            clone._distinct_fields = [x.column_name for x in self.model._partition_keys.values()]

        return clone

    def limit(self, v):
        """Limits the number of results returned by Cassandra. Use *0* or
        *None* to disable.

        *Note that CQL's default limit is 10,000, so all queries without a
        limit set explicitly will have an implicit limit of 10,000*

        .. code-block:: python

            # Fetch 100 users
            for user in User.objects().limit(100):
                print(user)

            # Fetch all users
            for user in User.objects().limit(None):
                print(user)
        """

        if v is None:
            v = 0

        if not isinstance(v, six.integer_types):
            raise TypeError
        if v == self._limit:
            return self

        if v < 0:
            raise QueryException("Negative limit is not allowed")

        clone = copy.deepcopy(self)
        clone._limit = v
        return clone

    def fetch_size(self, v):
        """Sets the number of rows that are fetched at a time.

        *Note that driver's default fetch size is 5000.*

        .. code-block:: python

            for user in User.objects().fetch_size(500):
                print(user)
        """

        if not isinstance(v, six.integer_types):
            raise TypeError
        if v == self._fetch_size:
            return self

        if v < 1:
            raise QueryException("fetch size less than 1 is not allowed")

        clone = copy.deepcopy(self)
        clone._fetch_size = v
        return clone

    def allow_filtering(self):
        """ Enables the (usually) unwise practive of querying on a clustering
        key without also defining a partition key.
        """
        clone = copy.deepcopy(self)
        clone._allow_filtering = True
        return clone

    def _only_or_defer(self, action, fields):
        if action == "only" and self._only_fields:
            raise QueryException("QuerySet already has 'only' fields defined")

        clone = copy.deepcopy(self)

        # check for strange fields
        missing_fields = [f for f in fields if f not in self.model._columns.keys()]
        if missing_fields:
            raise QueryException(
                "Can't resolve fields {0} in {1}".format(
                    ", ".join(missing_fields), self.model.__name__
                )
            )

        if action == "defer":
            clone._defer_fields.update(fields)
        elif action == "only":
            clone._only_fields = fields
        else:
            raise ValueError

        return clone

    def only(self, fields):
        """ Load only these fields for the returned query """
        return self._only_or_defer("only", fields)

    def defer(self, fields):
        """ Don't load these fields for the returned query """
        return self._only_or_defer("defer", fields)

    def create(self, conn, **kwargs):
        """ Build a new instance of self.model with the given kwargs, save it
        to the database, and return it.

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass
        :param kwargs: keyword arguments that will be passed to the constructor
            for the class specified by self.model
        """
        return (
            self.model(**kwargs)
            .ttl(self._ttl)
            .consistency(self._consistency)
            .if_not_exists(self._if_not_exists)
            .timestamp(self._timestamp)
            .if_exists(self._if_exists)
            .save(conn)
        )

    def delete(self, conn):
        """
        Deletes the contents of a query

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass
        """
        # validate where clause
        partition_keys = set(x.db_field_name for x in self.model._partition_keys.values())
        if partition_keys - set(c.field for c in self._where):
            raise QueryException("The partition key must be defined on delete queries")

        dq = DeleteStatement(
            self.column_family_name,
            where=self._where,
            timestamp=self._timestamp,
            conditionals=self._conditional,
            if_exists=self._if_exists,
        )
        self._execute_statement(conn, dq)

    def __eq__(self, q):
        if len(self._where) == len(q._where):
            return all([w in q._where for w in self._where])
        return False

    def __ne__(self, q):
        return not (self != q)

    def timeout(self, timeout):
        """
        :param timeout: Timeout for the query (in seconds)
        :type timeout: float or None
        """
        clone = copy.deepcopy(self)
        clone._timeout = timeout
        return clone

    def using(self):
        """Return a deepcopy of self"""

        clone = copy.deepcopy(self)

        return clone

    def values_list(self, *fields, **kwargs):
        """Instructs the query set to return tuples, not model instance."""
        flat = kwargs.pop("flat", False)
        if kwargs:
            raise TypeError("Unexpected keyword arguments to values_list: %s" % (kwargs.keys(),))
        if flat and len(fields) > 1:
            raise TypeError(
                "'flat' is not valid when values_list is called with more " "than one field."
            )
        clone = self.only(fields)
        clone._values_list = True
        clone._flat_values_list = flat
        return clone

    def ttl(self, ttl):
        """
        Sets the ttl (in seconds) for modified data.

        *Note that running a select query with a ttl value will raise an
        exception*
        """
        clone = copy.deepcopy(self)
        clone._ttl = ttl
        return clone

    def timestamp(self, timestamp):
        """Allows for custom timestamps to be saved with the record."""
        clone = copy.deepcopy(self)
        clone._timestamp = timestamp
        return clone

    def if_not_exists(self):
        """Check the existence of an object before insertion.

        If the insertion isn't applied, a LWTException is raised.
        """
        if self.model._has_counter:
            raise IfNotExistsWithCounterColumn(
                "if_not_exists cannot be used with tables containing " "counter columns"
            )
        clone = copy.deepcopy(self)
        clone._if_not_exists = True
        return clone

    def if_exists(self):
        """
        Check the existence of an object before an update or delete.

        If the update or delete isn't applied, a LWTException is raised.
        """
        if self.model._has_counter:
            raise IfExistsWithCounterColumn(
                "if_exists cannot be used with tables containing " "counter columns"
            )
        clone = copy.deepcopy(self)
        clone._if_exists = True
        return clone

    def update(self, conn, **values):
        """
        Performs an update on the row selected by the queryset. Include
        values to update in the update like so:

        :param conn: Cassandra connection wrapper used to execute the query.
        :type: cqlengine.ConnectionInterface subclass
        :param values: keyword arguments used to specify the update query.

        .. code-block:: python

            Model.objects(key=n).update(value='x')

        Passing in updates for columns which are not part of the model will
        raise a ValidationError.

        Per column validation will be performed, but instance level validation
        will not (i.e., `Model.validate` is not called).  This is sometimes
        referred to as a blind update.

        For example:

        .. code-block:: python

            class User(Model):
                id = Integer(primary_key=True)
                name = Text()

            setup(["localhost"], "test")
            sync_table(User)

            u = User.create(id=1, name="jon")

            User.objects(id=1).update(name="Steve")

            # sets name to null
            User.objects(id=1).update(name=None)


        Also supported is blindly adding and removing elements from container
        columns, without loading a model instance from Cassandra.

        Using the syntax `.update(column_name={x, y, z})` will overwrite the
        contents of the container, like updating a non container column.
        However, adding `__<operation>` to the end of the keyword arg, makes
        the update call add or remove items from the collection, without
        overwriting then entire column.

        Given the model below, here are the operations that can be performed
        on the different container columns:

        .. code-block:: python

            class Row(Model):
                row_id      = columns.Integer(primary_key=True)
                set_column  = columns.Set(Integer)
                list_column = columns.List(Integer)
                map_column  = columns.Map(Integer, Integer)

        :class:`~cqlengine.columns.Set`

        - `add`: adds the elements of the given set to the column
        - `remove`: removes the elements of the given set to the column


        .. code-block:: python

            # add elements to a set
            Row.objects(row_id=5).update(set_column__add={6})

            # remove elements to a set
            Row.objects(row_id=5).update(set_column__remove={4})

        :class:`~cqlengine.columns.List`

        - `append`: appends the elements of the given list to the end of the
            column
        - `prepend`: prepends the elements of the given list to the beginning
            of the column

        .. code-block:: python

            # append items to a list
            Row.objects(row_id=5).update(list_column__append=[6, 7])

            # prepend items to a list
            Row.objects(row_id=5).update(list_column__prepend=[1, 2])


        :class:`~cqlengine.columns.Map`

        - `update`: adds the given keys/values to the columns, creating new
            entries if they didn't exist, and overwriting old ones if they did

        .. code-block:: python

            # add items to a map
            Row.objects(row_id=5).update(map_column__update={1: 2, 3: 4})
        """
        if not values:
            return

        nulled_columns = set()
        updated_columns = set()
        us = UpdateStatement(
            self.column_family_name,
            where=self._where,
            ttl=self._ttl,
            timestamp=self._timestamp,
            conditionals=self._conditional,
            if_exists=self._if_exists,
        )
        for name, val in values.items():
            col_name, col_op = self._parse_filter_arg(name)
            col = self.model._columns.get(col_name)
            # check for nonexistant columns
            if col is None:
                raise ValidationError(
                    "{0}.{1} has no column named: {2}".format(
                        self.__module__, self.model.__name__, col_name
                    )
                )
            # check for primary key update attempts
            if col.is_primary_key:
                raise ValidationError(
                    "Cannot apply update to primary key '{0}' for "
                    "{1}.{2}".format(col_name, self.__module__, self.model.__name__)
                )

            # we should not provide default values in this use case.
            val = col.validate(val)

            if val is None:
                nulled_columns.add(col_name)
                continue

            us.add_update(col, val, operation=col_op)
            updated_columns.add(col_name)

        if us.assignments:
            self._execute_statement(conn, us)

        if nulled_columns:
            delete_conditional = (
                [
                    condition
                    for condition in self._conditional
                    if condition.field not in updated_columns
                ]
                if self._conditional
                else None
            )
            ds = DeleteStatement(
                self.column_family_name,
                fields=nulled_columns,
                where=self._where,
                conditionals=delete_conditional,
                if_exists=self._if_exists,
            )
            self._execute_statement(conn, ds)
