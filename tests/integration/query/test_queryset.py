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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from uuid import uuid4
from datetime import datetime, timedelta, tzinfo

from cassandra import InvalidRequest
from cassandra.util import uuid_from_time

from cqlmapper import columns, functions, operators, query, statements, TIMEOUT_NOT_SET
from cqlmapper.batch import Batch
from cqlmapper.management import sync_table, drop_table
from cqlmapper.models import Model
from cqlmapper.operators import EqualsOperator, GreaterThanOrEqualOperator
from cqlmapper.query import QueryException

from tests.integration import PROTOCOL_VERSION, CASSANDRA_VERSION, execute_count
from tests.integration.base import BaseCassEngTestCase, main

from unittest import mock


class TzOffset(tzinfo):
    """Minimal implementation of a timezone offset to help testing with timezone
    aware datetimes.
    """

    def __init__(self, offset):
        self._offset = timedelta(hours=offset)

    def utcoffset(self, dt):
        return self._offset

    def tzname(self, dt):
        return "TzOffset: {}".format(self._offset.hours)

    def dst(self, dt):
        return timedelta(0)


class TestModel(Model):

    test_id = columns.Integer(primary_key=True)
    attempt_id = columns.Integer(primary_key=True)
    description = columns.Text()
    expected_result = columns.Integer()
    test_result = columns.Integer()


class IndexedTestModel(Model):

    test_id = columns.Integer(primary_key=True)
    attempt_id = columns.Integer(index=True)
    description = columns.Text()
    expected_result = columns.Integer()
    test_result = columns.Integer(index=True)


class IndexedCollectionsTestModel(Model):

    test_id = columns.Integer(primary_key=True)
    attempt_id = columns.Integer(index=True)
    description = columns.Text()
    expected_result = columns.Integer()
    test_result = columns.Integer(index=True)
    test_list = columns.List(columns.Integer, index=True)
    test_set = columns.Set(columns.Integer, index=True)
    test_map = columns.Map(columns.Text, columns.Integer, index=True)

    test_list_no_index = columns.List(columns.Integer, index=False)
    test_set_no_index = columns.Set(columns.Integer, index=False)
    test_map_no_index = columns.Map(columns.Text, columns.Integer, index=False)


class TestMultiClusteringModel(Model):

    one = columns.Integer(primary_key=True)
    two = columns.Integer(primary_key=True)
    three = columns.Integer(primary_key=True)


class TestQuerySetOperation(BaseCassEngTestCase):
    def test_query_filter_parsing(self):
        """
        Tests the queryset filter method parses it's kwargs properly
        """
        query1 = TestModel.objects(test_id=5)
        assert len(query1._where) == 1

        op = query1._where[0]
        assert isinstance(op.operator, operators.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

        op = query2._where[1]
        assert isinstance(op.operator, operators.GreaterThanOrEqualOperator)
        assert op.value == 1

    def test_filter_method_where_clause_generation(self):
        """
        Tests the where clause creation
        """
        query1 = TestModel.objects(test_id=5)
        self.assertEqual(len(query1._where), 1)
        where = query1._where[0]
        self.assertEqual(where.field, "test_id")
        self.assertEqual(where.value, 5)

        query2 = query1.filter(expected_result__gte=1)
        self.assertEqual(len(query2._where), 2)

        where = query2._where[0]
        self.assertEqual(where.field, "test_id")
        self.assertIsInstance(where.operator, EqualsOperator)
        self.assertEqual(where.value, 5)

        where = query2._where[1]
        self.assertEqual(where.field, "expected_result")
        self.assertIsInstance(where.operator, GreaterThanOrEqualOperator)
        self.assertEqual(where.value, 1)


class TestQuerySetOperation(BaseCassEngTestCase):
    def test_query_filter_parsing(self):
        """
        Tests the queryset filter method parses it's kwargs properly
        """
        query1 = TestModel.objects(test_id=5)
        assert len(query1._where) == 1

        op = query1._where[0]

        assert isinstance(op, statements.WhereClause)
        assert isinstance(op.operator, operators.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

        op = query2._where[1]
        self.assertIsInstance(op, statements.WhereClause)
        self.assertIsInstance(op.operator, operators.GreaterThanOrEqualOperator)
        assert op.value == 1

    def test_using_invalid_column_names_in_filter_kwargs_raises_error(self):
        """
        Tests that using invalid or nonexistant column names for filter args
        raises an error
        """
        with self.assertRaises(query.QueryException):
            TestModel.objects(nonsense=5)

    def test_queryset_is_immutable(self):
        """
        Tests that calling a queryset function that changes it's state returns
        a new queryset
        """
        query1 = TestModel.objects(test_id=5)
        assert len(query1._where) == 1

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2
        assert len(query1._where) == 1

    def test_queryset_limit_immutability(self):
        """
        Tests that calling a queryset function that changes it's state returns
        a new queryset with same limit
        """
        query1 = TestModel.objects(test_id=5).limit(1)
        assert query1._limit == 1

        query2 = query1.filter(expected_result__gte=1)
        assert query2._limit == 1

        query3 = query1.filter(expected_result__gte=1).limit(2)
        assert query1._limit == 1
        assert query3._limit == 2

    def test_the_all_method_duplicates_queryset(self):
        """
        Tests that calling all on a queryset with previously defined filters
        duplicates queryset
        """
        query1 = TestModel.objects(test_id=5)
        assert len(query1._where) == 1

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

        query3 = query2.all()
        assert query3 == query2

    def test_queryset_with_distinct(self):
        """
        Tests that calling distinct on a queryset w/without parameter are
        evaluated properly.
        """

        query1 = TestModel.objects.distinct()
        self.assertEqual(len(query1._distinct_fields), 1)

        query2 = TestModel.objects.distinct(["test_id"])
        self.assertEqual(len(query2._distinct_fields), 1)

        query3 = TestModel.objects.distinct(["test_id", "attempt_id"])
        self.assertEqual(len(query3._distinct_fields), 2)

    def test_defining_only_fields(self):
        """
        Tests defining only fields

        @since 3.5
        @jira_ticket PYTHON-560
        @expected_result deferred fields should not be returned

        @test_category object_mapper
        """
        # simple only definition
        q = TestModel.objects.only(["attempt_id", "description"])
        self.assertEqual(q._select_fields(), ["attempt_id", "description"])

        with self.assertRaises(query.QueryException):
            TestModel.objects.only(["nonexistent_field"])

        # Cannot define more than once only fields
        with self.assertRaises(query.QueryException):
            TestModel.objects.only(["description"]).only(["attempt_id"])

        # only with defer fields
        q = TestModel.objects.only(["attempt_id", "description"])
        q = q.defer(["description"])
        self.assertEqual(q._select_fields(), ["attempt_id"])

        # Eliminate all results confirm exception is thrown
        q = TestModel.objects.only(["description"])
        q = q.defer(["description"])
        with self.assertRaises(query.QueryException):
            q._select_fields()

        q = TestModel.objects.filter(test_id=0).only(["test_id", "attempt_id", "description"])
        self.assertEqual(q._select_fields(), ["attempt_id", "description"])

        # no fields to select
        with self.assertRaises(query.QueryException):
            q = TestModel.objects.only(["test_id"]).defer(["test_id"])
            q._select_fields()

        with self.assertRaises(query.QueryException):
            q = TestModel.objects.filter(test_id=0).only(["test_id"])
            q._select_fields()

    def test_defining_defer_fields(self):
        """
        Tests defining defer fields

        @since 3.5
        @jira_ticket PYTHON-560
        @jira_ticket PYTHON-599
        @expected_result deferred fields should not be returned

        @test_category object_mapper
        """

        # simple defer definition
        q = TestModel.objects.defer(["attempt_id", "description"])
        self.assertEqual(q._select_fields(), ["test_id", "expected_result", "test_result"])

        with self.assertRaises(query.QueryException):
            TestModel.objects.defer(["nonexistent_field"])

        # defer more than one
        q = TestModel.objects.defer(["attempt_id", "description"])
        q = q.defer(["expected_result"])
        self.assertEqual(q._select_fields(), ["test_id", "test_result"])

        # defer with only
        q = TestModel.objects.defer(["description", "attempt_id"])
        q = q.only(["description", "test_id"])
        self.assertEqual(q._select_fields(), ["test_id"])

        # Eliminate all results confirm exception is thrown
        q = TestModel.objects.defer(["description", "attempt_id"])
        q = q.only(["description"])
        with self.assertRaises(query.QueryException):
            q._select_fields()

        # implicit defer
        q = TestModel.objects.filter(test_id=0)
        self.assertEqual(
            q._select_fields(), ["attempt_id", "description", "expected_result", "test_result"]
        )

        # when all fields are defered, it fallbacks select the partition keys
        q = TestModel.objects.defer(
            ["test_id", "attempt_id", "description", "expected_result", "test_result"]
        )
        self.assertEqual(q._select_fields(), ["test_id"])


class BaseQuerySetUsage(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(BaseQuerySetUsage, cls).setUpClass()
        conn = cls.connection()
        drop_table(conn, TestModel)
        drop_table(conn, IndexedTestModel)

        sync_table(conn, TestModel)
        sync_table(conn, IndexedTestModel)
        sync_table(conn, TestMultiClusteringModel)

        TestModel.objects.create(
            conn, test_id=0, attempt_id=0, description="try1", expected_result=5, test_result=30
        )
        TestModel.objects.create(
            conn, test_id=0, attempt_id=1, description="try2", expected_result=10, test_result=30
        )
        TestModel.objects.create(
            conn, test_id=0, attempt_id=2, description="try3", expected_result=15, test_result=30
        )
        TestModel.objects.create(
            conn, test_id=0, attempt_id=3, description="try4", expected_result=20, test_result=25
        )

        TestModel.objects.create(
            conn, test_id=1, attempt_id=0, description="try5", expected_result=5, test_result=25
        )
        TestModel.objects.create(
            conn, test_id=1, attempt_id=1, description="try6", expected_result=10, test_result=25
        )
        TestModel.objects.create(
            conn, test_id=1, attempt_id=2, description="try7", expected_result=15, test_result=25
        )
        TestModel.objects.create(
            conn, test_id=1, attempt_id=3, description="try8", expected_result=20, test_result=20
        )

        TestModel.objects.create(
            conn, test_id=2, attempt_id=0, description="try9", expected_result=50, test_result=40
        )
        TestModel.objects.create(
            conn, test_id=2, attempt_id=1, description="try10", expected_result=60, test_result=40
        )
        TestModel.objects.create(
            conn, test_id=2, attempt_id=2, description="try11", expected_result=70, test_result=45
        )
        TestModel.objects.create(
            conn, test_id=2, attempt_id=3, description="try12", expected_result=75, test_result=45
        )

        IndexedTestModel.objects.create(
            conn, test_id=0, attempt_id=0, description="try1", expected_result=5, test_result=30
        )
        IndexedTestModel.objects.create(
            conn, test_id=1, attempt_id=1, description="try2", expected_result=10, test_result=30
        )
        IndexedTestModel.objects.create(
            conn, test_id=2, attempt_id=2, description="try3", expected_result=15, test_result=30
        )
        IndexedTestModel.objects.create(
            conn, test_id=3, attempt_id=3, description="try4", expected_result=20, test_result=25
        )

        IndexedTestModel.objects.create(
            conn, test_id=4, attempt_id=0, description="try5", expected_result=5, test_result=25
        )
        IndexedTestModel.objects.create(
            conn, test_id=5, attempt_id=1, description="try6", expected_result=10, test_result=25
        )
        IndexedTestModel.objects.create(
            conn, test_id=6, attempt_id=2, description="try7", expected_result=15, test_result=25
        )
        IndexedTestModel.objects.create(
            conn, test_id=7, attempt_id=3, description="try8", expected_result=20, test_result=20
        )

        IndexedTestModel.objects.create(
            conn, test_id=8, attempt_id=0, description="try9", expected_result=50, test_result=40
        )
        IndexedTestModel.objects.create(
            conn, test_id=9, attempt_id=1, description="try10", expected_result=60, test_result=40
        )
        IndexedTestModel.objects.create(
            conn, test_id=10, attempt_id=2, description="try11", expected_result=70, test_result=45
        )
        IndexedTestModel.objects.create(
            conn, test_id=11, attempt_id=3, description="try12", expected_result=75, test_result=45
        )

        if CASSANDRA_VERSION >= "2.1":
            drop_table(conn, IndexedCollectionsTestModel)
            sync_table(conn, IndexedCollectionsTestModel)
            IndexedCollectionsTestModel.objects.create(
                conn,
                test_id=12,
                attempt_id=3,
                description="list12",
                expected_result=75,
                test_result=45,
                test_list=[1, 2, 42],
                test_set=set([1, 2, 3]),
                test_map={"1": 1, "2": 2, "3": 3},
            )
            IndexedCollectionsTestModel.objects.create(
                conn,
                test_id=13,
                attempt_id=3,
                description="list13",
                expected_result=75,
                test_result=45,
                test_list=[3, 4, 5],
                test_set=set([4, 5, 42]),
                test_map={"1": 5, "2": 6, "3": 7},
            )
            IndexedCollectionsTestModel.objects.create(
                conn,
                test_id=14,
                attempt_id=3,
                description="list14",
                expected_result=75,
                test_result=45,
                test_list=[1, 2, 3],
                test_set=set([1, 2, 3]),
                test_map={"1": 1, "2": 2, "3": 42},
            )
            IndexedCollectionsTestModel.objects.create(
                conn,
                test_id=15,
                attempt_id=4,
                description="list14",
                expected_result=75,
                test_result=45,
                test_list_no_index=[1, 2, 3],
                test_set_no_index=set([1, 2, 3]),
                test_map_no_index={"1": 1, "2": 2, "3": 42},
            )

    @classmethod
    def tearDownClass(cls):
        super(BaseQuerySetUsage, cls).tearDownClass()
        conn = cls.connection()
        drop_table(conn, TestModel)
        drop_table(conn, IndexedTestModel)
        drop_table(conn, TestMultiClusteringModel)


class TestQuerySetCountSelectionAndIteration(BaseQuerySetUsage):
    @execute_count(2)
    def test_count(self):
        """
        Tests that adding filtering statements affects the count query as
        expected
        """
        assert TestModel.objects.count(self.conn) == 12

        q = TestModel.objects(test_id=0)
        assert q.count(self.conn) == 4

    @execute_count(2)
    def test_iteration(self):
        """
        Tests that iterating over a query set pulls back all of the expected
        results
        """
        q = TestModel.objects(test_id=0)
        # tuple of expected attempt_id, expected_result values
        compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
        for t in q.iter(self.conn):
            val = t.attempt_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

        # test with regular filtering
        q = TestModel.objects(attempt_id=3).allow_filtering()
        assert len(q.find_all(self.conn)) == 3
        # tuple of expected test_id, expected_result values
        compare_set = set([(0, 20), (1, 20), (2, 75)])
        for t in q.iter(self.conn):
            val = t.test_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

    @execute_count(1)
    def test_multiple_iterations_work_properly(self):
        """ Tests that iterating over a query set more than once works """
        q = TestModel.objects(test_id=0)
        # tuple of expected attempt_id, expected_result values
        compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
        for t in q.iter(self.conn):
            val = t.attempt_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

        # try it again
        compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
        for t in q.iter(self.conn):
            val = t.attempt_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

    @execute_count(1)
    def test_multiple_iterators_are_isolated(self):
        """
        tests that the use of one iterator does not affect the behavior of
        another
        """
        q = TestModel.objects(test_id=0)
        q = q.order_by("attempt_id")
        expected_order = [0, 1, 2, 3]
        iter1 = q.iter(self.conn)
        iter2 = q.iter(self.conn)
        for attempt_id in expected_order:
            assert next(iter1).attempt_id == attempt_id
            assert next(iter2).attempt_id == attempt_id

    @execute_count(3)
    def test_get_success_case(self):
        """
        Tests that the .get() method works on new and existing querysets
        """
        m = TestModel.objects.get(self.conn, test_id=0, attempt_id=0)
        assert isinstance(m, TestModel)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = TestModel.objects(test_id=0, attempt_id=0)
        m = q.get(self.conn)
        assert isinstance(m, TestModel)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = TestModel.objects(test_id=0)
        m = q.get(self.conn, attempt_id=0)
        assert isinstance(m, TestModel)
        assert m.test_id == 0
        assert m.attempt_id == 0

    @execute_count(1)
    def test_get_doesnotexist_exception(self):
        """
        Tests that get calls that don't return a result raises a DoesNotExist
        error
        """
        with self.assertRaises(TestModel.DoesNotExist):
            TestModel.objects.get(self.conn, test_id=100)

    @execute_count(1)
    def test_get_multipleobjects_exception(self):
        """
        Tests that get calls that return multiple results raise a
        MultipleObjectsReturned error
        """
        with self.assertRaises(TestModel.MultipleObjectsReturned):
            TestModel.objects.get(self.conn, test_id=1)

    @execute_count(7)
    def test_non_quality_filtering(self):
        class NonEqualityFilteringModel(Model):

            example_id = columns.UUID(primary_key=True, default=uuid4)
            sequence_id = columns.Integer(primary_key=True)  # sequence_id is a clustering key
            example_type = columns.Integer(index=True)
            created_at = columns.DateTime()

        # This makes 2 calls to execute since it has to create an index
        sync_table(self.conn, NonEqualityFilteringModel)

        # setup table, etc.

        NonEqualityFilteringModel.create(
            self.conn, sequence_id=1, example_type=0, created_at=datetime.now()
        )
        NonEqualityFilteringModel.create(
            self.conn, sequence_id=3, example_type=0, created_at=datetime.now()
        )
        NonEqualityFilteringModel.create(
            self.conn, sequence_id=5, example_type=1, created_at=datetime.now()
        )

        qa = NonEqualityFilteringModel.objects(sequence_id__gt=3).allow_filtering()
        num = qa.count(self.conn)
        assert num == 1, num
        drop_table(self.conn, NonEqualityFilteringModel)


class TestQuerySetDistinct(BaseQuerySetUsage):
    @execute_count(1)
    def test_distinct_without_parameter(self):
        q = TestModel.objects.distinct()
        self.assertEqual(len(q.find_all(self.conn)), 3)

    @execute_count(1)
    def test_distinct_with_parameter(self):
        q = TestModel.objects.distinct(["test_id"])
        self.assertEqual(len(q.find_all(self.conn)), 3)

    @execute_count(1)
    def test_distinct_with_filter(self):
        q = TestModel.objects.distinct(["test_id"]).filter(test_id__in=[1, 2])
        self.assertEqual(len(q.find_all(self.conn)), 2)

    @execute_count(1)
    def test_distinct_with_non_partition(self):
        with self.assertRaises(InvalidRequest):
            q = TestModel.objects.distinct(["description"]).filter(test_id__in=[1, 2])
            len(q.find_all(self.conn))

    @execute_count(1)
    def test_zero_result(self):
        q = TestModel.objects.distinct(["test_id"]).filter(test_id__in=[52])
        self.assertEqual(len(q.find_all(self.conn)), 0)

    @execute_count(2)
    def test_distinct_with_explicit_count(self):
        q = TestModel.objects.distinct(["test_id"])
        self.assertEqual(q.count(self.conn), 3)

        q = TestModel.objects.distinct(["test_id"]).filter(test_id__in=[1, 2])
        self.assertEqual(q.count(self.conn), 2)


class TestQuerySetOrdering(BaseQuerySetUsage):
    @execute_count(2)
    def test_order_by_success_case(self):
        q = TestModel.objects(test_id=0).order_by("attempt_id")
        expected_order = [0, 1, 2, 3]
        for model, expect in zip(q.iter(self.conn), expected_order):
            assert model.attempt_id == expect

        q = q.order_by("-attempt_id")
        expected_order.reverse()
        for model, expect in zip(q.iter(self.conn), expected_order):
            assert model.attempt_id == expect

    def test_ordering_by_non_second_primary_keys_fail(self):
        # kwarg filtering
        with self.assertRaises(query.QueryException):
            TestModel.objects(test_id=0).order_by("test_id")

    def test_ordering_by_non_primary_keys_fails(self):
        with self.assertRaises(query.QueryException):
            TestModel.objects(test_id=0).order_by("description")

    def test_ordering_on_indexed_columns_fails(self):
        with self.assertRaises(query.QueryException):
            IndexedTestModel.objects(test_id=0).order_by("attempt_id")

    @execute_count(8)
    def test_ordering_on_multiple_clustering_columns(self):
        TestMultiClusteringModel.create(self.conn, one=1, two=1, three=4)
        TestMultiClusteringModel.create(self.conn, one=1, two=1, three=2)
        TestMultiClusteringModel.create(self.conn, one=1, two=1, three=5)
        TestMultiClusteringModel.create(self.conn, one=1, two=1, three=1)
        TestMultiClusteringModel.create(self.conn, one=1, two=1, three=3)

        results = (
            TestMultiClusteringModel.objects.filter(one=1, two=1)
            .order_by("-two", "-three")
            .iter(self.conn)
        )
        assert [r.three for r in results] == [5, 4, 3, 2, 1]

        results = (
            TestMultiClusteringModel.objects.filter(one=1, two=1)
            .order_by("two", "three")
            .iter(self.conn)
        )
        assert [r.three for r in results] == [1, 2, 3, 4, 5]

        results = (
            TestMultiClusteringModel.objects.filter(one=1, two=1)
            .order_by("two")
            .order_by("three")
            .iter(self.conn)
        )
        assert [r.three for r in results] == [1, 2, 3, 4, 5]


class TestQuerySetValidation(BaseQuerySetUsage):
    def test_primary_key_or_index_must_be_specified(self):
        """
        Tests that queries that don't have an equals relation to a primary
        key or indexed field fail
        """
        with self.assertRaises(query.QueryException):
            q = TestModel.objects(test_result=25).iter(self.conn)
            list([i for i in q])

    def test_primary_key_or_index_must_have_equal_relation_filter(self):
        """
        Tests that queries that don't have non equal (>,<, etc) relation to a
        primary key or indexed field fail
        """
        with self.assertRaises(query.QueryException):
            q = TestModel.objects(test_id__gt=0).iter(self.conn)
            list([i for i in q])

    @execute_count(7)
    def test_indexed_field_can_be_queried(self):
        """
        Tests that queries on an indexed field will work without any primary key relations specified
        """
        q = IndexedTestModel.objects(test_result=25)
        self.assertEqual(q.count(self.conn), 4)

        q = IndexedCollectionsTestModel.objects.filter(test_list__contains=42)
        self.assertEqual(q.count(self.conn), 1)

        q = IndexedCollectionsTestModel.objects.filter(test_list__contains=13)
        self.assertEqual(q.count(self.conn), 0)

        q = IndexedCollectionsTestModel.objects.filter(test_set__contains=42)
        self.assertEqual(q.count(self.conn), 1)

        q = IndexedCollectionsTestModel.objects.filter(test_set__contains=13)
        self.assertEqual(q.count(self.conn), 0)

        q = IndexedCollectionsTestModel.objects.filter(test_map__contains=42)
        self.assertEqual(q.count(self.conn), 1)

        q = IndexedCollectionsTestModel.objects.filter(test_map__contains=13)
        self.assertEqual(q.count(self.conn), 0)


class TestQuerySetDelete(BaseQuerySetUsage):
    @execute_count(9)
    def test_delete(self):
        TestModel.objects.create(
            self.conn,
            test_id=3,
            attempt_id=0,
            description="try9",
            expected_result=50,
            test_result=40,
        )
        TestModel.objects.create(
            self.conn,
            test_id=3,
            attempt_id=1,
            description="try10",
            expected_result=60,
            test_result=40,
        )
        TestModel.objects.create(
            self.conn,
            test_id=3,
            attempt_id=2,
            description="try11",
            expected_result=70,
            test_result=45,
        )
        TestModel.objects.create(
            self.conn,
            test_id=3,
            attempt_id=3,
            description="try12",
            expected_result=75,
            test_result=45,
        )

        assert TestModel.objects.count(self.conn) == 16
        assert TestModel.objects(test_id=3).count(self.conn) == 4

        TestModel.objects(test_id=3).delete(self.conn)

        assert TestModel.objects.count(self.conn) == 12
        assert TestModel.objects(test_id=3).count(self.conn) == 0

    def test_delete_without_partition_key(self):
        """
        Tests that attempting to delete a model without defining a partition
        key fails
        """
        with self.assertRaises(query.QueryException):
            TestModel.objects(attempt_id=0).delete(self.conn)

    def test_delete_without_any_where_args(self):
        """
        Tests that attempting to delete a whole table without any arguments
        will fail
        """
        with self.assertRaises(query.QueryException):
            TestModel.objects(attempt_id=0).delete(self.conn)

    @unittest.skipIf(
        CASSANDRA_VERSION < "3.0",
        "range deletion was introduce in C* 3.0, currently running {0}".format(CASSANDRA_VERSION),
    )
    @execute_count(18)
    def test_range_deletion(self):
        """
        Tests that range deletion work as expected
        """

        for i in range(10):
            TestMultiClusteringModel.objects().create(self.conn, one=1, two=i, three=i)

        TestMultiClusteringModel.objects(one=1, two__gte=0, two__lte=3).delete(self.conn)
        self.assertEqual(6, len(TestMultiClusteringModel.objects.find_all(self.conn)))

        TestMultiClusteringModel.objects(one=1, two__gt=3, two__lt=5).delete(self.conn)
        self.assertEqual(5, len(TestMultiClusteringModel.objects.find_all(self.conn)))

        TestMultiClusteringModel.objects(one=1, two__in=[8, 9]).delete(self.conn)
        self.assertEqual(3, len(TestMultiClusteringModel.objects.find_all(self.conn)))

        TestMultiClusteringModel.objects(one__in=[1], two__gte=0).delete(self.conn)
        self.assertEqual(0, len(TestMultiClusteringModel.objects.find_all(self.conn)))


class TimeUUIDQueryModel(Model):

    partition = columns.UUID(primary_key=True)
    time = columns.TimeUUID(primary_key=True)
    data = columns.Text(required=False)


class TestMinMaxTimeUUIDFunctions(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(TestMinMaxTimeUUIDFunctions, cls).setUpClass()
        sync_table(cls.connection(), TimeUUIDQueryModel)

    @classmethod
    def tearDownClass(cls):
        super(TestMinMaxTimeUUIDFunctions, cls).tearDownClass()
        drop_table(cls.connection(), TimeUUIDQueryModel)

    @execute_count(7)
    def test_tzaware_datetime_support(self):
        """Test that using timezone aware datetime instances works with the
        MinTimeUUID/MaxTimeUUID functions.
        """
        pk = uuid4()
        midpoint_utc = datetime.utcnow().replace(tzinfo=TzOffset(0))
        midpoint_helsinki = midpoint_utc.astimezone(TzOffset(3))

        # Assert pre-condition that we have the same logical point in time
        assert midpoint_utc.utctimetuple() == midpoint_helsinki.utctimetuple()
        assert midpoint_utc.timetuple() != midpoint_helsinki.timetuple()

        TimeUUIDQueryModel.create(
            self.conn,
            partition=pk,
            time=uuid_from_time(midpoint_utc - timedelta(minutes=1)),
            data="1",
        )

        TimeUUIDQueryModel.create(
            self.conn, partition=pk, time=uuid_from_time(midpoint_utc), data="2"
        )

        TimeUUIDQueryModel.create(
            self.conn,
            partition=pk,
            time=uuid_from_time(midpoint_utc + timedelta(minutes=1)),
            data="3",
        )

        assert ["1", "2"] == [
            o.data
            for o in TimeUUIDQueryModel.filter(
                partition=pk, time__lte=functions.MaxTimeUUID(midpoint_utc)
            ).iter(self.conn)
        ]

        assert ["1", "2"] == [
            o.data
            for o in TimeUUIDQueryModel.filter(
                partition=pk, time__lte=functions.MaxTimeUUID(midpoint_helsinki)
            ).iter(self.conn)
        ]

        assert ["2", "3"] == [
            o.data
            for o in TimeUUIDQueryModel.filter(
                partition=pk, time__gte=functions.MinTimeUUID(midpoint_utc)
            ).iter(self.conn)
        ]

        assert ["2", "3"] == [
            o.data
            for o in TimeUUIDQueryModel.filter(
                partition=pk, time__gte=functions.MinTimeUUID(midpoint_helsinki)
            ).iter(self.conn)
        ]

    @execute_count(6)
    def test_success_case(self):
        """ Test that the min and max time uuid functions work as expected """
        pk = uuid4()
        startpoint = datetime.utcnow()
        TimeUUIDQueryModel.create(
            self.conn,
            partition=pk,
            time=uuid_from_time(startpoint + timedelta(seconds=1)),
            data="1",
        )
        TimeUUIDQueryModel.create(
            self.conn,
            partition=pk,
            time=uuid_from_time(startpoint + timedelta(seconds=2)),
            data="2",
        )
        midpoint = startpoint + timedelta(seconds=3)
        TimeUUIDQueryModel.create(
            self.conn,
            partition=pk,
            time=uuid_from_time(startpoint + timedelta(seconds=4)),
            data="3",
        )
        TimeUUIDQueryModel.create(
            self.conn,
            partition=pk,
            time=uuid_from_time(startpoint + timedelta(seconds=5)),
            data="4",
        )

        # test kwarg filtering
        q = TimeUUIDQueryModel.filter(partition=pk, time__lte=functions.MaxTimeUUID(midpoint)).iter(
            self.conn
        )
        q = [d for d in q]
        self.assertEqual(len(q), 2, msg="Got: %s" % q)
        datas = [d.data for d in q]
        assert "1" in datas
        assert "2" in datas

        q = TimeUUIDQueryModel.filter(partition=pk, time__gte=functions.MinTimeUUID(midpoint)).iter(
            self.conn
        )
        q = [d for d in q]
        assert len(q) == 2
        datas = [d.data for d in q]
        assert "3" in datas
        assert "4" in datas


class TestInOperator(BaseQuerySetUsage):
    @execute_count(1)
    def test_kwarg_success_case(self):
        """ Tests the in operator works with the kwarg query method """
        q = TestModel.filter(test_id__in=[0, 1])
        assert q.count(self.conn) == 8

    @execute_count(7)
    def test_bool(self):
        """
        Adding coverage to cqlengine for bool types.

        @since 3.6
        @jira_ticket PYTHON-596
        @expected_result bool results should be filtered appropriately

        @test_category object_mapper
        """

        class bool_model(Model):
            k = columns.Integer(primary_key=True)
            b = columns.Boolean(primary_key=True)
            v = columns.Integer(default=3)

        sync_table(self.conn, bool_model)

        bool_model.create(self.conn, k=0, b=True)
        bool_model.create(self.conn, k=0, b=False)
        self.assertEqual(len(bool_model.objects.find_all(self.conn)), 2)
        self.assertEqual(len(bool_model.objects.filter(k=0, b=True).find_all(self.conn)), 1)
        self.assertEqual(len(bool_model.objects.filter(k=0, b=False).find_all(self.conn)), 1)
        drop_table(self.conn, bool_model)

    @execute_count(5)
    def test_bool_filter(self):
        """
        Test to ensure that we don't translate boolean objects to String
        unnecessarily in filter clauses

        @since 3.6
        @jira_ticket PYTHON-596
        @expected_result We should not receive a server error

        @test_category object_mapper
        """

        class bool_model2(Model):
            k = columns.Boolean(primary_key=True)
            b = columns.Integer(primary_key=True)
            v = columns.Text()

        sync_table(self.conn, bool_model2)

        bool_model2.create(self.conn, k=True, b=1, v="a")
        bool_model2.create(self.conn, k=False, b=1, v="b")
        q = bool_model2.objects(k__in=(True, False))
        self.assertEqual(len(q.find_all(self.conn)), 2)
        drop_table(self.conn, bool_model2)


class TestContainsOperator(BaseQuerySetUsage):
    @execute_count(6)
    def test_kwarg_success_case(self):
        """ Tests the CONTAINS operator works with the kwarg query method """
        q = IndexedCollectionsTestModel.filter(test_list__contains=1)
        self.assertEqual(q.count(self.conn), 2)

        q = IndexedCollectionsTestModel.filter(test_list__contains=13)
        self.assertEqual(q.count(self.conn), 0)

        q = IndexedCollectionsTestModel.filter(test_set__contains=3)
        self.assertEqual(q.count(self.conn), 2)

        q = IndexedCollectionsTestModel.filter(test_set__contains=13)
        self.assertEqual(q.count(self.conn), 0)

        q = IndexedCollectionsTestModel.filter(test_map__contains=42)
        self.assertEqual(q.count(self.conn), 1)

        q = IndexedCollectionsTestModel.filter(test_map__contains=13)
        self.assertEqual(q.count(self.conn), 0)

        with self.assertRaises(QueryException):
            q = IndexedCollectionsTestModel.filter(test_list_no_index__contains=1)
            self.assertEqual(q.count(self.conn), 0)
        with self.assertRaises(QueryException):
            q = IndexedCollectionsTestModel.filter(test_set_no_index__contains=1)
            self.assertEqual(q.count(self.conn), 0)
        with self.assertRaises(QueryException):
            q = IndexedCollectionsTestModel.filter(test_map_no_index__contains=1)
            self.assertEqual(q.count(self.conn), 0)


class TestValuesList(BaseQuerySetUsage):
    @execute_count(2)
    def test_values_list(self):
        q = TestModel.objects.filter(test_id=0, attempt_id=1)
        item = q.values_list(
            "test_id", "attempt_id", "description", "expected_result", "test_result"
        ).first(self.conn)
        assert item == [0, 1, "try2", 10, 30]

        item = q.values_list("expected_result", flat=True).first(self.conn)
        assert item == 10


class TestObjectsProperty(BaseQuerySetUsage):
    @execute_count(1)
    def test_objects_property_returns_fresh_queryset(self):
        assert TestModel.objects._result_cache is None
        len(TestModel.objects.find_all(self.conn))  # evaluate queryset
        assert TestModel.objects._result_cache is None


class PageQueryTests(BaseCassEngTestCase):
    @execute_count(5)
    def test_paged_result_handling(self):
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest(
                "Paging requires native protocol 2+, "
                "currently using: {0}".format(PROTOCOL_VERSION)
            )

        # addresses #225
        class PagingTest(Model):
            id = columns.Integer(primary_key=True)
            val = columns.Integer()

        sync_table(self.conn, PagingTest)

        PagingTest.create(self.conn, id=1, val=1)
        PagingTest.create(self.conn, id=2, val=2)

        with mock.patch.object(self.conn.session, "default_fetch_size", 1):
            results = PagingTest.objects().find_all(self.conn)

        assert len(results) == 2
        drop_table(self.conn, PagingTest)


class ModelQuerySetTimeoutTestCase(BaseQuerySetUsage):
    def test_default_timeout(self):
        with mock.patch.object(self.conn.session, "execute") as mock_execute:
            TestModel.objects().find_all(self.conn)
            self.assertEqual(mock_execute.call_args[-1]["timeout"], TIMEOUT_NOT_SET)

    def test_float_timeout(self):
        with mock.patch.object(self.conn.session, "execute") as mock_execute:
            TestModel.objects().timeout(0.5).find_all(self.conn)
            self.assertEqual(mock_execute.call_args[-1]["timeout"], 0.5)

    def test_none_timeout(self):
        with mock.patch.object(self.conn.session, "execute") as mock_execute:
            TestModel.objects().timeout(None).find_all(self.conn)
            self.assertEqual(mock_execute.call_args[-1]["timeout"], None)


class DMLQueryTimeoutTestCase(BaseQuerySetUsage):
    def setUp(self):
        self.model = TestModel(test_id=1, attempt_id=1, description="timeout test")
        super(DMLQueryTimeoutTestCase, self).setUp()

    def test_default_timeout(self):
        with mock.patch.object(self.conn.session, "execute") as mock_execute:
            self.model.save(self.conn)
            self.assertEqual(mock_execute.call_args[-1]["timeout"], TIMEOUT_NOT_SET)

    def test_float_timeout(self):
        with mock.patch.object(self.conn.session, "execute") as mock_execute:
            self.model.timeout(0.5).save(self.conn)
            self.assertEqual(mock_execute.call_args[-1]["timeout"], 0.5)

    def test_none_timeout(self):
        with mock.patch.object(self.conn.session, "execute") as mock_execute:
            self.model.timeout(None).save(self.conn)
            self.assertEqual(mock_execute.call_args[-1]["timeout"], None)


class DBFieldModel(Model):
    k0 = columns.Integer(partition_key=True, db_field="a")
    k1 = columns.Integer(partition_key=True, db_field="b")
    c0 = columns.Integer(primary_key=True, db_field="c")
    v0 = columns.Integer(db_field="d")
    v1 = columns.Integer(db_field="e", index=True)


class DBFieldModelMixed1(Model):
    k0 = columns.Integer(partition_key=True, db_field="a")
    k1 = columns.Integer(partition_key=True)
    c0 = columns.Integer(primary_key=True, db_field="c")
    v0 = columns.Integer(db_field="d")
    v1 = columns.Integer(index=True)


class DBFieldModelMixed2(Model):
    k0 = columns.Integer(partition_key=True)
    k1 = columns.Integer(partition_key=True, db_field="b")
    c0 = columns.Integer(primary_key=True)
    v0 = columns.Integer(db_field="d")
    v1 = columns.Integer(index=True, db_field="e")


class TestModelQueryWithDBField(BaseCassEngTestCase):
    def setUp(self):
        super(TestModelQueryWithDBField, self).setUp()
        self.model_list = [DBFieldModel, DBFieldModelMixed1, DBFieldModelMixed2]
        for model in self.model_list:
            sync_table(self.conn, model)

    def tearDown(self):
        super(TestModelQueryWithDBField, self).tearDown()
        for model in self.model_list:
            drop_table(self.conn, model)

    @execute_count(33)
    def test_basic_crud(self):
        """
        Tests creation update and delete of object model queries that are
        using db_field mappings.

        @since 3.1
        @jira_ticket PYTHON-351
        @expected_result results are properly retrieved without errors

        @test_category object_mapper
        """
        for model in self.model_list:
            values = {"k0": 1, "k1": 2, "c0": 3, "v0": 4, "v1": 5}
            # create
            i = model.create(self.conn, **values)
            i = model.objects(k0=i.k0, k1=i.k1).first(self.conn)
            self.assertEqual(i, model(**values))

            # create
            values["v0"] = 101
            i.update(self.conn, v0=values["v0"])
            i = model.objects(k0=i.k0, k1=i.k1).first(self.conn)
            self.assertEqual(i, model(**values))

            # delete
            model.objects(k0=i.k0, k1=i.k1).delete(self.conn)
            i = model.objects(k0=i.k0, k1=i.k1).first(self.conn)
            self.assertIsNone(i)

            i = model.create(self.conn, **values)
            i = model.objects(k0=i.k0, k1=i.k1).first(self.conn)
            self.assertEqual(i, model(**values))
            i.delete(self.conn)
            model.objects(k0=i.k0, k1=i.k1).delete(self.conn)
            i = model.objects(k0=i.k0, k1=i.k1).first(self.conn)
            self.assertIsNone(i)

    @execute_count(15)
    def test_order(self):
        """
        Tests order by queries for object models that are using db_field
        mapping

        @since 3.1
        @jira_ticket PYTHON-351
        @expected_result results are properly retrieved without errors

        @test_category object_mapper
        """
        for model in self.model_list:
            values = {"k0": 1, "k1": 4, "c0": 3, "v0": 4, "v1": 5}
            clustering_values = range(3)
            for c in clustering_values:
                values["c0"] = c
                i = model.create(self.conn, **values)
            self.assertEqual(
                model.objects(k0=i.k0, k1=i.k1).order_by("c0").first(self.conn).c0,
                clustering_values[0],
            )
            self.assertEqual(
                model.objects(k0=i.k0, k1=i.k1).order_by("-c0").first(self.conn).c0,
                clustering_values[-1],
            )

    @execute_count(15)
    def test_index(self):
        """
        Tests queries using index fields for object models using db_field
        mapping

        @since 3.1
        @jira_ticket PYTHON-351
        @expected_result results are properly retrieved without errors

        @test_category object_mapper
        """
        for model in self.model_list:
            values = {"k0": 1, "k1": 5, "c0": 3, "v0": 4, "v1": 5}
            clustering_values = range(3)
            for c in clustering_values:
                values["c0"] = c
                values["v1"] = c
                i = model.create(self.conn, **values)
            self.assertEqual(
                model.objects(k0=i.k0, k1=i.k1).count(self.conn), len(clustering_values)
            )
            self.assertEqual(model.objects(k0=i.k0, k1=i.k1, v1=0).count(self.conn), 1)

    @execute_count(1)
    def test_db_field_names_used(self):
        """
        Tests to ensure that with generated cql update statements correctly
        utilize the db_field values.

        @since 3.2
        @jira_ticket PYTHON-530
        @expected_result resulting cql_statements will use the db_field values

        @test_category object_mapper
        """

        values = ("k0", "k1", "c0", "v0", "v1")
        # Test QuerySet Path
        b_conn_1 = Batch(self.conn)
        DBFieldModel.objects(k0=1).update(b_conn_1, v0=0, v1=9)
        for value in values:
            self.assertTrue(value not in str(b_conn_1.queries[0]))

        # Test DML path
        b_conn_2 = Batch(self.conn)
        dml_field_model = DBFieldModel.create(self.conn, k0=1, k1=5, c0=3, v0=4, v1=5)
        dml_field_model.update(b_conn_2, v0=0, v1=9)
        for value in values:
            self.assertTrue(value not in str(b_conn_2.queries[0]))


class TestModelSmall(Model):

    test_id = columns.Integer(primary_key=True)


class TestModelQueryWithFetchSize(BaseCassEngTestCase):
    """
    Test FetchSize, and ensure that results are returned correctly
    regardless of the paging size

    @since 3.1
    @jira_ticket PYTHON-324
    @expected_result results are properly retrieved and the correct size

    @test_category object_mapper
    """

    @classmethod
    def setUpClass(cls):
        super(TestModelQueryWithFetchSize, cls).setUpClass()
        sync_table(cls.connection(), TestModelSmall)

    @classmethod
    def tearDownClass(cls):
        super(TestModelQueryWithFetchSize, cls).tearDownClass()
        drop_table(cls.connection(), TestModelSmall)

    @execute_count(9)
    def test_defaultFetchSize(self):
        with Batch(self.conn) as b_conn:
            for i in range(5100):
                TestModelSmall.create(b_conn, test_id=i)
        self.assertEqual(len(TestModelSmall.objects.fetch_size(1).find_all(self.conn)), 5100)
        self.assertEqual(len(TestModelSmall.objects.fetch_size(500).find_all(self.conn)), 5100)
        self.assertEqual(len(TestModelSmall.objects.fetch_size(4999).find_all(self.conn)), 5100)
        self.assertEqual(len(TestModelSmall.objects.fetch_size(5000).find_all(self.conn)), 5100)
        self.assertEqual(len(TestModelSmall.objects.fetch_size(5001).find_all(self.conn)), 5100)
        self.assertEqual(len(TestModelSmall.objects.fetch_size(5100).find_all(self.conn)), 5100)
        self.assertEqual(len(TestModelSmall.objects.fetch_size(5101).find_all(self.conn)), 5100)
        self.assertEqual(len(TestModelSmall.objects.fetch_size(1).find_all(self.conn)), 5100)

        with self.assertRaises(QueryException):
            TestModelSmall.objects.fetch_size(0)
        with self.assertRaises(QueryException):
            TestModelSmall.objects.fetch_size(-1)


class People(Model):
    __table_name__ = "people"
    last_name = columns.Text(primary_key=True, partition_key=True)
    first_name = columns.Text(primary_key=True)
    birthday = columns.DateTime()


class People2(Model):
    __table_name__ = "people"
    last_name = columns.Text(primary_key=True, partition_key=True)
    first_name = columns.Text(primary_key=True)
    middle_name = columns.Text()
    birthday = columns.DateTime()


class TestModelQueryWithDifferedFeld(BaseCassEngTestCase):
    """
    Tests that selects with filter will deffer population of known values until after the results are returned.
    I.E.  Instead of generating SELECT * FROM People WHERE last_name="Smith" It will generate
    SELECT first_name, birthday FROM People WHERE last_name="Smith"
    Where last_name 'smith' will populated post query

    @since 3.2
    @jira_ticket PYTHON-520
    @expected_result only needed fields are included in the query

    @test_category object_mapper
    """

    @classmethod
    def setUpClass(cls):
        super(TestModelQueryWithDifferedFeld, cls).setUpClass()
        sync_table(cls.connection(), People)

    @classmethod
    def tearDownClass(cls):
        super(TestModelQueryWithDifferedFeld, cls).tearDownClass()
        drop_table(cls.connection(), People)

    @execute_count(9)
    def test_defaultFetchSize(self):
        # Populate Table
        People.objects.create(
            self.conn, last_name="Smith", first_name="John", birthday=datetime.now()
        )
        People.objects.create(
            self.conn, last_name="Bestwater", first_name="Alan", birthday=datetime.now()
        )
        People.objects.create(
            self.conn, last_name="Smith", first_name="Greg", birthday=datetime.now()
        )
        People.objects.create(
            self.conn, last_name="Smith", first_name="Adam", birthday=datetime.now()
        )

        # Check query constructions
        expected_fields = ["first_name", "birthday"]
        self.assertEqual(People.filter(last_name="Smith")._select_fields(), expected_fields)
        # Validate correct fields are fetched
        smiths = People.filter(last_name="Smith").find_all(self.conn)
        self.assertEqual(len(smiths), 3)
        self.assertTrue(smiths[0].last_name is not None)

        # Modify table with new value
        sync_table(self.conn, People2)

        # populate new format
        People2.objects.create(
            self.conn,
            last_name="Smith",
            first_name="Chris",
            middle_name="Raymond",
            birthday=datetime.now(),
        )
        People2.objects.create(
            self.conn,
            last_name="Smith",
            first_name="Andrew",
            middle_name="Micheal",
            birthday=datetime.now(),
        )

        # validate query construction
        expected_fields = ["first_name", "middle_name", "birthday"]
        self.assertEqual(People2.filter(last_name="Smith")._select_fields(), expected_fields)

        # validate correct items are returneds
        smiths = People2.filter(last_name="Smith").find_all(self.conn)
        self.assertEqual(len(smiths), 5)
        self.assertTrue(smiths[0].last_name is not None)


if __name__ == "__main__":
    main()
