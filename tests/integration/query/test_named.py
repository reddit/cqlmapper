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

from cassandra import ConsistencyLevel
from cqlmapper import operators
from cqlmapper.named import NamedKeyspace
from cqlmapper.operators import EqualsOperator, GreaterThanOrEqualOperator
from cqlmapper.query import ResultObject
from cassandra.concurrent import execute_concurrent_with_args
from cqlmapper import models

from tests.integration.cqlengine import setup_connection, execute_count
from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration.cqlengine.query.test_queryset import BaseQuerySetUsage


from tests.integration import BasicSharedKeyspaceUnitTestCase


class TestQuerySetOperation(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestQuerySetOperation, cls).setUpClass()
        cls.table = cls.connection().keyspace.table('test_model')

    def test_query_filter_parsing(self):
        """
        Tests the queryset filter method parses it's kwargs properly
        """
        query1 = self.table.objects(test_id=5)
        assert len(query1._where) == 1

        op = query1._where[0]
        assert isinstance(op.operator, operators.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

        op = query2._where[1]
        assert isinstance(op.operator, operators.GreaterThanOrEqualOperator)
        assert op.value == 1

    def test_query_expression_parsing(self):
        """ Tests that query experessions are evaluated properly """
        query1 = self.table.filter(self.table.column('test_id') == 5)
        assert len(query1._where) == 1

        op = query1._where[0]
        assert isinstance(op.operator, operators.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(self.table.column('expected_result') >= 1)
        assert len(query2._where) == 2

        op = query2._where[1]
        assert isinstance(op.operator, operators.GreaterThanOrEqualOperator)
        assert op.value == 1

    def test_filter_method_where_clause_generation(self):
        """
        Tests the where clause creation
        """
        query1 = self.table.objects(test_id=5)
        self.assertEqual(len(query1._where), 1)
        where = query1._where[0]
        self.assertEqual(where.field, 'test_id')
        self.assertEqual(where.value, 5)

        query2 = query1.filter(expected_result__gte=1)
        self.assertEqual(len(query2._where), 2)

        where = query2._where[0]
        self.assertEqual(where.field, 'test_id')
        self.assertIsInstance(where.operator, EqualsOperator)
        self.assertEqual(where.value, 5)

        where = query2._where[1]
        self.assertEqual(where.field, 'expected_result')
        self.assertIsInstance(where.operator, GreaterThanOrEqualOperator)
        self.assertEqual(where.value, 1)

    def test_query_expression_where_clause_generation(self):
        """
        Tests the where clause creation
        """
        query1 = self.table.objects(self.table.column('test_id') == 5)
        self.assertEqual(len(query1._where), 1)
        where = query1._where[0]
        self.assertEqual(where.field, 'test_id')
        self.assertEqual(where.value, 5)

        query2 = query1.filter(self.table.column('expected_result') >= 1)
        self.assertEqual(len(query2._where), 2)

        where = query2._where[0]
        self.assertEqual(where.field, 'test_id')
        self.assertIsInstance(where.operator, EqualsOperator)
        self.assertEqual(where.value, 5)

        where = query2._where[1]
        self.assertEqual(where.field, 'expected_result')
        self.assertIsInstance(where.operator, GreaterThanOrEqualOperator)
        self.assertEqual(where.value, 1)


class TestQuerySetCountSelectionAndIteration(BaseQuerySetUsage):

    @classmethod
    def setUpClass(cls):
        super(TestQuerySetCountSelectionAndIteration, cls).setUpClass()

        from tests.integration.cqlengine.query.test_queryset import TestModel

        ks, tn = TestModel.column_family_name().split('.')
        cls.keyspace = NamedKeyspace(ks)
        cls.table = cls.keyspace.table(tn)

    @execute_count(2)
    def test_count(self):
        """ Tests that adding filtering statements affects the count query as expected """
        assert self.table.objects.count() == 12

        q = self.table.objects(test_id=0)
        assert q.count() == 4

    @execute_count(2)
    def test_query_expression_count(self):
        """ Tests that adding query statements affects the count query as expected """
        assert self.table.objects.count() == 12

        q = self.table.objects(self.table.column('test_id') == 0)
        assert q.count() == 4

    @execute_count(3)
    def test_iteration(self):
        """ Tests that iterating over a query set pulls back all of the expected results """
        q = self.table.objects(test_id=0)
        # tuple of expected attempt_id, expected_result values
        compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
        for t in q:
            val = t.attempt_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

        # test with regular filtering
        q = self.table.objects(attempt_id=3).allow_filtering()
        assert len(q) == 3
        # tuple of expected test_id, expected_result values
        compare_set = set([(0, 20), (1, 20), (2, 75)])
        for t in q:
            val = t.test_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

        # test with query method
        q = self.table.objects(self.table.column('attempt_id') == 3).allow_filtering()
        assert len(q) == 3
        # tuple of expected test_id, expected_result values
        compare_set = set([(0, 20), (1, 20), (2, 75)])
        for t in q:
            val = t.test_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

    @execute_count(2)
    def test_multiple_iterations_work_properly(self):
        """ Tests that iterating over a query set more than once works """
        # test with both the filtering method and the query method
        for q in (self.table.objects(test_id=0), self.table.objects(self.table.column('test_id') == 0)):
            # tuple of expected attempt_id, expected_result values
            compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
            for t in q:
                val = t.attempt_id, t.expected_result
                assert val in compare_set
                compare_set.remove(val)
            assert len(compare_set) == 0

            # try it again
            compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
            for t in q:
                val = t.attempt_id, t.expected_result
                assert val in compare_set
                compare_set.remove(val)
            assert len(compare_set) == 0

    @execute_count(2)
    def test_multiple_iterators_are_isolated(self):
        """
        tests that the use of one iterator does not affect the behavior of another
        """
        for q in (self.table.objects(test_id=0), self.table.objects(self.table.column('test_id') == 0)):
            q = q.order_by('attempt_id')
            expected_order = [0, 1, 2, 3]
            iter1 = iter(q)
            iter2 = iter(q)
            for attempt_id in expected_order:
                assert next(iter1).attempt_id == attempt_id
                assert next(iter2).attempt_id == attempt_id

    @execute_count(3)
    def test_get_success_case(self):
        """
        Tests that the .get() method works on new and existing querysets
        """
        m = self.table.objects.get(test_id=0, attempt_id=0)
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = self.table.objects(test_id=0, attempt_id=0)
        m = q.get()
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = self.table.objects(test_id=0)
        m = q.get(attempt_id=0)
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

    @execute_count(3)
    def test_query_expression_get_success_case(self):
        """
        Tests that the .get() method works on new and existing querysets
        """
        m = self.table.get(self.table.column('test_id') == 0, self.table.column('attempt_id') == 0)
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = self.table.objects(self.table.column('test_id') == 0, self.table.column('attempt_id') == 0)
        m = q.get()
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = self.table.objects(self.table.column('test_id') == 0)
        m = q.get(self.table.column('attempt_id') == 0)
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

    @execute_count(1)
    def test_get_doesnotexist_exception(self):
        """
        Tests that get calls that don't return a result raises a DoesNotExist error
        """
        with self.assertRaises(self.table.DoesNotExist):
            self.table.objects.get(test_id=100)

    @execute_count(1)
    def test_get_multipleobjects_exception(self):
        """
        Tests that get calls that return multiple results raise a MultipleObjectsReturned error
        """
        with self.assertRaises(self.table.MultipleObjectsReturned):
            self.table.objects.get(test_id=1)
