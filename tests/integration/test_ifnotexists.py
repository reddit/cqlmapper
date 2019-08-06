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

from unittest import mock
from uuid import uuid4

from cqlmapper import columns, LWTException
from cqlmapper.batch import Batch
from cqlmapper.management import sync_table, drop_table
from cqlmapper.models import Model
from cqlmapper.query import IfNotExistsWithCounterColumn
from cqlmapper.query_set import ModelQuerySet

from tests.integration.base import BaseCassEngTestCase
from tests.integration import PROTOCOL_VERSION


class TestIfNotExistsModel(Model):

    id = columns.UUID(primary_key=True, default=uuid4)
    count = columns.Integer()
    text = columns.Text(required=False)


class TestIfNotExistsWithCounterModel(Model):

    id = columns.UUID(primary_key=True, default=uuid4)
    likes = columns.Counter()


class BaseIfNotExistsTest(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(BaseIfNotExistsTest, cls).setUpClass()
        """
        when receiving an insert statement with 'if not exist', cassandra would
        perform a read with QUORUM level. Unittest would be failed if
        replica_factor is 3 and one node only. Therefore I have create a new
        keyspace with replica_factor:1.
        """
        sync_table(cls.connection(), TestIfNotExistsModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseIfNotExistsTest, cls).tearDownClass()
        drop_table(cls.connection(), TestIfNotExistsModel)


class BaseIfNotExistsWithCounterTest(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(BaseIfNotExistsWithCounterTest, cls).setUpClass()
        sync_table(cls.connection(), TestIfNotExistsWithCounterModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseIfNotExistsWithCounterTest, cls).tearDownClass()
        drop_table(cls.connection(), TestIfNotExistsWithCounterModel)


class IfNotExistsInsertTests(BaseIfNotExistsTest):
    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_insert_if_not_exists(self):
        """ tests that insertion with if_not_exists work as expected """

        id = uuid4()

        TestIfNotExistsModel.create(self.conn, id=id, count=8, text="123456789")

        with self.assertRaises(LWTException) as assertion:
            TestIfNotExistsModel.if_not_exists().create(
                self.conn, id=id, count=9, text="111111111111"
            )

        with self.assertRaises(LWTException) as assertion:
            TestIfNotExistsModel.objects(count=9, text="111111111111").if_not_exists().create(
                self.conn, id=id
            )

        self.assertEqual(
            assertion.exception.existing,
            {"count": 8, "id": id, "text": "123456789", "[applied]": False},
        )

        q = TestIfNotExistsModel.objects(id=id)
        self.assertEqual(len(q.find_all(self.conn)), 1)

        tm = q.first(self.conn)
        self.assertEqual(tm.count, 8)
        self.assertEqual(tm.text, "123456789")

    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_batch_insert_if_not_exists(self):
        """ tests that batch insertion with if_not_exists work as expected """

        id = uuid4()

        with Batch(self.conn) as b_conn:
            TestIfNotExistsModel.if_not_exists().create(b_conn, id=id, count=8, text="123456789")
        with self.assertRaises(LWTException) as assertion:
            with Batch(self.conn) as b_conn:
                TestIfNotExistsModel.if_not_exists().create(
                    b_conn, id=id, count=9, text="111111111111"
                )

        self.assertEqual(
            assertion.exception.existing,
            {"count": 8, "id": id, "text": "123456789", "[applied]": False},
        )

        q = TestIfNotExistsModel.objects(id=id)
        self.assertEqual(len(q.find_all(self.conn)), 1)

        tm = q.first(self.conn)
        self.assertEqual(tm.count, 8)
        self.assertEqual(tm.text, "123456789")


class IfNotExistsModelTest(BaseIfNotExistsTest):
    def test_if_not_exists_included_on_create(self):
        """ tests that if_not_exists on models works as expected """

        with mock.patch.object(self.conn.session, "execute") as m:
            TestIfNotExistsModel.if_not_exists().create(self.conn, count=8)

        query = m.call_args[0][0].query_string
        self.assertIn("IF NOT EXISTS", query)

    def test_if_not_exists_included_on_save(self):
        """ tests if we correctly put 'IF NOT EXISTS' for insert statement """

        with mock.patch.object(self.conn.session, "execute") as m:
            tm = TestIfNotExistsModel(count=8)
            tm.if_not_exists().save(self.conn)

        query = m.call_args[0][0].query_string
        self.assertIn("IF NOT EXISTS", query)

    def test_queryset_is_returned_on_class(self):
        """ ensure we get a queryset description back """
        qs = TestIfNotExistsModel.if_not_exists()
        self.assertTrue(isinstance(qs, ModelQuerySet), type(qs))

    def test_batch_if_not_exists(self):
        """ ensure 'IF NOT EXISTS' exists in statement when in batch """
        with mock.patch.object(self.conn.session, "execute") as m:
            with Batch(self.conn) as b_conn:
                TestIfNotExistsModel.if_not_exists().create(b_conn, count=8)

        self.assertIn("IF NOT EXISTS", m.call_args[0][0].query_string)


class IfNotExistsInstanceTest(BaseIfNotExistsTest):
    def test_instance_is_returned(self):
        """
        ensures that we properly handle the instance.if_not_exists().save()
        scenario
        """
        o = TestIfNotExistsModel.create(self.conn, text="whatever")
        o.text = "new stuff"
        o = o.if_not_exists()
        self.assertEqual(True, o._if_not_exists)

    def test_if_not_exists_is_not_include_with_query_on_update(self):
        """
        make sure we don't put 'IF NOT EXIST' in update statements
        """
        o = TestIfNotExistsModel.create(self.conn, text="whatever")
        o.text = "new stuff"
        o = o.if_not_exists()

        with mock.patch.object(self.conn.session, "execute") as m:
            o.save(self.conn)

        query = m.call_args[0][0].query_string
        self.assertNotIn("IF NOT EXIST", query)


class IfNotExistWithCounterTest(BaseIfNotExistsWithCounterTest):
    def test_instance_raise_exception(self):
        """ make sure exception is raised when calling
        if_not_exists on table with counter column
        """
        id = uuid4()
        with self.assertRaises(IfNotExistsWithCounterColumn):
            TestIfNotExistsWithCounterModel.if_not_exists()
