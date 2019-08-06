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
import six
from uuid import uuid4

from cqlmapper import columns, LWTException
from cqlmapper.batch import Batch
from cqlmapper.management import sync_table, drop_table
from cqlmapper.models import Model
from cqlmapper.statements import ConditionalClause

from tests.integration.base import BaseCassEngTestCase, main
from tests.integration import CASSANDRA_VERSION


class TestConditionalModel(Model):
    id = columns.UUID(primary_key=True, default=uuid4)
    count = columns.Integer()
    text = columns.Text(required=False)


@unittest.skipUnless(
    CASSANDRA_VERSION >= "2.0.0", "conditionals only supported on cassandra 2.0 or higher"
)
class TestConditional(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(TestConditional, cls).setUpClass()
        sync_table(cls.connection(), TestConditionalModel)

    @classmethod
    def tearDownClass(cls):
        super(TestConditional, cls).tearDownClass()
        drop_table(cls.connection(), TestConditionalModel)

    def test_update_using_conditional(self):
        t = TestConditionalModel.create(self.conn, text="blah blah")
        t.text = "new blah"
        with mock.patch.object(self.conn.session, "execute") as m:
            t.iff(text="blah blah").save(self.conn)

        args = m.call_args
        self.assertIn('IF "text" = %(0)s', args[0][0].query_string)

    def test_update_conditional_success(self):
        t = TestConditionalModel.create(self.conn, text="blah blah", count=5)
        id = t.id
        t.text = "new blah"
        t.iff(text="blah blah").save(self.conn)

        updated = TestConditionalModel.objects(id=id).first(self.conn)
        self.assertEqual(updated.count, 5)
        self.assertEqual(updated.text, "new blah")

    def test_update_failure(self):
        t = TestConditionalModel.create(self.conn, text="blah blah")
        t.text = "new blah"
        t = t.iff(text="something wrong")

        with self.assertRaises(LWTException) as assertion:
            t.save(self.conn)

        self.assertEqual(assertion.exception.existing, {"text": "blah blah", "[applied]": False})

    def test_blind_update(self):
        t = TestConditionalModel.create(self.conn, text="blah blah")
        t.text = "something else"
        uid = t.id

        with mock.patch.object(self.conn.session, "execute") as m:
            TestConditionalModel.objects(id=uid).iff(text="blah blah").update(
                self.conn, text="oh hey der"
            )

        args = m.call_args
        self.assertIn('IF "text" = %(1)s', args[0][0].query_string)

    def test_blind_update_fail(self):
        t = TestConditionalModel.create(self.conn, text="blah blah")
        t.text = "something else"
        uid = t.id
        qs = TestConditionalModel.objects(id=uid).iff(text="Not dis!")
        with self.assertRaises(LWTException) as assertion:
            qs.update(self.conn, text="this will never work")

        self.assertEqual(assertion.exception.existing, {"text": "blah blah", "[applied]": False})

    def test_conditional_clause(self):
        tc = ConditionalClause("some_value", 23)
        tc.set_context_id(3)

        self.assertEqual('"some_value" = %(3)s', six.text_type(tc))
        self.assertEqual('"some_value" = %(3)s', str(tc))

    def test_batch_update_conditional(self):
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        id = t.id
        with Batch(self.conn) as b_conn:
            t.iff(count=5).update(b_conn, text="something else")

        updated = TestConditionalModel.objects(id=id).first(self.conn)
        self.assertEqual(updated.text, "something else")

        with self.assertRaises(LWTException) as assertion:
            with Batch(self.conn) as b_conn:
                updated.iff(count=6).update(b_conn, text="and another thing")

        self.assertEqual(assertion.exception.existing, {"id": id, "count": 5, "[applied]": False})

        updated = TestConditionalModel.objects(id=id).first(self.conn)
        self.assertEqual(updated.text, "something else")

    def test_delete_conditional(self):
        # DML path
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 1)
        with self.assertRaises(LWTException):
            t.iff(count=9999).delete(self.conn)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 1)
        t.iff(count=5).delete(self.conn)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 0)

        # QuerySet path
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 1)
        with self.assertRaises(LWTException):
            TestConditionalModel.objects(id=t.id).iff(count=9999).delete(self.conn)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 1)
        TestConditionalModel.objects(id=t.id).iff(count=5).delete(self.conn)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 0)

    def test_delete_lwt_ne(self):
        """
        Test to ensure that deletes using IF and not equals are honored
        correctly

        @since 3.2
        @jira_ticket PYTHON-328
        @expected_result Delete conditional with NE should be honored

        @test_category object_mapper
        """

        # DML path
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 1)
        with self.assertRaises(LWTException):
            t.iff(count__ne=5).delete(self.conn)
        t.iff(count__ne=2).delete(self.conn)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 0)

        # QuerySet path
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 1)
        with self.assertRaises(LWTException):
            TestConditionalModel.objects(id=t.id).iff(count__ne=5).delete(self.conn)
        TestConditionalModel.objects(id=t.id).iff(count__ne=2).delete(self.conn)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 0)

    def test_update_lwt_ne(self):
        """
        Test to ensure that update using IF and not equals are honored
        correctly

        @since 3.2
        @jira_ticket PYTHON-328
        @expected_result update conditional with NE should be honored

        @test_category object_mapper
        """

        # DML path
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 1)
        with self.assertRaises(LWTException):
            t.iff(count__ne=5).update(self.conn, text="nothing")
        t.iff(count__ne=2).update(self.conn, text="nothing")
        self.assertEqual(TestConditionalModel.objects(id=t.id).first(self.conn).text, "nothing")
        t.delete(self.conn)

        # QuerySet path
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 1)
        with self.assertRaises(LWTException):
            TestConditionalModel.objects(id=t.id).iff(count__ne=5).update(self.conn, text="nothing")
        TestConditionalModel.objects(id=t.id).iff(count__ne=2).update(self.conn, text="nothing")
        self.assertEqual(TestConditionalModel.objects(id=t.id).first(self.conn).text, "nothing")
        t.delete(self.conn)

    def test_update_to_none(self):
        # This test is done because updates to none are split into deletes
        # for old versions of cassandra. Can be removed when we drop that code
        # https://github.com/datastax/python-driver/blob/3.1.1/cassandra/cqlengine/query.py#L1197-L1200

        # DML path
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 1)
        with self.assertRaises(LWTException):
            t.iff(count=9999).update(self.conn, text=None)
        self.assertIsNotNone(TestConditionalModel.objects(id=t.id).first(self.conn).text)
        t.iff(count=5).update(self.conn, text=None)
        self.assertIsNone(TestConditionalModel.objects(id=t.id).first(self.conn).text)

        # QuerySet path
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        self.assertEqual(TestConditionalModel.objects(id=t.id).count(self.conn), 1)
        with self.assertRaises(LWTException):
            TestConditionalModel.objects(id=t.id).iff(count=9999).update(self.conn, text=None)
        self.assertIsNotNone(TestConditionalModel.objects(id=t.id).first(self.conn).text)
        TestConditionalModel.objects(id=t.id).iff(count=5).update(self.conn, text=None)
        self.assertIsNone(TestConditionalModel.objects(id=t.id).first(self.conn).text)

    def test_column_delete_after_update(self):
        # DML path
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        t.iff(count=5).update(self.conn, text=None, count=6)

        self.assertIsNone(t.text)
        self.assertEqual(t.count, 6)

        # QuerySet path
        t = TestConditionalModel.create(self.conn, text="something", count=5)
        TestConditionalModel.objects(id=t.id).iff(count=5).update(self.conn, text=None, count=6)

        self.assertIsNone(TestConditionalModel.objects(id=t.id).first(self.conn).text)
        self.assertEqual(TestConditionalModel.objects(id=t.id).first(self.conn).count, 6)


if __name__ == "__main__":
    main()
