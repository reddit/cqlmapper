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
import warnings

from cqlmapper import columns
from cqlmapper.management import drop_table, sync_table
from cqlmapper.models import Model
from cqlmapper.query import BatchQuery
from tests.integration.base import BaseCassEngTestCase


class TestMultiKeyModel(Model):
    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False)


class BatchQueryTests(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BatchQueryTests, cls).setUpClass()
        conn = cls.connection()
        drop_table(conn, TestMultiKeyModel)
        sync_table(conn, TestMultiKeyModel)

    @classmethod
    def tearDownClass(cls):
        super(BatchQueryTests, cls).tearDownClass()
        conn = cls.connection()
        drop_table(conn, TestMultiKeyModel)

    def setUp(self):
        super(BatchQueryTests, self).setUp()
        self.pkey = 1

        def clean_up():
            for obj in TestMultiKeyModel.objects.find(self.conn, partition=self.pkey):
                obj.delete(self.conn)

        self.addCleanup(clean_up)

    def test_insert_success_case(self):
        b = BatchQuery()
        TestMultiKeyModel.batch(b).create(
            self.conn,
            partition=self.pkey,
            cluster=2,
            count=3,
            text='4',
        )

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)

        self.conn.execute_query(b)

        TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)

    def test_update_success_case(self):
        inst = TestMultiKeyModel.create(
            self.conn,
            partition=self.pkey,
            cluster=2,
            count=3,
            text='4',
        )

        b = BatchQuery()

        inst.count = 4
        inst.batch(b).save(self.conn)

        inst2 = TestMultiKeyModel.get(
            self.conn,
            partition=self.pkey,
            cluster=2,
        )
        self.assertEqual(inst2.count, 3)

        self.conn.execute_query(b)

        inst3 = TestMultiKeyModel.get(
            self.conn,
            partition=self.pkey,
            cluster=2,
        )
        self.assertEqual(inst3.count, 4)

    def test_delete_success_case(self):

        inst = TestMultiKeyModel.create(
            self.conn,
            partition=self.pkey,
            cluster=2,
            count=3,
            text='4',
        )

        b = BatchQuery()

        inst.batch(b).delete(self.conn)

        TestMultiKeyModel.get(
            self.conn,
            partition=self.pkey,
            cluster=2,
        )

        self.conn.execute_query(b)

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)

    def test_bulk_delete_success_case(self):

        for i in range(1):
            for j in range(5):
                TestMultiKeyModel.create(
                    self.conn,
                    partition=i,
                    cluster=j,
                    count=i * j,
                    text='{0}:{1}'.format(i, j),
                )

        b = BatchQuery()
        TestMultiKeyModel.batch(b).filter(partition=0).delete(self.conn)
        self.assertEqual(
            TestMultiKeyModel.filter(partition=0).count(self.conn), 5
        )
        self.conn.execute_query(b)
        self.assertEqual(
            TestMultiKeyModel.filter(partition=0).count(self.conn), 0
        )
        # cleanup
        for m in TestMultiKeyModel.objects.iter(self.conn):
            m.delete(self.conn)

    def test_empty_batch(self):
        b = BatchQuery()
        self.conn.execute_query(b)


class BatchQueryCallbacksTests(BaseCassEngTestCase):

    def test_API_managing_callbacks(self):

        # Callbacks can be added at init and after

        def my_callback(*args, **kwargs):
            pass

        # adding on init:
        batch = BatchQuery()

        batch.add_callback(my_callback)
        batch.add_callback(my_callback, 2, named_arg='value')
        batch.add_callback(my_callback, 1, 3)

        self.assertEqual(batch._callbacks, [
            (my_callback, (), {}),
            (my_callback, (2,), {'named_arg':'value'}),
            (my_callback, (1, 3), {})
        ])

    def test_callbacks_properly_execute_callables_and_tuples(self):

        call_history = []

        def my_callback(*args, **kwargs):
            call_history.append(args)

        # adding on init:
        batch = BatchQuery()

        batch.add_callback(my_callback)
        batch.add_callback(my_callback, 'more', 'args')

        self.conn.execute_query(batch)

        self.assertEqual(len(call_history), 2)
        self.assertEqual([(), ('more', 'args')], call_history)

    def test_callbacks_tied_to_execute(self):
        """Batch callbacks should NOT fire if batch is not executed in
        context manager mode"""

        call_history = []

        def my_callback(*args, **kwargs):
            call_history.append(args)

        batch = BatchQuery()
        batch.add_callback(my_callback)
        self.conn.execute_query(batch)

        self.assertEqual(len(call_history), 1)

        class SomeError(Exception):
            pass

        def prepare_mock(*a, **kw):
            raise SomeError

        with self.assertRaises(SomeError):
            batch = BatchQuery()
            batch.prepare = prepare_mock
            batch.add_callback(my_callback)
            # this error bubbling up through context manager
            # should prevent callback runs (along with b.execute())
            self.conn.execute_query(batch)

        # still same call history. Nothing added
        self.assertEqual(len(call_history), 1)

        # but if execute ran, even with an error bubbling through
        # the callbacks also would have fired
        with self.assertRaises(SomeError):
            batch = BatchQuery(execute_on_exception=True)
            batch.prepare = prepare_mock
            batch.add_callback(my_callback)
            self.conn.execute_query(batch)

        # updated call history
        self.assertEqual(len(call_history), 2)

    def test_callbacks_work_multiple_times(self):
        """
        Tests that multiple executions of execute on a batch statement
        logs a warning, and that we don't encounter an attribute error.
        @since 3.1
        @jira_ticket PYTHON-445
        @expected_result warning message is logged

        @test_category object_mapper
        """
        call_history = []

        def my_callback(*args, **kwargs):
            call_history.append(args)

        with warnings.catch_warnings(record=True) as w:
            batch = BatchQuery()
            batch.add_callback(my_callback)
            self.conn.execute_query(batch)
            self.conn.execute_query(batch)
        self.assertEqual(len(w), 1)
        self.assertRegexpMatches(str(w[0].message), r"^Batch.*multiple.*")

