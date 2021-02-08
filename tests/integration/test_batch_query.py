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
import warnings

from unittest import mock

from cqlmapper import columns
from cqlmapper import TIMEOUT_NOT_SET
from cqlmapper.batch import Batch
from cqlmapper.management import drop_table
from cqlmapper.management import sync_table
from cqlmapper.models import Model
from tests.integration import execute_count
from tests.integration.base import BaseCassEngTestCase


class TestMultiKeyModel(Model):
    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False)


class BatchLogModel(Model):

    # simple k/v table
    k = columns.Integer(primary_key=True)
    v = columns.Integer()


class BatchTests(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(BatchTests, cls).setUpClass()
        conn = cls.connection()
        drop_table(conn, TestMultiKeyModel)
        sync_table(conn, TestMultiKeyModel)

    @classmethod
    def tearDownClass(cls):
        super(BatchTests, cls).tearDownClass()
        conn = cls.connection()
        drop_table(conn, TestMultiKeyModel)

    def setUp(self):
        super(BatchTests, self).setUp()
        self.pkey = 1

        def clean_up():
            for obj in TestMultiKeyModel.objects.find(self.conn, partition=self.pkey):
                obj.delete(self.conn)

        self.addCleanup(clean_up)

    @execute_count(3)
    def test_insert_success_case(self):
        b_conn = Batch(self.conn)
        TestMultiKeyModel.create(b_conn, partition=self.pkey, cluster=2, count=3, text="4")

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)

        b_conn.execute_batch()

        TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)

    def test_context_manager(self):

        with Batch(self.conn) as b_conn:
            for i in range(5):
                TestMultiKeyModel.create(b_conn, partition=self.pkey, cluster=i, count=3, text="4")

            for i in range(5):
                with self.assertRaises(TestMultiKeyModel.DoesNotExist):
                    TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=i)

        for i in range(5):
            TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=i)

    @execute_count(4)
    def test_update_success_case(self):
        inst = TestMultiKeyModel.create(
            self.conn, partition=self.pkey, cluster=2, count=3, text="4"
        )

        with Batch(self.conn) as b_conn:
            inst.count = 4
            inst.save(b_conn)

            inst2 = TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)
            self.assertEqual(inst2.count, 3)

        inst3 = TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)
        self.assertEqual(inst3.count, 4)

    @execute_count(4)
    def test_delete_success_case(self):

        inst = TestMultiKeyModel.create(
            self.conn, partition=self.pkey, cluster=2, count=3, text="4"
        )

        with Batch(self.conn) as b_conn:
            inst.delete(b_conn)
            TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)

    @execute_count(9)
    def test_bulk_delete_success_case(self):

        for i in range(1):
            for j in range(5):
                TestMultiKeyModel.create(
                    self.conn, partition=i, cluster=j, count=i * j, text="{0}:{1}".format(i, j)
                )

        with Batch(self.conn) as b_conn:
            TestMultiKeyModel.filter(partition=0).delete(b_conn)
            self.assertEqual(TestMultiKeyModel.filter(partition=0).count(self.conn), 5)

        self.assertEqual(TestMultiKeyModel.filter(partition=0).count(self.conn), 0)
        # cleanup
        for m in TestMultiKeyModel.objects.iter(self.conn):
            m.delete(self.conn)

    @execute_count(0)
    def test_empty_batch(self):
        b = Batch(self.conn)
        b.execute_batch()

    def test_batch_execute_timeout(self):
        with mock.patch.object(self.conn.session, "execute") as mock_execute:
            with Batch(self.conn, timeout=1) as b_conn:
                BatchLogModel.create(b_conn, k=2, v=2)
            self.assertEqual(mock_execute.call_args[-1]["timeout"], 1)
            self.assertEqual(mock_execute.call_count, 1)

    def test_batch_execute_no_timeout(self):
        with mock.patch.object(self.conn.session, "execute") as mock_execute:
            with Batch(self.conn) as b_conn:
                BatchLogModel.create(b_conn, k=2, v=2)
            self.assertEqual(mock_execute.call_args[-1]["timeout"], TIMEOUT_NOT_SET)
            self.assertEqual(mock_execute.call_count, 1)


class BatchCallbacksTests(BaseCassEngTestCase):
    def test_API_managing_callbacks(self):

        # Callbacks can be added at init and after

        def my_callback(*args, **kwargs):
            pass

        # adding on init:
        batch = Batch(self.conn)

        batch.add_callback(my_callback)
        batch.add_callback(my_callback, 2, named_arg="value")
        batch.add_callback(my_callback, 1, 3)

        self.assertEqual(
            batch._callbacks,
            [
                (my_callback, (), {}),
                (my_callback, (2,), {"named_arg": "value"}),
                (my_callback, (1, 3), {}),
            ],
        )

    def test_callbacks_properly_execute_callables_and_tuples(self):

        call_history = []

        def my_callback(*args, **kwargs):
            call_history.append(args)

        # adding on init:
        batch = Batch(self.conn)

        batch.add_callback(my_callback)
        batch.add_callback(my_callback, "more", "args")

        batch.execute_batch()

        self.assertEqual(len(call_history), 2)
        self.assertEqual([(), ("more", "args")], call_history)

    def test_callbacks_tied_to_execute(self):
        """Batch callbacks should NOT fire if batch is not executed in
        context manager mode"""

        call_history = []

        def my_callback(*args, **kwargs):
            call_history.append(args)

        b_conn = Batch(self.conn)
        b_conn.add_callback(my_callback)
        b_conn.execute_batch()

        self.assertEqual(len(call_history), 1)

        class SomeError(Exception):
            pass

        with self.assertRaises(SomeError):
            with Batch(self.conn) as b_conn:
                b_conn.add_callback(my_callback)
                # this error bubbling up through context manager
                # should prevent callback runs (along with b.execute())
                raise SomeError

        # still same call history. Nothing added
        self.assertEqual(len(call_history), 1)

        # but if execute ran, even with an error bubbling through
        # the callbacks also would have fired
        with self.assertRaises(SomeError):
            with Batch(self.conn, execute_on_exception=True) as b_conn:
                b_conn.add_callback(my_callback)
                raise SomeError

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
            batch = Batch(self.conn)
            batch.add_callback(my_callback)
            batch.execute_batch()
            batch.execute_batch()
        self.assertEqual(len(w), 1)
        self.assertRegexpMatches(str(w[0].message), r"^Batch.*multiple.*")
