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

import mock

from cqlmapper import columns, TIMEOUT_NOT_SET
from cqlmapper.management import drop_table, sync_table
from cqlmapper.models import Model
from cqlmapper.query import BatchQuery, DMLQuery
from tests.integration.base import BaseCassEngTestCase
from tests.integration import execute_count


class TestMultiKeyModel(Model):

    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False)


class BatchQueryLogModel(Model):

    # simple k/v table
    k = columns.Integer(primary_key=True)
    v = columns.Integer()


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
        drop_table(cls.connection(), TestMultiKeyModel)

    def setUp(self):
        super(BatchQueryTests, self).setUp()
        self.pkey = 1
        for obj in TestMultiKeyModel.filter(partition=self.pkey).iter(self.conn):
            obj.delete(self.conn)

    @execute_count(3)
    def test_insert_success_case(self):

        b = BatchQuery()
        inst = TestMultiKeyModel.batch(b).create(
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

    @execute_count(4)
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

    @execute_count(4)
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

        TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)

        self.conn.execute_query(b)

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(self.conn, partition=self.pkey, cluster=2)

    @execute_count(9)
    def test_bulk_delete_success_case(self):

        for i in range(1):
            for j in range(5):
                TestMultiKeyModel.create(
                    self.conn,
                    partition=i,
                    cluster=j,
                    count=i*j,
                    text='{0}:{1}'.format(i,j),
                )

        b = BatchQuery()
        TestMultiKeyModel.objects.batch(b).filter(
            partition=0
        ).delete(self.conn)
        self.assertEqual(
            TestMultiKeyModel.filter(partition=0).count(self.conn), 5
        )
        self.conn.execute_query(b)
        self.assertEqual(
            TestMultiKeyModel.filter(partition=0).count(self.conn), 0)
        #cleanup
        for m in TestMultiKeyModel.all(self.conn):
            m.delete(self.conn)

    @execute_count(0)
    def test_none_success_case(self):
        """
        Tests that passing None into the batch call clears any batch object
        """
        b = BatchQuery()

        q = TestMultiKeyModel.objects.batch(b)
        self.assertEqual(q._batch, b)

        q = q.batch(None)
        self.assertIsNone(q._batch)

    @execute_count(1)
    def test_batch_execute_timeout(self):
        with mock.patch.object(self.conn.session, 'execute') as mock_execute:
            b = BatchQuery(timeout=1)
            BatchQueryLogModel.batch(b).create(self.conn, k=2, v=2)
            self.conn.execute_query(b)
            self.assertEqual(mock_execute.call_args[-1]['timeout'], 1)

    @execute_count(1)
    def test_batch_execute_no_timeout(self):
        with mock.patch.object(self.conn.session, 'execute') as mock_execute:
            b = BatchQuery()
            BatchQueryLogModel.batch(b).create(self.conn, k=2, v=2)
            self.conn.execute_query(b)
            self.assertEqual(
                mock_execute.call_args[-1]['timeout'],
                TIMEOUT_NOT_SET,
            )
