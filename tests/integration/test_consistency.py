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
from unittest import mock
from uuid import uuid4

from cassandra import ConsistencyLevel
from cassandra import ConsistencyLevel as CL
from cassandra.cluster import Session

from cqlmapper import columns
from cqlmapper import connection
from cqlmapper.batch import Batch
from cqlmapper.management import drop_table
from cqlmapper.management import sync_table
from cqlmapper.models import Model
from cqlmapper.query_set import ModelQuerySet
from tests.integration.base import BaseCassEngTestCase


class TestConsistencyModel(Model):

    id = columns.UUID(primary_key=True, default=uuid4)
    count = columns.Integer()
    text = columns.Text(required=False)


class BaseConsistencyTest(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(BaseConsistencyTest, cls).setUpClass()
        sync_table(cls.connection(), TestConsistencyModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseConsistencyTest, cls).tearDownClass()
        drop_table(cls.connection(), TestConsistencyModel)


class TestConsistency(BaseConsistencyTest):
    def test_create_uses_consistency(self):

        qs = TestConsistencyModel.consistency(CL.ALL)
        with mock.patch.object(self.conn.session, "execute") as m:
            qs.create(self.conn, text="i am not fault tolerant this way")

        args = m.call_args
        self.assertEqual(CL.ALL, args[0][0].consistency_level)

    def test_queryset_is_returned_on_create(self):
        qs = TestConsistencyModel.consistency(CL.ALL)
        self.assertTrue(isinstance(qs, ModelQuerySet), type(qs))

    def test_update_uses_consistency(self):
        t = TestConsistencyModel.create(self.conn, text="bacon and eggs")
        t.text = "ham sandwich"

        with mock.patch.object(self.conn.session, "execute") as m:
            t.consistency(CL.ALL).save(self.conn)

        args = m.call_args
        self.assertEqual(CL.ALL, args[0][0].consistency_level)

    def test_batch_consistency(self):

        with mock.patch.object(self.conn.session, "execute") as m:
            with Batch(self.conn, consistency=CL.ALL) as b_conn:
                TestConsistencyModel.create(b_conn, text="monkey")

        args = m.call_args

        self.assertEqual(CL.ALL, args[0][0].consistency_level)

        with mock.patch.object(self.conn.session, "execute") as m:
            with Batch(self.conn) as b_conn:
                TestConsistencyModel.create(b_conn, text="monkey")

        args = m.call_args
        self.assertNotEqual(CL.ALL, args[0][0].consistency_level)

    def test_blind_update(self):
        t = TestConsistencyModel.create(self.conn, text="bacon and eggs")
        t.text = "ham sandwich"
        uid = t.id

        with mock.patch.object(self.conn.session, "execute") as m:
            TestConsistencyModel.objects(id=uid).consistency(CL.ALL).update(
                self.conn, text="grilled cheese"
            )

        args = m.call_args
        self.assertEqual(CL.ALL, args[0][0].consistency_level)

    def test_delete(self):
        # ensures we always carry consistency through on delete statements
        t = TestConsistencyModel.create(self.conn, text="bacon and eggs")
        t.text = "ham and cheese sandwich"
        uid = t.id

        with mock.patch.object(self.conn.session, "execute") as m:
            t.consistency(CL.ALL).delete(self.conn)

        with mock.patch.object(self.conn.session, "execute") as m:
            TestConsistencyModel.objects(id=uid).consistency(CL.ALL).delete(self.conn)

        args = m.call_args
        self.assertEqual(CL.ALL, args[0][0].consistency_level)
