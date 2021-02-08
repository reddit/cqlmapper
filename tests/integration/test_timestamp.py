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
import re

from datetime import datetime
from datetime import timedelta
from unittest import mock
from uuid import uuid4

from cqlmapper import columns
from cqlmapper.batch import Batch
from cqlmapper.management import sync_table
from cqlmapper.models import Model
from tests.integration.base import BaseCassEngTestCase
from tests.integration.base import main


class TestTimestampModel(Model):
    id = columns.UUID(primary_key=True, default=uuid4)
    count = columns.Integer()


class BaseTimestampTest(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(BaseTimestampTest, cls).setUpClass()
        sync_table(cls.connection(), TestTimestampModel)


class BatchTest(BaseTimestampTest):
    def test_batch_is_included(self):
        with mock.patch.object(self.conn.session, "execute") as m:
            with Batch(self.conn, timestamp=timedelta(seconds=30)) as b_conn:
                TestTimestampModel.create(b_conn, count=1)

        self.assertIn("USING TIMESTAMP", m.call_args[0][0].query_string)


class CreateWithTimestampTest(BaseTimestampTest):
    def test_batch(self):
        with mock.patch.object(self.conn.session, "execute") as m:
            with Batch(self.conn) as b_conn:
                TestTimestampModel.timestamp(timedelta(seconds=10)).create(b_conn, count=1)

        query = m.call_args[0][0].query_string
        self.assertIsNot(re.search(r"INSERT.*USING TIMESTAMP", query), None)
        self.assertIs(re.search(r"TIMESTAMP.*INSERT", query), None)

    def test_timestamp_not_included_on_normal_create(self):
        with mock.patch.object(self.conn.session, "execute") as m:
            TestTimestampModel.create(self.conn, count=2)

        self.assertNotIn("USING TIMESTAMP", m.call_args[0][0].query_string)

    def test_timestamp_is_set_on_model_queryset(self):
        delta = timedelta(seconds=30)
        tmp = TestTimestampModel.timestamp(delta)
        self.assertEqual(tmp._timestamp, delta)

    def test_non_batch_syntax_integration(self):
        tmp = TestTimestampModel.timestamp(timedelta(seconds=30)).create(self.conn, count=1)
        self.assertIsNot(tmp, None)

    def test_non_batch_syntax_unit(self):

        with mock.patch.object(self.conn.session, "execute") as m:
            TestTimestampModel.timestamp(timedelta(seconds=30)).create(self.conn, count=1)

        query = m.call_args[0][0].query_string

        self.assertIn("USING TIMESTAMP", query)


class UpdateWithTimestampTest(BaseTimestampTest):
    def setUp(self):
        super(UpdateWithTimestampTest, self).setUp()
        self.instance = TestTimestampModel.create(self.conn, count=1)

    def test_instance_update_includes_timestamp_in_query(self):
        # not a batch

        with mock.patch.object(self.conn.session, "execute") as m:
            self.instance.timestamp(timedelta(seconds=30)).update(self.conn, count=2)

        self.assertIn("USING TIMESTAMP", m.call_args[0][0].query_string)

    def test_instance_update_in_batch(self):
        with mock.patch.object(self.conn.session, "execute") as m:
            with Batch(self.conn) as b_conn:
                self.instance.timestamp(timedelta(seconds=30)).update(b_conn, count=2)

        query = m.call_args[0][0].query_string
        self.assertIn("USING TIMESTAMP", query)


class DeleteWithTimestampTest(BaseTimestampTest):
    def test_non_batch(self):
        """
        we don't expect the model to come back at the end because the
        deletion timestamp should be in the future
        """
        uid = uuid4()
        tmp = TestTimestampModel.create(self.conn, id=uid, count=1)

        self.assertIsNot(TestTimestampModel.get(self.conn, id=uid), None)

        tmp.timestamp(timedelta(seconds=5)).delete(self.conn)

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(self.conn, id=uid)

        tmp = TestTimestampModel.create(self.conn, id=uid, count=1)

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(self.conn, id=uid)

        # calling .timestamp sets the TS on the model
        tmp.timestamp(timedelta(seconds=5))
        self.assertIsNot(tmp._timestamp, None)

        # calling save clears the set timestamp
        tmp.save(self.conn)
        self.assertIs(tmp._timestamp, None)

        tmp.timestamp(timedelta(seconds=5))
        tmp.update(self.conn)
        self.assertIs(tmp._timestamp, None)

    def test_blind_delete(self):
        """
        we don't expect the model to come back at the end because the
        deletion timestamp should be in the future
        """
        uid = uuid4()
        tmp = TestTimestampModel.create(self.conn, id=uid, count=1)

        self.assertIsNot(TestTimestampModel.get(self.conn, id=uid), None)

        TestTimestampModel.objects(id=uid).timestamp(timedelta(seconds=5)).delete(self.conn)

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(self.conn, id=uid)

        tmp = TestTimestampModel.create(self.conn, id=uid, count=1)

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(self.conn, id=uid)

    def test_blind_delete_with_datetime(self):
        """
        we don't expect the model to come back at the end because the
        deletion timestamp should be in the future
        """
        uid = uuid4()
        tmp = TestTimestampModel.create(self.conn, id=uid, count=1)

        self.assertIsNot(TestTimestampModel.get(self.conn, id=uid), None)

        plus_five_seconds = datetime.now() + timedelta(seconds=5)

        TestTimestampModel.objects(id=uid).timestamp(plus_five_seconds).delete(self.conn)

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(self.conn, id=uid)

        tmp = TestTimestampModel.create(self.conn, id=uid, count=1)

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(self.conn, id=uid)

    def test_delete_in_the_past(self):
        uid = uuid4()
        tmp = TestTimestampModel.create(self.conn, id=uid, count=1)

        self.assertIsNot(TestTimestampModel.get(self.conn, id=uid), None)

        # delete the in past, should not affect the object created above
        TestTimestampModel.objects(id=uid).timestamp(timedelta(seconds=-60)).delete(self.conn)

        TestTimestampModel.get(self.conn, id=uid)


if __name__ == "__main__":
    main()
