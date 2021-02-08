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
from unittest.mock import patch
from uuid import uuid4

from cqlmapper import columns
from cqlmapper import ValidationError
from cqlmapper.management import drop_table
from cqlmapper.management import sync_table
from cqlmapper.models import Model
from tests.integration.base import BaseCassEngTestCase


class TestUpdateModel(Model):

    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid4)
    count = columns.Integer(required=False)
    text = columns.Text(required=False, index=True)


class ModelUpdateTests(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(ModelUpdateTests, cls).setUpClass()
        sync_table(cls.connection(), TestUpdateModel)

    @classmethod
    def tearDownClass(cls):
        super(ModelUpdateTests, cls).tearDownClass()
        drop_table(cls.connection(), TestUpdateModel)

    def test_update_model(self):
        """ tests calling udpate on models with no values passed in """
        m0 = TestUpdateModel.create(self.conn, count=5, text="monkey")

        # independently save over a new count value, unknown to original
        # instance
        m1 = TestUpdateModel.get(self.conn, partition=m0.partition, cluster=m0.cluster)
        m1.count = 6
        m1.save(self.conn)

        # update the text, and call update
        m0.text = "monkey land"
        m0.update(self.conn)

        # database should reflect both updates
        m2 = TestUpdateModel.get(self.conn, partition=m0.partition, cluster=m0.cluster)
        self.assertEqual(m2.count, m1.count)
        self.assertEqual(m2.text, m0.text)

    def test_update_values(self):
        """ tests calling update on models with values passed in """
        m0 = TestUpdateModel.create(self.conn, count=5, text="monkey")

        # independently save over a new count value, unknown to original
        # instance
        m1 = TestUpdateModel.get(self.conn, partition=m0.partition, cluster=m0.cluster)
        m1.count = 6
        m1.save(self.conn)

        # update the text, and call update
        m0.update(self.conn, text="monkey land")
        self.assertEqual(m0.text, "monkey land")

        # database should reflect both updates
        m2 = TestUpdateModel.get(self.conn, partition=m0.partition, cluster=m0.cluster)
        self.assertEqual(m2.count, m1.count)
        self.assertEqual(m2.text, m0.text)

    def test_noop_model_direct_update(self):
        """
        Tests that calling update on a model with no changes will do nothing.
        """
        m0 = TestUpdateModel.create(self.conn, count=5, text="monkey")

        with patch.object(self.conn.session, "execute") as execute:
            m0.update(self.conn)
        assert execute.call_count == 0

        with patch.object(self.conn.session, "execute") as execute:
            m0.update(self.conn, count=5)
        assert execute.call_count == 0

        with self.assertRaises(ValidationError):
            m0.update(self.conn, partition=m0.partition)

        with self.assertRaises(ValidationError):
            m0.update(self.conn, cluster=m0.cluster)

    def test_noop_model_assignation_update(self):
        """ Tests that assigning the same value on a model will do nothing. """
        # Create object and fetch it back to eliminate any hidden variable
        # cache effect.
        m0 = TestUpdateModel.create(self.conn, count=5, text="monkey")
        m1 = TestUpdateModel.get(self.conn, partition=m0.partition, cluster=m0.cluster)

        with patch.object(self.conn.session, "execute") as execute:
            m1.save(self.conn)
        assert execute.call_count == 0

        with patch.object(self.conn.session, "execute") as execute:
            m1.count = 5
            m1.save(self.conn)
        assert execute.call_count == 0

        with patch.object(self.conn.session, "execute") as execute:
            m1.partition = m0.partition
            m1.save(self.conn)
        assert execute.call_count == 0

        with patch.object(self.conn.session, "execute") as execute:
            m1.cluster = m0.cluster
            m1.save(self.conn)
        assert execute.call_count == 0

    def test_invalid_update_kwarg(self):
        """
        tests that passing in a kwarg to the update method that isn't a
        column will fail
        """
        m0 = TestUpdateModel.create(self.conn, count=5, text="monkey")
        with self.assertRaises(ValidationError):
            m0.update(self.conn, numbers=20)

    def test_primary_key_update_failure(self):
        """
        tests that attempting to update the value of a primary key will fail
        """
        m0 = TestUpdateModel.create(self.conn, count=5, text="monkey")
        with self.assertRaises(ValidationError):
            m0.update(self.conn, partition=uuid4())
