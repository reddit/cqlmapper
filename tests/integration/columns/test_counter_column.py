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
from uuid import uuid4

from cqlmapper import columns
from cqlmapper.management import drop_table
from cqlmapper.management import sync_table
from cqlmapper.models import Model
from cqlmapper.models import ModelDefinitionException
from tests.integration.base import BaseCassEngTestCase


class TestCounterModel(Model):

    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid4)
    counter = columns.Counter()


class TestClassConstruction(BaseCassEngTestCase):
    def test_defining_a_non_counter_column_fails(self):
        """
        Tests that defining a non counter column field in a model with a
        counter column fails
        """
        try:

            class model(Model):
                partition = columns.UUID(primary_key=True, default=uuid4)
                counter = columns.Counter()
                text = columns.Text()

            self.fail("did not raise expected ModelDefinitionException")
        except ModelDefinitionException:
            pass

    def test_defining_a_primary_key_counter_column_fails(self):
        """ Tests that defining primary keys on counter columns fails """
        try:

            class model(Model):
                partition = columns.UUID(primary_key=True, default=uuid4)
                cluster = columns.Counter(primary_ley=True)
                counter = columns.Counter()

            self.fail("did not raise expected TypeError")
        except TypeError:
            pass

        # force it
        try:

            class model(Model):
                partition = columns.UUID(primary_key=True, default=uuid4)
                cluster = columns.Counter()
                cluster.primary_key = True
                counter = columns.Counter()

            self.fail("did not raise expected ModelDefinitionException")
        except ModelDefinitionException:
            pass


class TestCounterColumn(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(TestCounterColumn, cls).setUpClass()
        conn = cls.connection()
        drop_table(conn, TestCounterModel)
        sync_table(conn, TestCounterModel)

    @classmethod
    def tearDownClass(cls):
        super(TestCounterColumn, cls).tearDownClass()
        drop_table(cls.connection(), TestCounterModel)

    def test_updates(self):
        """ Tests that counter updates work as intended """
        instance = TestCounterModel.create(self.conn)
        instance.counter += 5
        instance.save(self.conn)

        actual = TestCounterModel.get(self.conn, partition=instance.partition)
        assert actual.counter == 5

    def test_concurrent_updates(self):
        """ Tests updates from multiple queries reaches the correct value """
        instance = TestCounterModel.create(self.conn)
        new1 = TestCounterModel.get(self.conn, partition=instance.partition)
        new2 = TestCounterModel.get(self.conn, partition=instance.partition)

        new1.counter += 5
        new1.save(self.conn)
        new2.counter += 5
        new2.save(self.conn)

        actual = TestCounterModel.get(self.conn, partition=instance.partition)
        assert actual.counter == 10

    def test_update_from_none(self):
        """ Tests that updating from None uses a create statement """
        instance = TestCounterModel()
        instance.counter += 1
        instance.save(self.conn)

        new = TestCounterModel.get(self.conn, partition=instance.partition)
        assert new.counter == 1

    def test_new_instance_defaults_to_zero(self):
        """ Tests that instantiating a new model instance will set the counter
        column to zero """
        instance = TestCounterModel()
        assert instance.counter == 0

    def test_save_after_no_update(self):
        expected_value = 15
        instance = TestCounterModel.create(self.conn)
        instance.update(self.conn, counter=expected_value)

        # read back
        instance = TestCounterModel.get(self.conn, partition=instance.partition)
        self.assertEqual(instance.counter, expected_value)

        # save after doing nothing
        instance.save(self.conn)
        self.assertEqual(instance.counter, expected_value)

        # make sure there was no increment
        instance = TestCounterModel.get(self.conn, partition=instance.partition)
        self.assertEqual(instance.counter, expected_value)
