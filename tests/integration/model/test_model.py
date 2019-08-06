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

from unittest.mock import patch

from cqlmapper import columns, CQLEngineException
from cqlmapper.management import sync_table, drop_table, create_keyspace_simple, drop_keyspace
from cqlmapper import models
from cqlmapper.models import Model, ModelDefinitionException
from uuid import uuid1
from tests.integration import pypy
from tests.integration.base import BaseCassEngTestCase


class TestModel(BaseCassEngTestCase):
    """ Tests the non-io functionality of models """

    def test_instance_equality(self):
        """ tests the model equality functionality """

        class EqualityModel(Model):

            pk = columns.Integer(primary_key=True)

        m0 = EqualityModel(pk=0)
        m1 = EqualityModel(pk=1)

        self.assertEqual(m0, m0)
        self.assertNotEqual(m0, m1)

    def test_model_equality(self):
        """ tests the model equality functionality """

        class EqualityModel0(Model):

            pk = columns.Integer(primary_key=True)

        class EqualityModel1(Model):

            kk = columns.Integer(primary_key=True)

        m0 = EqualityModel0(pk=0)
        m1 = EqualityModel1(kk=1)

        self.assertEqual(m0, m0)
        self.assertNotEqual(m0, m1)

    def test_keywords_as_names(self):
        """
        Test for CQL keywords as names

        test_keywords_as_names tests that CQL keywords are properly and
        automatically quoted in cqlengine. It creates a keyspace, keyspace,
        which should be automatically quoted to "keyspace" in CQL. It then
        creates a table, table, which should also be automatically quoted to
        "table". It then verfies that operations can be done on the
        "keyspace"."table" which has been created. It also verifies that table
        alternations work and operations can be performed on the altered table.

        @since 2.6.0
        @jira_ticket PYTHON-244
        @expected_result Cqlengine should quote CQL keywords properly when
        creating keyspaces and tables.

        @test_category schema:generation
        """

        # If the keyspace exists, it will not be re-created
        create_keyspace_simple(self.conn, "keyspace", 1)
        k_conn = self.connection("keyspace")

        class table(Model):
            select = columns.Integer(primary_key=True)
            table = columns.Text()

        # In case the table already exists in keyspace
        drop_table(k_conn, table)

        # Create should work
        sync_table(k_conn, table)

        created = table.create(k_conn, select=0, table="table")
        selected = table.objects(select=0).first(k_conn)
        self.assertEqual(created.select, selected.select)
        self.assertEqual(created.table, selected.table)

        # Alter should work
        class table(Model):
            select = columns.Integer(primary_key=True)
            table = columns.Text()
            where = columns.Text()

        sync_table(k_conn, table)

        created = table.create(k_conn, select=1, table="table")
        selected = table.objects(select=1).first(k_conn)
        self.assertEqual(created.select, selected.select)
        self.assertEqual(created.table, selected.table)
        self.assertEqual(created.where, selected.where)

        drop_keyspace(self.conn, "keyspace")
        del k_conn

    def test_column_family(self):
        class TestModel(Model):
            k = columns.Integer(primary_key=True)

        self.assertEqual(TestModel.column_family_name(), "test_model")

    def test_column_family_case_sensitive(self):
        """
        Test to ensure case sensitivity is honored when
        __table_name_case_sensitive__ flag is set

        @since 3.1
        @jira_ticket PYTHON-337
        @expected_result table_name case is respected

        @test_category object_mapper
        """

        class TestModel(Model):
            __table_name__ = "TestModel"
            __table_name_case_sensitive__ = True

            k = columns.Integer(primary_key=True)

        self.assertEqual(TestModel.column_family_name(), '"TestModel"')


class BuiltInAttributeConflictTest(unittest.TestCase):
    """
    tests Model definitions that conflict with built-in attributes/methods
    """

    def test_model_with_attribute_name_conflict(self):
        """
        should raise exception when model defines column that conflicts with
        built-in attribute
        """
        with self.assertRaises(ModelDefinitionException):

            class IllegalTimestampColumnModel(Model):

                my_primary_key = columns.Integer(primary_key=True)
                timestamp = columns.BigInt()

    def test_model_with_method_name_conflict(self):
        """
        hould raise exception when model defines column that conflicts
        with built-in method
        """
        with self.assertRaises(ModelDefinitionException):

            class IllegalFilterColumnModel(Model):

                my_primary_key = columns.Integer(primary_key=True)
                filter = columns.Text()


@pypy
class ModelOverWriteTest(BaseCassEngTestCase):
    def test_model_over_write(self):
        """
        Test to ensure overwriting of primary keys in model inheritance is
        allowed

        This is currently only an issue in PyPy. When PYTHON-504 is
        introduced this should be updated error out and warn the user

        @since 3.6.0
        @jira_ticket PYTHON-576
        @expected_result primary keys can be overwritten via inheritance

        @test_category object_mapper
        """

        class TimeModelBase(Model):
            uuid = columns.TimeUUID(primary_key=True)

        class DerivedTimeModel(TimeModelBase):
            __table_name__ = "derived_time"
            uuid = columns.TimeUUID(primary_key=True, partition_key=True)
            value = columns.Text(required=False)

        # In case the table already exists in keyspace
        drop_table(self.conn, DerivedTimeModel)

        sync_table(self.conn, DerivedTimeModel)
        uuid_value = uuid1()
        uuid_value2 = uuid1()
        DerivedTimeModel.create(self.conn, uuid=uuid_value, value="first")
        DerivedTimeModel.create(self.conn, uuid=uuid_value2, value="second")
        DerivedTimeModel.objects.filter(uuid=uuid_value)
