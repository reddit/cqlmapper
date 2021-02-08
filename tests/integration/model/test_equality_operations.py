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
from tests.integration.base import BaseCassEngTestCase


class TestModel(Model):

    id = columns.UUID(primary_key=True, default=uuid4)
    count = columns.Integer()
    text = columns.Text(required=False)


class TestEqualityOperators(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(TestEqualityOperators, cls).setUpClass()
        sync_table(cls.connection(), TestModel)

    def setUp(self):
        super(TestEqualityOperators, self).setUp()
        self.t0 = TestModel.create(self.conn, count=5, text="words")
        self.t1 = TestModel.create(self.conn, count=5, text="words")

    @classmethod
    def tearDownClass(cls):
        super(TestEqualityOperators, cls).tearDownClass()
        drop_table(cls.connection(), TestModel)

    def test_an_instance_evaluates_as_equal_to_itself(self):
        """
        """
        assert self.t0 == self.t0

    def test_two_instances_referencing_the_same_rows_and_different_values_evaluate_not_equal(self):
        """
        """
        t0 = TestModel.get(self.conn, id=self.t0.id)
        t0.text = "bleh"
        assert t0 != self.t0

    def test_two_instances_referencing_the_same_rows_and_values_evaluate_equal(self):
        """
        """
        t0 = TestModel.get(self.conn, id=self.t0.id)
        assert t0 == self.t0

    def test_two_instances_referencing_different_rows_evaluate_to_not_equal(self):
        """
        """
        assert self.t0 != self.t1
