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
import copy

from unittest.mock import patch

import six

from cqlmapper import columns
from cqlmapper.management import _get_table_metadata
from cqlmapper.management import _update_options
from cqlmapper.management import drop_table
from cqlmapper.management import sync_table
from cqlmapper.models import Model
from tests.integration.base import BaseCassEngTestCase


class LeveledCompactionTestTable(Model):

    __options__ = {
        "compaction": {
            "class": ("org.apache.cassandra.db.compaction." "LeveledCompactionStrategy"),
            "sstable_size_in_mb": "64",
        }
    }

    user_id = columns.UUID(primary_key=True)
    name = columns.Text()


class AlterTableTest(BaseCassEngTestCase):
    def test_alter_is_called_table(self):
        drop_table(self.conn, LeveledCompactionTestTable)
        sync_table(self.conn, LeveledCompactionTestTable)
        with patch("cqlmapper.management._update_options") as mock:
            sync_table(self.conn, LeveledCompactionTestTable)
        assert mock.called == 1

    def test_compaction_not_altered_without_changes_leveled(self):
        class LeveledCompactionChangesDetectionTest(Model):

            __options__ = {
                "compaction": {
                    "class": ("org.apache.cassandra.db.compaction." "LeveledCompactionStrategy"),
                    "sstable_size_in_mb": "160",
                    "tombstone_threshold": "0.125",
                    "tombstone_compaction_interval": "3600",
                }
            }
            pk = columns.Integer(primary_key=True)

        drop_table(self.conn, LeveledCompactionChangesDetectionTest)
        sync_table(self.conn, LeveledCompactionChangesDetectionTest)

        self.assertFalse(_update_options(self.conn, LeveledCompactionChangesDetectionTest))

    def test_compaction_not_altered_without_changes_sizetiered(self):
        class SizeTieredCompactionChangesDetectionTest(Model):

            __options__ = {
                "compaction": {
                    "class": ("org.apache.cassandra.db.compaction." "SizeTieredCompactionStrategy"),
                    "bucket_high": "20",
                    "bucket_low": "10",
                    "max_threshold": "200",
                    "min_threshold": "100",
                    "min_sstable_size": "1000",
                    "tombstone_threshold": "0.125",
                    "tombstone_compaction_interval": "3600",
                }
            }
            pk = columns.Integer(primary_key=True)

        drop_table(self.conn, SizeTieredCompactionChangesDetectionTest)
        sync_table(self.conn, SizeTieredCompactionChangesDetectionTest)

        self.assertFalse(_update_options(self.conn, SizeTieredCompactionChangesDetectionTest))

    def test_alter_actually_alters(self):
        tmp = copy.deepcopy(LeveledCompactionTestTable)
        drop_table(self.conn, tmp)
        sync_table(self.conn, tmp)
        tmp.__options__ = {
            "compaction": {
                "class": ("org.apache.cassandra.db.compaction." "SizeTieredCompactionStrategy")
            }
        }
        sync_table(self.conn, tmp)

        table_meta = _get_table_metadata(self.conn, tmp)

        self.assertRegexpMatches(table_meta.export_as_string(), ".*SizeTieredCompactionStrategy.*")

    def test_alter_options(self):
        class AlterTable(Model):

            __options__ = {
                "compaction": {
                    "class": ("org.apache.cassandra.db.compaction." "LeveledCompactionStrategy"),
                    "sstable_size_in_mb": "64",
                }
            }
            user_id = columns.UUID(primary_key=True)
            name = columns.Text()

        drop_table(self.conn, AlterTable)
        sync_table(self.conn, AlterTable)
        table_meta = _get_table_metadata(self.conn, AlterTable)
        self.assertRegexpMatches(table_meta.export_as_string(), ".*'sstable_size_in_mb': '64'.*")
        AlterTable.__options__["compaction"]["sstable_size_in_mb"] = "128"
        sync_table(self.conn, AlterTable)
        table_meta = _get_table_metadata(self.conn, AlterTable)
        self.assertRegexpMatches(table_meta.export_as_string(), ".*'sstable_size_in_mb': '128'.*")


class OptionsTest(BaseCassEngTestCase):
    def _verify_options(self, table_meta, expected_options):
        cql = table_meta.export_as_string()

        for name, value in expected_options.items():
            if isinstance(value, six.string_types):
                self.assertIn("%s = '%s'" % (name, value), cql)
            else:
                start = cql.find("%s = {" % (name,))
                end = cql.find("}", start)
                for subname, subvalue in value.items():
                    attr = "'%s': '%s'" % (subname, subvalue)
                    found_at = cql.find(attr, start)
                    self.assertTrue(found_at > start)
                    self.assertTrue(found_at < end)

    def test_all_size_tiered_options(self):
        class AllSizeTieredOptionsModel(Model):
            __options__ = {
                "compaction": {
                    "class": ("org.apache.cassandra.db.compaction." "SizeTieredCompactionStrategy"),
                    "bucket_low": ".3",
                    "bucket_high": "2",
                    "min_threshold": "2",
                    "max_threshold": "64",
                    "tombstone_compaction_interval": "86400",
                }
            }

            cid = columns.UUID(primary_key=True)
            name = columns.Text()

        drop_table(self.conn, AllSizeTieredOptionsModel)
        sync_table(self.conn, AllSizeTieredOptionsModel)

        table_meta = _get_table_metadata(self.conn, AllSizeTieredOptionsModel)
        self._verify_options(table_meta, AllSizeTieredOptionsModel.__options__)

    def test_all_leveled_options(self):
        class AllLeveledOptionsModel(Model):
            __options__ = {
                "compaction": {
                    "class": ("org.apache.cassandra.db.compaction." "LeveledCompactionStrategy"),
                    "sstable_size_in_mb": "64",
                }
            }

            cid = columns.UUID(primary_key=True)
            name = columns.Text()

        drop_table(self.conn, AllLeveledOptionsModel)
        sync_table(self.conn, AllLeveledOptionsModel)

        table_meta = _get_table_metadata(self.conn, AllLeveledOptionsModel)
        self._verify_options(table_meta, AllLeveledOptionsModel.__options__)
