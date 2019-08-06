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

import sys
from datetime import datetime, timedelta, date, tzinfo
from decimal import Decimal as D
from uuid import uuid4, uuid1

from cassandra import InvalidRequest
from cqlmapper.columns import (
    TimeUUID,
    Ascii,
    Text,
    Integer,
    BigInt,
    VarInt,
    DateTime,
    Date,
    UUID,
    Boolean,
    Decimal,
    Inet,
)
from cqlmapper.management import sync_table, drop_table
from cqlmapper.models import Model, ValidationError
from cassandra import util

from tests.integration import PROTOCOL_VERSION
from tests.integration.base import BaseCassEngTestCase


class TestDatetime(BaseCassEngTestCase):
    class DatetimeTest(Model):

        test_id = Integer(primary_key=True)
        created_at = DateTime()

    @classmethod
    def setUpClass(cls):
        super(TestDatetime, cls).setUpClass()
        sync_table(cls.connection(), cls.DatetimeTest)

    @classmethod
    def tearDownClass(cls):
        super(TestDatetime, cls).tearDownClass()
        drop_table(cls.connection(), cls.DatetimeTest)

    def test_datetime_io(self):
        now = datetime.now()
        self.DatetimeTest.objects.create(self.conn, test_id=0, created_at=now)
        dt2 = self.DatetimeTest.objects(test_id=0).first(self.conn)
        assert dt2.created_at.timetuple()[:6] == now.timetuple()[:6]

    def test_datetime_tzinfo_io(self):
        class TZ(tzinfo):
            def utcoffset(self, date_time):
                return timedelta(hours=-1)

            def dst(self, date_time):
                return None

        now = datetime(1982, 1, 1, tzinfo=TZ())
        dt = self.DatetimeTest.objects.create(self.conn, test_id=1, created_at=now)
        dt2 = self.DatetimeTest.objects(test_id=1).first(self.conn)
        assert dt2.created_at.timetuple()[:6] == (now + timedelta(hours=1)).timetuple()[:6]

    def test_datetime_date_support(self):
        today = date.today()
        self.DatetimeTest.objects.create(self.conn, test_id=2, created_at=today)
        dt2 = self.DatetimeTest.objects(test_id=2).first(self.conn)
        assert (
            dt2.created_at.isoformat() == datetime(today.year, today.month, today.day).isoformat()
        )

    def test_datetime_none(self):
        dt = self.DatetimeTest.objects.create(self.conn, test_id=3, created_at=None)
        dt2 = self.DatetimeTest.objects(test_id=3).first(self.conn)
        assert dt2.created_at is None

        dts = self.DatetimeTest.objects.filter(test_id=3).values_list("created_at")
        assert dts.find_all(self.conn)[0][0] is None

    def test_datetime_invalid(self):
        dt_value = "INVALID"
        with self.assertRaises(TypeError):
            self.DatetimeTest.objects.create(self.conn, test_id=4, created_at=dt_value)

    def test_datetime_timestamp(self):
        dt_value = 1454520554
        self.DatetimeTest.objects.create(self.conn, test_id=5, created_at=dt_value)
        dt2 = self.DatetimeTest.objects(test_id=5).first(self.conn)
        assert dt2.created_at == datetime.utcfromtimestamp(dt_value)

    def test_datetime_large(self):
        dt_value = datetime(2038, 12, 31, 10, 10, 10, 123000)
        self.DatetimeTest.objects.create(self.conn, test_id=6, created_at=dt_value)
        dt2 = self.DatetimeTest.objects(test_id=6).first(self.conn)
        assert dt2.created_at == dt_value

    def test_datetime_truncate_microseconds(self):
        """
        Test to ensure that truncate microseconds works as expected.
        This will be default behavior in the future and we will need to modify
        the tests to comply with new behavior

        @since 3.2
        @jira_ticket PYTHON-273
        @expected_result microseconds should be to the nearest thousand when
        truncate is set.

        @test_category object_mapper
        """
        DateTime.truncate_microseconds = True
        try:
            dt_value = datetime(2024, 12, 31, 10, 10, 10, 923567)
            dt_truncated = datetime(2024, 12, 31, 10, 10, 10, 923000)
            self.DatetimeTest.objects.create(self.conn, test_id=6, created_at=dt_value)
            dt2 = self.DatetimeTest.objects(test_id=6).first(self.conn)
            self.assertEqual(dt2.created_at, dt_truncated)
        finally:
            # We need to always return behavior to default
            DateTime.truncate_microseconds = False


class TestBoolDefault(BaseCassEngTestCase):
    class BoolDefaultValueTest(Model):

        test_id = Integer(primary_key=True)
        stuff = Boolean(default=True)

    @classmethod
    def setUpClass(cls):
        super(TestBoolDefault, cls).setUpClass()
        sync_table(cls.connection(), cls.BoolDefaultValueTest)

    def test_default_is_set(self):
        tmp = self.BoolDefaultValueTest.create(self.conn, test_id=1)
        self.assertEqual(True, tmp.stuff)
        tmp2 = self.BoolDefaultValueTest.get(self.conn, test_id=1)
        self.assertEqual(True, tmp2.stuff)


class TestBoolValidation(BaseCassEngTestCase):
    class BoolValidationTest(Model):

        test_id = Integer(primary_key=True)
        bool_column = Boolean()

    @classmethod
    def setUpClass(cls):
        super(TestBoolValidation, cls).setUpClass()
        sync_table(cls.connection(), cls.BoolValidationTest)

    def test_validation_preserves_none(self):
        test_obj = self.BoolValidationTest(test_id=1)

        test_obj.validate()
        self.assertIsNone(test_obj.bool_column)


class TestVarInt(BaseCassEngTestCase):
    class VarIntTest(Model):

        test_id = Integer(primary_key=True)
        bignum = VarInt(primary_key=True)

    @classmethod
    def setUpClass(cls):
        super(TestVarInt, cls).setUpClass()
        sync_table(cls.connection(), cls.VarIntTest)

    @classmethod
    def tearDownClass(cls):
        super(TestVarInt, cls).tearDownClass()
        sync_table(cls.connection(), cls.VarIntTest)

    def test_varint_io(self):
        # TODO: this is a weird test.  i changed the number from sys.maxint (which doesn't exist in python 3)
        # to the giant number below and it broken between runs.
        long_int = 92834902384092834092384028340283048239048203480234823048230482304820348239
        int1 = self.VarIntTest.objects.create(self.conn, test_id=0, bignum=long_int)
        int2 = self.VarIntTest.objects(test_id=0).first(self.conn)
        self.assertEqual(int1.bignum, int2.bignum)


class TestDate(BaseCassEngTestCase):
    class DateTest(Model):

        test_id = Integer(primary_key=True)
        created_at = Date()

    @classmethod
    def setUpClass(cls):
        super(TestDate, cls).setUpClass()
        if PROTOCOL_VERSION < 4:
            return
        sync_table(cls.connection(), cls.DateTest)

    @classmethod
    def tearDownClass(cls):
        super(TestDate, cls).tearDownClass()
        if PROTOCOL_VERSION < 4:
            return
        drop_table(cls.connection(), cls.DateTest)

    def setUp(self):
        super(TestDate, self).setUp()
        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest(
                "Protocol v4 datatypes require native protocol 4+, "
                "currently using: {0}".format(PROTOCOL_VERSION)
            )

    def test_date_io(self):
        today = date.today()
        self.DateTest.objects.create(self.conn, test_id=0, created_at=today)
        result = self.DateTest.objects(test_id=0).first(self.conn)
        self.assertEqual(result.created_at, util.Date(today))

    def test_date_io_using_datetime(self):
        now = datetime.utcnow()
        self.DateTest.objects.create(self.conn, test_id=0, created_at=now)
        result = self.DateTest.objects(test_id=0).first(self.conn)
        self.assertIsInstance(result.created_at, util.Date)
        self.assertEqual(result.created_at, util.Date(now))

    def test_date_none(self):
        self.DateTest.objects.create(self.conn, test_id=1, created_at=None)
        dt2 = self.DateTest.objects(test_id=1).first(self.conn)
        assert dt2.created_at is None

        dts = self.DateTest.objects(test_id=1).values_list("created_at")
        assert dts.find_all(self.conn)[0][0] is None


class TestDecimal(BaseCassEngTestCase):
    class DecimalTest(Model):

        test_id = Integer(primary_key=True)
        dec_val = Decimal()

    @classmethod
    def setUpClass(cls):
        super(TestDecimal, cls).setUpClass()
        sync_table(cls.connection(), cls.DecimalTest)

    @classmethod
    def tearDownClass(cls):
        super(TestDecimal, cls).tearDownClass()
        drop_table(cls.connection(), cls.DecimalTest)

    def test_decimal_io(self):
        dt = self.DecimalTest.objects.create(self.conn, test_id=0, dec_val=D("0.00"))
        dt2 = self.DecimalTest.objects(test_id=0).first(self.conn)
        assert dt2.dec_val == dt.dec_val

        dt = self.DecimalTest.objects.create(self.conn, test_id=0, dec_val=5)
        dt2 = self.DecimalTest.objects(test_id=0).first(self.conn)
        assert dt2.dec_val == D("5")


class TestUUID(BaseCassEngTestCase):
    class UUIDTest(Model):

        test_id = Integer(primary_key=True)
        a_uuid = UUID(default=uuid4())

    @classmethod
    def setUpClass(cls):
        super(TestUUID, cls).setUpClass()
        sync_table(cls.connection(), cls.UUIDTest)

    @classmethod
    def tearDownClass(cls):
        super(TestUUID, cls).tearDownClass()
        drop_table(cls.connection(), cls.UUIDTest)

    def test_uuid_str_with_dashes(self):
        a_uuid = uuid4()
        t0 = self.UUIDTest.create(self.conn, test_id=0, a_uuid=str(a_uuid))
        t1 = self.UUIDTest.get(self.conn, test_id=0)
        assert a_uuid == t1.a_uuid

    def test_uuid_str_no_dashes(self):
        a_uuid = uuid4()
        t0 = self.UUIDTest.create(self.conn, test_id=1, a_uuid=a_uuid.hex)
        t1 = self.UUIDTest.get(self.conn, test_id=1)
        assert a_uuid == t1.a_uuid

    def test_uuid_with_upcase(self):
        a_uuid = uuid4()
        val = str(a_uuid).upper()
        t0 = self.UUIDTest.create(self.conn, test_id=0, a_uuid=val)
        t1 = self.UUIDTest.get(self.conn, test_id=0)
        assert a_uuid == t1.a_uuid


class TestTimeUUID(BaseCassEngTestCase):
    class TimeUUIDTest(Model):

        test_id = Integer(primary_key=True)
        timeuuid = TimeUUID(default=uuid1())

    @classmethod
    def setUpClass(cls):
        super(TestTimeUUID, cls).setUpClass()
        sync_table(cls.connection(), cls.TimeUUIDTest)

    @classmethod
    def tearDownClass(cls):
        super(TestTimeUUID, cls).tearDownClass()
        drop_table(cls.connection(), cls.TimeUUIDTest)

    def test_timeuuid_io(self):
        """
        ensures that
        :return:
        """
        t0 = self.TimeUUIDTest.create(self.conn, test_id=0)
        t1 = self.TimeUUIDTest.get(self.conn, test_id=0)

        assert t1.timeuuid.time == t1.timeuuid.time


class TestInteger(BaseCassEngTestCase):
    class IntegerTest(Model):

        test_id = UUID(primary_key=True, default=lambda: uuid4())
        value = Integer(default=0, required=True)

    def test_default_zero_fields_validate(self):
        """ Tests that integer columns with a default value of 0 validate """
        it = self.IntegerTest()
        it.validate()


class TestBigInt(BaseCassEngTestCase):
    class BigIntTest(Model):

        test_id = UUID(primary_key=True, default=lambda: uuid4())
        value = BigInt(default=0, required=True)

    def test_default_zero_fields_validate(self):
        """ Tests that bigint columns with a default value of 0 validate """
        it = self.BigIntTest()
        it.validate()


class TestAscii(BaseCassEngTestCase):
    def test_min_length(self):
        """ Test arbitrary minimal lengths requirements. """
        Ascii(min_length=0).validate("")
        Ascii(min_length=0).validate(None)
        Ascii(min_length=0).validate("kevin")

        Ascii(min_length=1).validate("k")

        Ascii(min_length=5).validate("kevin")
        Ascii(min_length=5).validate("kevintastic")

        with self.assertRaises(ValidationError):
            Ascii(min_length=1).validate("")

        with self.assertRaises(ValidationError):
            Ascii(min_length=1).validate(None)

        with self.assertRaises(ValidationError):
            Ascii(min_length=6).validate("")

        with self.assertRaises(ValidationError):
            Ascii(min_length=6).validate(None)

        with self.assertRaises(ValidationError):
            Ascii(min_length=6).validate("kevin")

        with self.assertRaises(ValueError):
            Ascii(min_length=-1)

    def test_max_length(self):
        """ Test arbitrary maximal lengths requirements. """
        Ascii(max_length=0).validate("")
        Ascii(max_length=0).validate(None)

        Ascii(max_length=1).validate("")
        Ascii(max_length=1).validate(None)
        Ascii(max_length=1).validate("b")

        Ascii(max_length=5).validate("")
        Ascii(max_length=5).validate(None)
        Ascii(max_length=5).validate("b")
        Ascii(max_length=5).validate("blake")

        with self.assertRaises(ValidationError):
            Ascii(max_length=0).validate("b")

        with self.assertRaises(ValidationError):
            Ascii(max_length=5).validate("blaketastic")

        with self.assertRaises(ValueError):
            Ascii(max_length=-1)

    def test_length_range(self):
        Ascii(min_length=0, max_length=0)
        Ascii(min_length=0, max_length=1)
        Ascii(min_length=10, max_length=10)
        Ascii(min_length=10, max_length=11)

        with self.assertRaises(ValueError):
            Ascii(min_length=10, max_length=9)

        with self.assertRaises(ValueError):
            Ascii(min_length=1, max_length=0)

    def test_type_checking(self):
        Ascii().validate("string")
        Ascii().validate("unicode")
        Ascii().validate(bytearray("bytearray", encoding="ascii"))

        with self.assertRaises(ValidationError):
            Ascii().validate(5)

        with self.assertRaises(ValidationError):
            Ascii().validate(True)

        Ascii().validate("!#$%&'()*+,-./")

        with self.assertRaises(ValidationError):
            Ascii().validate("Beyonc" + chr(233))

        if sys.version_info < (3, 1):
            with self.assertRaises(ValidationError):
                Ascii().validate("Beyonc" + unichr(233))

    def test_unaltering_validation(self):
        """ Test the validation step doesn't re-interpret values. """
        self.assertEqual(Ascii().validate(""), "")
        self.assertEqual(Ascii().validate(None), None)
        self.assertEqual(Ascii().validate("yo"), "yo")

    def test_non_required_validation(self):
        """ Tests that validation is ok on none and blank values if required is False. """
        Ascii().validate("")
        Ascii().validate(None)

    def test_required_validation(self):
        """ Tests that validation raise on none and blank values if value required. """
        Ascii(required=True).validate("k")

        with self.assertRaises(ValidationError):
            Ascii(required=True).validate("")

        with self.assertRaises(ValidationError):
            Ascii(required=True).validate(None)

        # With min_length set.
        Ascii(required=True, min_length=0).validate("k")
        Ascii(required=True, min_length=1).validate("k")

        with self.assertRaises(ValidationError):
            Ascii(required=True, min_length=2).validate("k")

        # With max_length set.
        Ascii(required=True, max_length=1).validate("k")

        with self.assertRaises(ValidationError):
            Ascii(required=True, max_length=2).validate("kevin")

        with self.assertRaises(ValueError):
            Ascii(required=True, max_length=0)


class TestText(BaseCassEngTestCase):
    def test_min_length(self):
        """ Test arbitrary minimal lengths requirements. """
        Text(min_length=0).validate("")
        Text(min_length=0).validate(None)
        Text(min_length=0).validate("blake")

        Text(min_length=1).validate("b")

        Text(min_length=5).validate("blake")
        Text(min_length=5).validate("blaketastic")

        with self.assertRaises(ValidationError):
            Text(min_length=1).validate("")

        with self.assertRaises(ValidationError):
            Text(min_length=1).validate(None)

        with self.assertRaises(ValidationError):
            Text(min_length=6).validate("")

        with self.assertRaises(ValidationError):
            Text(min_length=6).validate(None)

        with self.assertRaises(ValidationError):
            Text(min_length=6).validate("blake")

        with self.assertRaises(ValueError):
            Text(min_length=-1)

    def test_max_length(self):
        """ Test arbitrary maximal lengths requirements. """
        Text(max_length=0).validate("")
        Text(max_length=0).validate(None)

        Text(max_length=1).validate("")
        Text(max_length=1).validate(None)
        Text(max_length=1).validate("b")

        Text(max_length=5).validate("")
        Text(max_length=5).validate(None)
        Text(max_length=5).validate("b")
        Text(max_length=5).validate("blake")

        with self.assertRaises(ValidationError):
            Text(max_length=0).validate("b")

        with self.assertRaises(ValidationError):
            Text(max_length=5).validate("blaketastic")

        with self.assertRaises(ValueError):
            Text(max_length=-1)

    def test_length_range(self):
        Text(min_length=0, max_length=0)
        Text(min_length=0, max_length=1)
        Text(min_length=10, max_length=10)
        Text(min_length=10, max_length=11)

        with self.assertRaises(ValueError):
            Text(min_length=10, max_length=9)

        with self.assertRaises(ValueError):
            Text(min_length=1, max_length=0)

    def test_type_checking(self):
        Text().validate("string")
        Text().validate("unicode")
        Text().validate(bytearray("bytearray", encoding="ascii"))

        with self.assertRaises(ValidationError):
            Text().validate(5)

        with self.assertRaises(ValidationError):
            Text().validate(True)

        Text().validate("!#$%&'()*+,-./")
        Text().validate("Beyonc" + chr(233))
        if sys.version_info < (3, 1):
            Text().validate("Beyonc" + unichr(233))

    def test_unaltering_validation(self):
        """ Test the validation step doesn't re-interpret values. """
        self.assertEqual(Text().validate(""), "")
        self.assertEqual(Text().validate(None), None)
        self.assertEqual(Text().validate("yo"), "yo")

    def test_non_required_validation(self):
        """ Tests that validation is ok on none and blank values if required is False """
        Text().validate("")
        Text().validate(None)

    def test_required_validation(self):
        """ Tests that validation raise on none and blank values if value required. """
        Text(required=True).validate("b")

        with self.assertRaises(ValidationError):
            Text(required=True).validate("")

        with self.assertRaises(ValidationError):
            Text(required=True).validate(None)

        # With min_length set.
        Text(required=True, min_length=0).validate("b")
        Text(required=True, min_length=1).validate("b")

        with self.assertRaises(ValidationError):
            Text(required=True, min_length=2).validate("b")

        # With max_length set.
        Text(required=True, max_length=1).validate("b")

        with self.assertRaises(ValidationError):
            Text(required=True, max_length=2).validate("blake")

        with self.assertRaises(ValueError):
            Text(required=True, max_length=0)


class TestExtraFieldsRaiseException(BaseCassEngTestCase):
    class TestModel(Model):

        id = UUID(primary_key=True, default=uuid4)

    def test_extra_field(self):
        with self.assertRaises(ValidationError):
            self.TestModel.create(self.conn, bacon=5000)


class TestPythonDoesntDieWhenExtraFieldIsInCassandra(BaseCassEngTestCase):
    class TestModel(Model):

        __table_name__ = "alter_doesnt_break_running_app"
        id = UUID(primary_key=True, default=uuid4)

    def test_extra_field(self):
        drop_table(self.conn, self.TestModel)
        sync_table(self.conn, self.TestModel)
        self.TestModel.create(self.conn)
        self.conn.execute(
            "ALTER TABLE {0} add blah int".format(self.TestModel.column_family_name())
        )
        self.TestModel.objects().find_all(self.conn)


class TestTimeUUIDFromDatetime(BaseCassEngTestCase):
    def test_conversion_specific_date(self):
        dt = datetime(1981, 7, 11, microsecond=555000)

        uuid = util.uuid_from_time(dt)

        from uuid import UUID

        assert isinstance(uuid, UUID)

        ts = (uuid.time - 0x01B21DD213814000) / 1e7  # back to a timestamp
        new_dt = datetime.utcfromtimestamp(ts)

        # checks that we created a UUID1 with the proper timestamp
        assert new_dt == dt


class TestInet(BaseCassEngTestCase):
    class InetTestModel(Model):
        id = UUID(primary_key=True, default=uuid4)
        address = Inet()

    def setUp(self):
        super(TestInet, self).setUp()
        drop_table(self.conn, self.InetTestModel)
        sync_table(self.conn, self.InetTestModel)

    def test_inet_saves(self):
        tmp = self.InetTestModel.create(self.conn, address="192.168.1.1")

        m = self.InetTestModel.get(self.conn, id=tmp.id)

        assert m.address == "192.168.1.1"

    def test_non_address_fails(self):
        # TODO: presently this only tests that the server blows it up. Is there supposed to be local validation?
        with self.assertRaises(InvalidRequest):
            self.InetTestModel.create(self.conn, address="what is going on here?")
