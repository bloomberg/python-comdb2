# Copyright 2017 Bloomberg Finance L.P.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from comdb2 import cdb2
import dateutil.tz
import pytest

import datetime
import time


def test_datetimeus_fromdatetime():
    dt = datetime.datetime(2016, 2, 24, 12, 0)
    dtu = cdb2.DatetimeUs.fromdatetime(dt)

    assert type(dtu) == cdb2.DatetimeUs
    assert dtu == dt


def test_datetimeus_add():
    dt = datetime.datetime(2016, 2, 24, 12, 0)
    td = datetime.timedelta(minutes=32)
    dtu = cdb2.DatetimeUs.fromdatetime(dt)

    dtu1 = dt + td
    dtu2 = dtu + td
    dtu3 = td + dtu

    assert type(dtu2) == cdb2.DatetimeUs
    assert type(dtu3) == cdb2.DatetimeUs

    assert dtu2 == dtu1
    assert dtu3 == dtu1


def test_datetimeus_sub_td():
    dt = datetime.datetime(2016, 2, 24, 12, 0)
    td = datetime.timedelta(minutes=32)
    dtu = cdb2.DatetimeUs.fromdatetime(dt)

    dtu1 = dt - td
    dtu2 = dtu - td

    assert type(dtu2) == cdb2.DatetimeUs

    assert dtu2 == dtu1


def test_datetimeus_sub_dt():
    dt = datetime.datetime(2016, 2, 24, 12, 0)
    dt2 = datetime.datetime(2016, 2, 24, 11, 0)
    dtu = cdb2.DatetimeUs.fromdatetime(dt)

    td1 = dt - dt2
    td2 = dtu - dt2

    td3 = dt2 - dt
    td4 = dt2 - dtu

    assert td1 == td2
    assert td3 == td4


def test_datetimeus_now():
    dtu = cdb2.DatetimeUs.now()

    assert type(dtu) == cdb2.DatetimeUs


def test_datetimeus_fromtimestamp():
    dtu = cdb2.DatetimeUs.fromtimestamp(time.time())

    assert type(dtu) == cdb2.DatetimeUs


def test_datetimeus_astimezone():
    eastern = dateutil.tz.gettz("US/Eastern")
    loc_dt = datetime.datetime(2002, 10, 27, 6, 0, 0, tzinfo=eastern)
    dtu = cdb2.DatetimeUs.fromdatetime(loc_dt)

    assert type(dtu.astimezone(datetime.timezone.utc)) == cdb2.DatetimeUs


def test_datetimeus_as_datetime_naive():
    dtu = cdb2.DatetimeUs.fromtimestamp(time.time())
    dt = dtu.as_datetime()
    assert type(dt) == datetime.datetime
    assert dt == dtu


def test_datetimeus_as_datetime_with_tz():
    dtu = cdb2.DatetimeUs.fromtimestamp(time.time(), tz=datetime.timezone.utc)
    dt = dtu.as_datetime()
    assert type(dt) == datetime.datetime
    assert dt == dtu


def test_datetimeus_type_stickiness():
    def check(obj):
        assert isinstance(obj, cdb2.DatetimeUs)

    new_york = dateutil.tz.gettz("America/New_York")
    utc = datetime.timezone.utc

    check(cdb2.DatetimeUs(2016, 8, 15, 18, 47, 15, 123456, new_york))
    check(cdb2.DatetimeUs(2016, 8, 15, 18, 47, 15, 123456))
    check(cdb2.DatetimeUs.today())
    check(cdb2.DatetimeUs.now())
    check(cdb2.DatetimeUs.now(new_york))
    check(cdb2.DatetimeUs.utcnow())
    check(cdb2.DatetimeUs.fromtimestamp(0))
    check(cdb2.DatetimeUs.fromtimestamp(0, new_york))
    check(cdb2.DatetimeUs.utcfromtimestamp(0))
    check(cdb2.DatetimeUs.fromordinal(1))
    check(cdb2.DatetimeUs.combine(datetime.date.today(), datetime.time()))
    check(cdb2.DatetimeUs.strptime("2015-01-01", "%Y-%m-%d"))
    check(cdb2.DatetimeUs.now() - datetime.timedelta(0))
    check(cdb2.DatetimeUs.now() + datetime.timedelta(0))
    check(datetime.timedelta(0) + cdb2.DatetimeUs.now())
    check(cdb2.DatetimeUs.now().replace(year=2016))
    check(cdb2.DatetimeUs.now().replace(tzinfo=new_york))
    check(cdb2.DatetimeUs.now(utc).astimezone(new_york))


@pytest.mark.skipif(
    not hasattr(datetime.datetime, "fold"), reason="Skipped before PEP 495"
)
def test_datetimeus_fold():
    NYC = dateutil.tz.gettz("America/New_York")
    dt = datetime.datetime(2004, 10, 31, 1, 30, fold=1, tzinfo=NYC)

    dtus = cdb2.DatetimeUs.fromdatetime(dt)

    assert dtus.fold == 1  # Check that it handles fold correctly

    # Make sure that the time is correctly disambiguating
    assert dt.tzname() == "EST"  # Just in case it's not a DatetimeUs problem
    assert dtus.tzname() == "EST"
