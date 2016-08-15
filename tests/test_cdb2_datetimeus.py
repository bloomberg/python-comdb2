from __future__ import unicode_literals, absolute_import

from comdb2 import cdb2
import pytest
import pytz
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
    eastern = pytz.timezone('US/Eastern')
    loc_dt = eastern.localize(datetime.datetime(2002, 10, 27, 6, 0, 0))
    dtu = cdb2.DatetimeUs.fromdatetime(loc_dt)

    assert type(dtu.astimezone(pytz.utc)) == cdb2.DatetimeUs

def test_datetimeus_type_stickiness():
    def check(obj):
        assert isinstance(obj, cdb2.DatetimeUs)

    new_york = pytz.timezone('America/New_York')
    utc = pytz.UTC

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
    check(cdb2.DatetimeUs.now().replace(year=2015))
    check(cdb2.DatetimeUs.now().replace(tzinfo=new_york))
    check(cdb2.DatetimeUs.now(utc).astimezone(new_york))
