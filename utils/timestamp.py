import time


def get_current_hour_timestamp_ms():
    return get_current_hour_timestamp_s() * 1000


def get_current_hour_timestamp_s():
    timestamp = int(time.time())
    hour = timestamp // (24*3600) * (24*3600)
    return hour

def get_current_day_timestamp_ms():
    return get_current_day_timestamp_s() * 1000

def get_current_day_timestamp_s():
    timestamp = int(time.time())
    day = timestamp // (24*3600) * (24*3600)
    return day