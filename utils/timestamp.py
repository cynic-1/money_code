import time


def get_current_hour_timestamp_ms():
    return get_current_hour_timestamp_s() * 1000


def get_current_hour_timestamp_s():
    timestamp = int(time.time())
    hour = timestamp // (8*3600) * (8*3600)
    return hour
