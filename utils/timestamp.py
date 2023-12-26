import time


def get_current_hour_timestamp():
    timestamp = int(time.time())
    hour = timestamp // 3600 * 3600 * 1000
    return hour
