from datetime import datetime
from time import sleep


def flat_map(f, xs):
    ys = []
    for x in xs:
        ys.extend([f(el) for el in x])
    return ys


def generate_months(start_date: datetime, end_date: datetime):
    year = start_date.year
    month = start_date.month
    end_year = end_date.year
    end_month = end_date.month

    while True:
        yield year, month

        if year == end_year and month == end_month:
            return

        month = month % 12 + 1
        year = year if month != 1 else year + 1


def retry(func=None, /, *, retries=5, delay=1.0):
    def inner(func):
        def wrapper(*args, **kwargs):
            retry_count = 0
            while True:
                # noinspection PyBroadException
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retry_count += 1
                    print(f"retrying {retry_count}...")
                    if retry_count >= retries:
                        raise e
                    sleep(delay)
        
        return wrapper
    
    # so that the decorator can be used with or without params
    return inner if func is None else inner(func)


