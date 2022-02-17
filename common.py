from datetime import datetime


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