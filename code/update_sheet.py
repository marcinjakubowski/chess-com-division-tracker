import config
import gspread
import sys

from common import retry
from db import setup_db


def get_stats_data(session):
    res = session.execute("""
        SELECT stats.division
             , stats.ts
             , stats.username
             , stats.trophy
             , stats.ranking
             , stats.delta_60min
             , stats.is_most_recent
             , stats.downtime
             , stats.delta_1d
             , stats.delta_2d
             , stats.downtime_1d
             , stats.downtime_2d 
          FROM division_stats_overview stats
         ORDER BY division, ts DESC, ranking
    """)
    records = list(res)
    # gsheet doesn't handle datetime so needs to be converted to iso format (yyyy-mm-ddTHH:MM:SS)
    return list(records[0].keys()), list(map(lambda r: (r[0], r[1].isoformat(), *r[2:]), records))


@retry
def update_sheet(spreadsheet_id, headers, rows):
    gc = gspread.service_account()
    sh = gc.open_by_key(spreadsheet_id)
    data = sh.worksheet('Data')
    # clear the worksheet, the entire dataset is uploaded anyway
    # and in case a new division starts, it will have fewer rows than the previous one
    data.clear()
    data.update('A1', [headers])
    data.update('A2', rows)


if __name__ == "__main__":
    config = config.get_config()
    session = setup_db(config['dsn'])
    headers, sheet_data = get_stats_data(session)
    sheet_id = sys.argv[1] if len(sys.argv) > 1 else config['sheet_id']
    update_sheet(sheet_id, headers, sheet_data)
