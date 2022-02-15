from db import setup_db
import config
import gspread


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
         ORDER BY division, ts, ranking
    """)
    records = list(res)
    # gsheet doesn't handle datetime so needs to be converted to iso format (yyyy-mm-ddTHH:MM:SS)
    return list(records[0].keys()), list(map(lambda r: (r[0], r[1].isoformat(), *r[2:]), records))


def update_sheet(spreadsheet_id, headers, rows):
    gc = gspread.service_account()
    sh = gc.open_by_key(spreadsheet_id)
    data = sh.worksheet('Data')
    data.update('A1', [headers])
    data.update('A2', rows)


if __name__ == "__main__":
    config = config.get_config()
    session = setup_db(config['dsn'])
    headers, sheet_data = get_stats_data(session)
    update_sheet(config['sheet_id'], headers, sheet_data)
