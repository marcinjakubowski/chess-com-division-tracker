from functools import reduce
import urllib3
from bs4 import BeautifulSoup
import json
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert as pg_insert
from db import Game, Stat, Division, setup_db
import config


Http = urllib3.PoolManager()


def standing_to_record(division, el):
    return Stat(division, el['username'], int(el['stats']['trophyCount']), int(el['stats']['ranking']))


def get_active_divisions(session):
    return session.query(Division).where(Division.is_active)


def get_division_data(division):
    url = f'https://www.chess.com/leagues/champion/{division}'
    req = Http.request('GET', url)
    soup = BeautifulSoup(req.data, 'html.parser')
    leagues = soup.find(id='leagues-division')
    json_res = json.loads(leagues.attrs['data-json'])
    rankings = list(map(lambda rec: standing_to_record(division, rec), json_res['divisionData']['standings']))
    return rankings


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


def get_user_games(username: str, start_date: datetime, end_date: datetime):
    return filter(lambda g: start_date <= g.end_time <= end_date,
                  reduce(lambda x, y: x+y,
                         [
                             list(get_user_games_for_month(username, year, month))
                             for year, month in generate_months(start_date, end_date)
                         ])
                  )


def get_user_games_for_month(username: str, year: int, month: int):
    url = f"https://api.chess.com/pub/player/{username}/games/{year}/{month:02d}"
    req = Http.request('GET', url)
    res = json.loads(req.data)

    fields = [
        'url', 'time_control', 'end_time', 'time_class', 'rules',
        'white:username', 'white:rating', 'white:result',
        'black:username', 'black:rating', 'black:result', 'uuid', 'time_class', 'pgn'
    ]

    def get_from_json(obj, path: str):
        try:
            first, second = path.split(':')
            return obj[first][second]
        except ValueError:
            return obj.get(path)

    for game in res.get('games', []):
        data = {'username': username}
        for field in fields:
            data[field.replace(":", "_")] = get_from_json(game, field)

        yield Game(data)


if __name__ == "__main__":
    config = config.get_config()
    session = setup_db(config['dsn'])

    for division in get_active_divisions(session):
        standings = get_division_data(division.id)
        session.add_all(standings)
        for player in division.players:
            games = get_user_games(player, division.start_time, division.end_time)
            stmt = pg_insert(Game).values(list(map(lambda g: {
                **g.to_dict(),
                "division": division.id
            }, games))).on_conflict_do_nothing()
            session.execute(stmt)
        session.commit()

