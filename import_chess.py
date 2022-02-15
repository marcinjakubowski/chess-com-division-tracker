import urllib3
from bs4 import BeautifulSoup
import json

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




def get_user_games(division: Division, username: str):
    year = division.start_time.year
    month = division.start_time.month

    games = list(get_user_games_for_month(division, year, month, username))

    if division.end_time.year != year or division.end_time.month != month:
        games.extend(get_user_games_for_month(division, division.end_time.year, division.end_time.month, username))

    return games


def get_user_games_for_month(division, year, month, username: str):
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

    for game in res['games']:
        data = {'username': username, 'division': division.id}
        for field in fields:
            data[field.replace(":", "_")] = get_from_json(game, field)

        game = Game(data)
        if division.start_time <= game.end_time <= division.end_time:
            yield game





if __name__ == "__main__":
    config = config.get_config()
    session = setup_db(config['dsn'])

    for division in get_active_divisions(session):
        standings = get_division_data(division.id)
        session.add_all(standings)
        for player in division.players:
            games = get_user_games(division, player)
            stmt = pg_insert(Game).values(list(map(lambda g: g.to_dict(), games))).on_conflict_do_nothing()
            session.execute(stmt)
        session.commit()

