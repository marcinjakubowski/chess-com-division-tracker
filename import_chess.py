from functools import reduce
import urllib3
from bs4 import BeautifulSoup
import json
from datetime import datetime
from common import generate_months
from db import Game, Stat, Division, Db
import re


Http = urllib3.PoolManager()


def standing_to_record(division, el):
    return Stat(division, el['username'], int(el['stats']['trophyCount']), int(el['stats']['ranking']))


def get_division_data(division):
    url = f'https://www.chess.com/leagues/champion/{division}'
    req = Http.request('GET', url)
    soup = BeautifulSoup(req.data, 'html.parser')
    leagues = soup.find(id='leagues-division')
    json_res = json.loads(leagues.attrs['data-json'])
    rankings = list(map(lambda rec: standing_to_record(division, rec), json_res['divisionData']['standings']))
    return rankings


def get_user_games(username: str, start_date: datetime, end_date: datetime):
    return filter(lambda g: start_date <= g.end_time <= end_date,
                  reduce(lambda x, y: x+y,
                         [
                             list(get_user_games_for_month(username, year, month))
                             for year, month in generate_months(start_date, end_date)
                         ])
                  )


def get_user_games_by_url(url):
    username = re.match(r".*\/player\/(.*?)/", url).groups()[0]
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


def get_user_games_for_month(username: str, year: int, month: int):
    url = f"https://api.chess.com/pub/player/{username}/games/{year}/{month:02d}"
    return get_user_games_by_url(url)


if __name__ == "__main__":
    db = Db()

    for division in db.get_active_divisions():
        for player in division.players:
            games = list(map(lambda g: {**g.to_dict(), "division": division.id},
                             get_user_games(player, division.start_time, division.end_time)))
            db.add_games(games, commit=False)
        standings = get_division_data(division.id)
        db.add_standings(standings)

