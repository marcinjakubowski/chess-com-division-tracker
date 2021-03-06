import re
import urllib3
import json
from bs4 import BeautifulSoup
from datetime import datetime
from functools import reduce

from common import generate_months, retry
from db import Game, Stat, Division

Http = urllib3.PoolManager()

def _standing_to_record(division, el):
    return Stat(division, el['username'], int(el['stats']['trophyCount']), int(el['stats']['ranking']))


@retry
def get_player_division(username):
    url = f'https://www.chess.com/callback/leagues/user-league/search/{username}'
    req = Http.request('GET', url)
    res = json.loads(req.data)

    div = res['division']
    id = div['name'][1:] # first letter is the indication of type
    
    level = div['league']['code']
    # divisions end at 19:59:59 a week after they start at 20:00:00
    start_time = datetime.utcfromtimestamp(div['endTime'] + 1 - 7 * 24 * 3600) 
    end_time = datetime.utcfromtimestamp(div['endTime'])
    description = f"{div['league']['name']} - {div['year']} Week {div['week']}"
    is_active = div['inProgress']

    return Division(id, level, description, start_time, end_time, is_active)

@retry
def get_division_data(level, division):
    url = f'https://www.chess.com/leagues/{level}/{division}'
    req = Http.request('GET', url)
    soup = BeautifulSoup(req.data, 'html.parser')
    leagues = soup.find(id='leagues-division')
    json_res = json.loads(leagues.attrs['data-json'])
    rankings = list(map(lambda rec: _standing_to_record(division, rec), json_res['divisionData']['standings']))
    return rankings


def get_user_games(username: str, start_date: datetime, end_date: datetime):
    return filter(lambda g: start_date <= g.end_time <= end_date,
                  reduce(lambda x, y: x+y,
                         [
                             list(get_user_games_for_month(username, year, month))
                             for year, month in generate_months(start_date, end_date)
                         ])
                  )


@retry
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