import urllib3
from bs4 import BeautifulSoup
import json
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from configparser import ConfigParser
import gspread
import re
import datetime

SQLALCHEMY_DSN = "postgresql+psycopg2://{username}:{password}@{host}:{port}/{db}"

Base = declarative_base()
Http = urllib3.PoolManager()


class Division(Base):
    __tablename__ = 'division'
    id = sqlalchemy.Column(sqlalchemy.Text, primary_key=True)
    description = sqlalchemy.Column(sqlalchemy.Text)
    players = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1))
    is_active = sqlalchemy.Column(sqlalchemy.Boolean)
    start_time = sqlalchemy.Column(sqlalchemy.DateTime)
    end_time = sqlalchemy.Column(sqlalchemy.DateTime)


class Stat(Base):
    __tablename__ = 'division_stats'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    ts = sqlalchemy.Column(sqlalchemy.DateTime, server_default=func.now(), onupdate=func.now())
    division = sqlalchemy.Column(sqlalchemy.String(length=100), sqlalchemy.ForeignKey('division.id'))
    username = sqlalchemy.Column(sqlalchemy.String(length=100))
    trophy = sqlalchemy.Column(sqlalchemy.Integer)
    ranking = sqlalchemy.Column(sqlalchemy.Integer)

    def __init__(self, division, username, trophy, ranking):
        self.division = division
        self.username = username
        self.trophy = trophy
        self.ranking = ranking


class Game(Base):
    __tablename__ = 'games'
    id = sqlalchemy.Column(UUID(as_uuid=True), primary_key=True)
    username = sqlalchemy.Column(sqlalchemy.String(length=100))
    division = sqlalchemy.Column(sqlalchemy.String(length=100), sqlalchemy.ForeignKey('division.id'))
    white_username = sqlalchemy.Column(sqlalchemy.String(length=100))
    white_rating = sqlalchemy.Column(sqlalchemy.Integer)
    white_result = sqlalchemy.Column(sqlalchemy.String(length=100))
    black_username = sqlalchemy.Column(sqlalchemy.String(length=100))
    black_rating = sqlalchemy.Column(sqlalchemy.Integer)
    black_result = sqlalchemy.Column(sqlalchemy.String(length=100))
    url = sqlalchemy.Column(sqlalchemy.String(length=1000))
    time_control = sqlalchemy.Column(sqlalchemy.String(length=100))
    time_class = sqlalchemy.Column(sqlalchemy.String(length=100))
    rules = sqlalchemy.Column(sqlalchemy.String(length=100))
    end_time = sqlalchemy.Column(sqlalchemy.DateTime)
    opening = sqlalchemy.Column(sqlalchemy.Text)
    opening_code = sqlalchemy.Column(sqlalchemy.Text)

    def to_dict(self):
        return dict(
            (key, value) for key, value in self.__dict__.items() if not callable(value) and not key.startswith('_')
        )

    @staticmethod
    def get_opening_code(pgn: str):
        if pgn is None:
            return None

        opening = re.findall(r'\[ECO \"(.*)\"\]', pgn)
        if len(opening) == 0:
            return None
        return opening[0]

    @staticmethod
    def get_opening(pgn: str):
        if pgn is None:
            return None

        opening = re.findall(r'\[ECOUrl \"https://www.chess.com/openings/(.*)\"\]', pgn)
        if len(opening) == 0:
            return None
        return opening[0].replace("-", " ")

    def __init__(self, data):
        self.id = data['uuid']
        self.white_username = data['white_username']
        self.white_rating = int(data['white_rating'])
        self.white_result = data['white_result']
        self.black_username = data['black_username']
        self.black_rating = int(data['black_rating'])
        self.black_result = data['black_result']
        self.url = data['url']
        self.time_control = data['time_control']
        self.rules = data['rules']
        self.end_time = datetime.datetime.fromtimestamp(data['end_time'])
        self.username = data['username']
        self.time_class = data['time_class']
        self.opening = Game.get_opening(data['pgn'])
        self.opening_code = Game.get_opening_code(data['pgn'])
        self.division = data['division']


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


def setup_db(dsn):
    engine = sqlalchemy.create_engine(dsn)
    Base.metadata.create_all(engine)
    session_maker = sqlalchemy.orm.sessionmaker()
    session_maker.configure(bind=engine)
    return session_maker()


def get_active_divisions(session):
    return session.query(Division).where(Division.is_active)


def update_sheet(spreadsheet_id, rows):
    gc = gspread.service_account()
    sh = gc.open_by_key(spreadsheet_id)
    data = sh.worksheet('Data')
    data.update('A2', rows)


def get_stats_data(cursor):
    res = cursor.execute("""
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

    return list(map(lambda r: (r[0], r[1].isoformat(), *r[2:]), records))


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


def get_config():
    config = ConfigParser()
    config.read("auth.ini")
    return {
        "dsn": SQLALCHEMY_DSN.format_map(dict(config['db'])),
        "sheet_id": config['sheet']['id']
    }


if __name__ == "__main__":
    config = get_config()
    session = setup_db(config['dsn'])

    for division in get_active_divisions(session):
        standings = get_division_data(division.id)
        session.add_all(standings)
        for player in division.players:
            games = get_user_games(division, player)
            stmt = pg_insert(Game).values(list(map(lambda g: g.to_dict(), games))).on_conflict_do_nothing()
            session.execute(stmt)
        session.commit()

    # update sheet
    sheet_data = get_stats_data(session)
    update_sheet(config['sheet_id'], sheet_data)
