import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
import re
import datetime

import config

Base = declarative_base()


class Division(Base):
    __tablename__ = 'division'
    id = sqlalchemy.Column(sqlalchemy.Text, primary_key=True)
    level = sqlalchemy.Column(sqlalchemy.Text)
    description = sqlalchemy.Column(sqlalchemy.Text)
    players = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1))
    is_active = sqlalchemy.Column(sqlalchemy.Boolean)
    start_time = sqlalchemy.Column(sqlalchemy.DateTime)
    end_time = sqlalchemy.Column(sqlalchemy.DateTime)

    def __init__(self, id, level, description, start_time, end_time, is_active):
        self.id = id
        self.level = level
        self.description = description
        self.start_time = start_time
        self.end_time = end_time
        self.is_active = is_active

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
        self.end_time = datetime.datetime.utcfromtimestamp(data['end_time'])
        self.username = data['username']
        self.time_class = data['time_class']
        self.opening = Game.get_opening(data['pgn'])
        self.opening_code = Game.get_opening_code(data['pgn'])
        self.division = data.get('division')

    def __repr__(self):
        return f"Game({self.id})\n"


def setup_db(dsn):
    engine = sqlalchemy.create_engine(dsn)
    Base.metadata.create_all(engine)
    session_maker = sqlalchemy.orm.sessionmaker()
    session_maker.configure(bind=engine)
    return session_maker()


class Db:
    session = None

    def get_active_divisions(self):
        return self.session.query(Division).where(Division.is_active)

    def __init__(self, dsn=None):
        if dsn is None:
            dsn = config.get_config()['dsn']
        self.session = setup_db(dsn)

    def add_games(self, games, commit=True):
        stmt = pg_insert(Game).values(list(map(lambda g: g.to_dict() if isinstance(g, Game) else g, games))).on_conflict_do_nothing()
        self.session.execute(stmt)
        if commit:
            self.session.commit()

    def add_standings(self, standings, commit=True):
        self.session.add_all(standings)
        if commit:
            self.session.commit()
