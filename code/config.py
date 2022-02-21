import os
from configparser import ConfigParser

DSN = "postgresql+psycopg2://{username}:{password}@{host}:{port}/{db}"


def get_config():
    ini_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'auth.ini'))
    config = ConfigParser()
    config.read(ini_path)
    return {
        "dsn": DSN.format_map(dict(config['db'])),
        "sheet_id": config['sheet']['id']
    }
