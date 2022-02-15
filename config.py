from configparser import ConfigParser

DSN = "postgresql+psycopg2://{username}:{password}@{host}:{port}/{db}"


def get_config():
    config = ConfigParser()
    config.read("auth.ini")
    return {
        "dsn": DSN.format_map(dict(config['db'])),
        "sheet_id": config['sheet']['id']
    }
