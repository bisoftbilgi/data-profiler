import configparser
from decimal import Decimal


def load_db_config(filename="profile.cfg", section="database"):
    config = configparser.ConfigParser()
    config.read(filename)
    if section in config:
        db_conf = {key: value for key, value in config[section].items()}

        # Convert port to int if exists
        if "port" in db_conf:
            db_conf["port"] = int(db_conf["port"])

        return db_conf

    raise Exception(f"Section '{section}' not found in {filename}")


def decimal_to_float(value):
    return float(value) if isinstance(value, Decimal) else value