import configparser
from decimal import Decimal
<<<<<<< HEAD
import streamlit as st
import os
from pages.Configure_Connection import test_connection


def load_db_config():
    """Load database configuration from profile.cfg file"""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'profile.cfg')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError("profile.cfg dosyası bulunamadı")
    
    config.read(config_path)
    
    if 'database' not in config:
        raise ValueError("profile.cfg dosyasında [database] bölümü bulunamadı")
    
    db_type = config['database'].get('type', 'mssql').lower()
    
    # Temel yapılandırma
    db_config = {
        'type': db_type,
        'host': config['database'].get('host', 'localhost'),
        'port': config['database'].getint('port', 1433 if db_type == 'mssql' else 3306),
        'user': config['database'].get('user'),
        'password': config['database'].get('password'),
        'schema': config['database'].get('schema', 'dbo' if db_type == 'mssql' else 'public')
    }
    
    # Veritabanı tipine göre özel alanlar
    if db_type == 'mysql':
        db_config['database'] = config['database'].get('dbname')
        required_fields = ['database', 'user', 'password']
    else:  # MSSQL
        db_config['dbname'] = config['database'].get('dbname')
        required_fields = ['dbname', 'user', 'password']
    
    # Gerekli alanların kontrolü
    missing_fields = [field for field in required_fields if not db_config.get(field)]
    
    if missing_fields:
        raise ValueError(f"profile.cfg dosyasında eksik alanlar: {', '.join(missing_fields)}")
    
    return db_config


def decimal_to_float(value):
    return float(value) if isinstance(value, Decimal) else value


def check_connection():
    """Check if database connection is configured"""
    try:
        config = load_db_config()
        return True
    except Exception as e:
        print(f"Veritabanı bağlantı hatası: {str(e)}")
        return False
=======


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
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
