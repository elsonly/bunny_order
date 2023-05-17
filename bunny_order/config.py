import yaml
import os
from dotenv import load_dotenv

if not os.path.exists("config.yaml"):
    raise Exception("please provide config.yaml")
with open("config.yaml", "r") as stream:
    config_yaml = yaml.load(stream, Loader=yaml.CLoader)

if not os.path.exists(".env"):
    raise Exception("please provide .env")
load_dotenv()


class BaseConfig:
    GRPC_HOST = config_yaml["base"]["grpc"]["host"]
    GRPC_PORT = int(config_yaml["base"]["grpc"]["port"])
    DB_HOST = config_yaml["base"]["database"]["host"]
    DB_PORT = int(config_yaml["base"]["database"]["port"])
    DB_USER = os.environ.get("DB_USER")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    DB_DATABASE = config_yaml["base"]["database"]["database"]
    RS_HOST = config_yaml["base"]["redis"]["host"]
    RS_PORT = int(config_yaml["base"]["redis"]["port"])
    RS_DB = int(config_yaml["base"]["redis"]["db"])
    RS_PASSWORD = os.environ.get("RS_PASSWORD")
    OBSERVER_PATH = config_yaml['base']['order_observer']['path']
    OBSERVER_ORDER_FILE = config_yaml['base']['order_observer']['order_file']
    OBSERVER_TRADE_FILE = config_yaml['base']['order_observer']['trade_file']
    OBSERVER_POSITION_FILE = config_yaml['base']['order_observer']['position_file']


Config = BaseConfig()
