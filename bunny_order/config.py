import yaml
import os
from dotenv import load_dotenv

if not os.path.exists(".env"):
    raise Exception("please provide .env")
load_dotenv()
env_flag = os.environ.get("ENV", "local")

if not os.path.exists("config.yaml"):
    raise Exception("please provide config.yaml")
with open("config.yaml", "r") as stream:
    data = yaml.load(stream, Loader=yaml.CLoader)
config_yaml = data[env_flag]

class BaseConfig:
    DB_HOST = config_yaml["database"]["host"]
    DB_PORT = int(config_yaml["database"]["port"])
    DB_USER = os.environ.get("DB_USER")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    DB_DATABASE = config_yaml["database"]["database"]
    OBSERVER_BASE_PATH = config_yaml["observer"]["base_path"]
    OBSERVER_SF31_ORDERS_DIR = config_yaml["observer"]["sf31_orders_dir"]
    OBSERVER_XQ_SIGNALS_DIR = config_yaml["observer"]["xq_signals_dir"]
    OBSERVER_ORDER_CALLBACK_DIR = config_yaml["observer"]["order_callback_dir"]
    OBSERVER_ORDER_CALLBACK_FILE = config_yaml["observer"]["order_callback_file"]
    OBSERVER_TRADE_CALLBACK_FILE = config_yaml["observer"]["trade_callback_file"]
    OBSERVER_POSITION_CALLBACK_FILE = config_yaml["observer"]["position_callback_file"]
    


    LOGURU_SINK_DIR = config_yaml["loguru"]["sink_dir"]
    LOGURU_SINK_FILE = config_yaml["loguru"]["sink_file"]


Config = BaseConfig()
