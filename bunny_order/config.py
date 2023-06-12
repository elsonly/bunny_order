import yaml
import os
from dotenv import load_dotenv
import datetime as dt

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
    DEBUG = env_flag == "local"
    # db
    DB_HOST = config_yaml["database"]["host"]
    DB_PORT = int(config_yaml["database"]["port"])
    DB_USER = os.environ.get("DB_USER")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    DB_DATABASE = config_yaml["database"]["database"]
    # observer
    OBSERVER_BASE_PATH = config_yaml["observer"]["base_path"]
    OBSERVER_SF31_ORDERS_DIR = config_yaml["observer"]["sf31_orders_dir"]
    OBSERVER_XQ_SIGNALS_DIR = config_yaml["observer"]["xq_signals_dir"]
    OBSERVER_ORDER_CALLBACK_DIR = config_yaml["observer"]["order_callback_dir"]
    OBSERVER_ORDER_CALLBACK_FILE = config_yaml["observer"]["order_callback_file"]
    OBSERVER_TRADE_CALLBACK_FILE = config_yaml["observer"]["trade_callback_file"]
    OBSERVER_POSITION_CALLBACK_FILE = config_yaml["observer"]["position_callback_file"]
    # engine
    TRADE_START_TIME = dt.time(
        hour=int(config_yaml["engine"]["trade_start_time"][:2]),
        minute=int(config_yaml["engine"]["trade_start_time"][2:4]),
        second=0,
    )
    TRADE_END_TIME = dt.time(
        hour=int(config_yaml["engine"]["trade_end_time"][:2]),
        minute=int(config_yaml["engine"]["trade_end_time"][2:4]),
        second=0,
    )
    SYNC_START_TIME = dt.time(
        hour=int(config_yaml["engine"]["sync_start_time"][:2]),
        minute=int(config_yaml["engine"]["sync_start_time"][2:4]),
        second=0,
    )
    SYNC_END_TIME = dt.time(
        hour=int(config_yaml["engine"]["sync_end_time"][:2]),
        minute=int(config_yaml["engine"]["sync_end_time"][2:4]),
        second=0,
    )
    UPDATE_CONTRACTS_TIME = dt.time(
        hour=int(config_yaml["engine"]["update_contracts_time"][:2]),
        minute=int(config_yaml["engine"]["update_contracts_time"][2:4]),
        second=0,
    )
    RESET_TIME1 = dt.time(
        hour=int(config_yaml["engine"]["reset_time1"][:2]),
        minute=int(config_yaml["engine"]["reset_time1"][2:4]),
        second=0,
    )
    RESET_TIME2 = dt.time(
        hour=int(config_yaml["engine"]["reset_time2"][:2]),
        minute=int(config_yaml["engine"]["reset_time2"][2:4]),
        second=0,
    )
    SIGNAL_TIME = dt.time(
        hour=int(config_yaml["engine"]["signal_time"][:2]),
        minute=int(config_yaml["engine"]["signal_time"][2:4]),
        second=0,
    )
    # exit handler
    QUOTE_DELAY_TOLERANCE = int(config_yaml["exit_handler"]["quote_delay_tolerance"])
    CHECKPOINTS_DIR = config_yaml["common"]["checkpoints_dir"]
    # order manager
    OM_DAILY_AMOUNT_LIMIT = config_yaml["order_manager"]["daily_amount_limit"]
    # loguru
    LOGURU_SINK_DIR = config_yaml["loguru"]["sink_dir"]
    LOGURU_SINK_FILE = config_yaml["loguru"]["sink_file"]
    LOGURU_LOG_LEVEL = config_yaml["loguru"]["level"]


Config = BaseConfig()
