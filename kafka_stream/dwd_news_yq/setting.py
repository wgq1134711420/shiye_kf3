# -*- coding:utf-8 -*-


import os

from config import conf

# 项目路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# log路径
LOG_PATH_BASE = "logs/"

# redis
REDIS_HOST = conf.get('REDIS', 'host')
REDIS_PORT = conf.get('REDIS', 'port')
REDIS_DB = conf.get('REDIS', 'db')
REDIS_PASSWORD = conf.get('REDIS', 'password')

# mongo
MONGO_URL = conf.get('MONGO', 'mongo_url')
MONGO_DB = conf.get('MONGO', 'database')

# rabbit
MQ_HOST = conf.get('MQ', 'host')
MQ_PORT = conf.getint('MQ', 'port')
MQ_USER = conf.get('MQ', 'user')
MQ_PASSWORD = conf.get('MQ', 'password')

# es
ES_HOST_PORT = [{"host": item[1].split(":")[0], "port": item[1].split(":")[1]} for item in conf.items('ES') if 'node' in item[0]]
ES_INDEX = conf.get('ES', 'index')

# 数据库引擎URI
DB_SERVER_URI = conf.get('DB_SERVER', 'mysql_url')
