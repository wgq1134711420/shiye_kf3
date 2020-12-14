
import configparser
import os

# 实例化configparser
conf = configparser.ConfigParser()

# 获取当前的绝对路径
current_path = os.path.abspath(__file__)
# 获取配置文件夹
current_dir = os.path.dirname(current_path)
# 拼接配置文件路径
config_path = os.path.join(current_dir, 'config.ini')
# 读取config.ini
conf.read(config_path)
