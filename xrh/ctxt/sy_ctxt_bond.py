# coding:utf-8

'''
文件名：sy_ctxt_bond.py
功能：长投学堂债类表增量数据处理
代码历史：20200421，徐荣华
'''

import pymysql
import datetime
import time
import os
import json
from DBUtils.PooledDB import PooledDB



# 读取连接数据库配置文件
load_config='/home/seeyii/increase_nlp/db_config.json'
with open(load_config, 'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)

# 129csc_risk，写log
pool = PooledDB(creator=pymysql, **config_local['local_sql_csc_pool'])
db = pool.connection()
cursor = db.cursor()

# 129sy_project_raw，写数据
pool2 = PooledDB(creator=pymysql, **config_local['local_sql_project_raw_pool'])
mysql_conn = pool2.connection()
cur = mysql_conn.cursor()

# 连接一部db_seeyii，取数据
pool3 = PooledDB(creator=pymysql, **config_local['fsp_event_pool'])
mysql_conn2 = pool3.connection()
cur2 = mysql_conn2.cursor()

isValid = 1  # 是否有效，0：否，1：是
createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # sql操作时间
dataState = 1  # 数据状态:1->新增;2->更新;3->删除
logging = True
table_name = 'sy_ctxt_bond'

# 写日志的表名
mysql_log_name = 'sy_ctxt_data_log'


# 获取数据源最大交易日期
max_trade_date_sql = 'select max(TradingDay) from bond_conbdexchangequote'
cur2.execute(max_trade_date_sql)
max_trade_date_info = cur2.fetchone()
max_trade_date = max_trade_date_info[0]
max_trade_date = max_trade_date.strftime('%Y-%m-%d')
max_time = time.strptime(max_trade_date,'%Y-%m-%d')
max_time = int(time.mktime(max_time))
# 获取目标表最大交易日期
bond_max_trade_date_sql = 'select max(TradingDay) from sy_ctxt_avg_bond '
cur.execute(bond_max_trade_date_sql)
bond_max_trade_date_info = cur.fetchone()
bond_max_trade_date = bond_max_trade_date_info[0]

# 获取目标表最大交易日期
year10_max_trade_date_sql = 'select max(timeDate) from sy_ctxt_chinabond_10year '
cur.execute(year10_max_trade_date_sql)
year10_max_trade_date_info = cur.fetchone()
year10_max_trade_date = year10_max_trade_date_info[0]
year10_max_time = time.strptime(year10_max_trade_date,'%Y-%m-%d')
year10_max_time = int(time.mktime(year10_max_time))


if max_time >= year10_max_time:
    max_trade_date = year10_max_trade_date
if max_time < year10_max_time:
    max_trade_date = max_trade_date
# 查询增量数据
select_sql = '''SELECT date_format(TradingDay, '%Y-%m-%d') as TradingDay, round(avg(ClosePrice),4) as avgClosePrice
    FROM bond_conbdexchangequote where TradingDay <= '{}' and TradingDay > '{}' GROUP BY TradingDay '''.format(max_trade_date,bond_max_trade_date)

cur2.execute(select_sql)
add_infos = cur2.fetchall()
add_data_num = len(add_infos)

print(max_trade_date,bond_max_trade_date,add_data_num)

# 判断是否有新数据
if add_data_num == 0:
    print('There is no new data!')
    os._exit(0)

# 写入129数据库
insert_sql = '''insert into sy_ctxt_avg_bond(tradingDay,avgClosePrice)
                        value (%s,%s)'''
cur.executemany(insert_sql, add_infos)
mysql_conn.commit()
print('插入表sy_ctxt_avg_bond数据成功')


bond_sql = '''SELECT A.tradingDay,A.avgClosePrice,B.dailyTime,'{}','{}','{}' FROM sy_ctxt_avg_bond as A INNER JOIN 
    (select * from sy_ctxt_chinabond_10year where timeDate <= '{}' and timeDate > '{}') as B ON A.tradingDay=B.timeDate'''.format(
        isValid, createTime, dataState,max_trade_date,bond_max_trade_date)
cur.execute(bond_sql)
add_bond_infos = cur.fetchall()

insert_bond_sql = '''insert into sy_ctxt_bond(tradingDay,avgClosePrice,tenYearYield,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s)'''
cur.executemany(insert_bond_sql, add_bond_infos)
mysql_conn.commit()
print('插入表sy_ctxt_bond数据成功')

# log
if logging == True:

    sql_ = '''INSERT INTO {0} (table_,
                                add_date, 
                               add_number
                               )VALUES(%s,%s,%s)'''
    cursor.execute(sql_.format(mysql_log_name),(table_name,
                                                max_trade_date,
                                                str(add_data_num)
                                                ))
    db.commit()

mysql_conn.close()
mysql_conn2.close()
db.close()
