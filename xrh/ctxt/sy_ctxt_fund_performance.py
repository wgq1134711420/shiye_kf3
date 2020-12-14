# coding:utf-8

'''
文件名：sy_ctxt_main_basic.py
功能：长投学堂基金回报率表增量数据处理
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
table_name = 'sy_ctxt_fund_performance'

# 写日志的表名
mysql_log_name = 'sy_ctxt_data_log'


# 获取数据源最大交易日期
max_trade_date_sql = 'select max(TradingDay) from mf_netvalueperformanceHis'
cur2.execute(max_trade_date_sql)
max_trade_date_info = cur2.fetchone()
max_trade_date = max_trade_date_info[0]

# 获取目标表最大交易日期
fund_max_trade_date_sql = 'select max(tradingDay) from sy_ctxt_fund_performance '
cur.execute(fund_max_trade_date_sql)
fund_max_trade_date_info = cur.fetchone()
fund_max_trade_date = fund_max_trade_date_info[0]



# 查询增量数据
select_sql = '''SELECT InnerCode,TradingDay,NVDailyGrowthRate,RRInSingleWeek,
    RRInSingleMonth,RRInSingleYear,RRInThreeYear,'{}','{}','{}' FROM mf_netvalueperformanceHis where TradingDay >'{}' and TradingDay <='{}' '''.format(isValid, createTime, dataState, fund_max_trade_date, max_trade_date)

cur2.execute(select_sql)
add_infos = cur2.fetchall()
add_data_num = len(add_infos)

print(max_trade_date,fund_max_trade_date,add_data_num)

# 判断是否有新数据
if add_data_num == 0:
    print('There is no new data!')
    with open('/data1/sy_ctxt_fund_performance.log','a') as f:
        f.write('sy_ctxt_fund_performance没有取到新数据\n')
    os._exit(0)

# 写入129数据库
insert_sql = '''insert into sy_ctxt_fund_performance(innerCode,tradingDay,nVDailyGrowthRate,rRInSingleWeek,
    rRInSingleMonth,rRInSingleYear,rRInThreeYear,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
cur.executemany(insert_sql, add_infos)
mysql_conn.commit()
print('插入表sy_ctxt_fund_performance数据成功')



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
