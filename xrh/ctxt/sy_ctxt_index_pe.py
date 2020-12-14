# coding:utf-8

'''
文件名：sy_ctxt_index_pe.py
功能：指数估值指标表增量数据处理
代码历史：20200424，徐荣华
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
table_name = 'sy_ctxt_index_pe'

# 写日志的表名
mysql_log_name = 'sy_ctxt_data_log'


# 获取数据源最大交易日期
max_trade_date_sql = 'select max(TradingDay) from lc_indexderivative'
cur2.execute(max_trade_date_sql)
max_trade_date_info = cur2.fetchone()
max_trade_date = max_trade_date_info[0]

# 获取目标表最大交易日期
index_pe_max_trade_date_sql = 'select max(tradingDay) from sy_ctxt_index_pe '
cur.execute(index_pe_max_trade_date_sql)
index_pe_max_trade_date_info = cur.fetchone()
index_pe_max_trade_date = index_pe_max_trade_date_info[0]

# 查询增量数据
select_sql = '''SELECT C.ChiName,C.SecuCode,C.SecuAbbr, D.TradingDay,D.PE_LYR, D.PB_LF,'{}','{}','{}' from 
    (SELECT A.TargetIndexInnerCode,A.InnerCode, A.InfoSource, B.SecuCode, B.ChiName, B.SecuAbbr 
    FROM (SELECT * from mf_etfprlist WHERE TargetIndexInnerCode IS NOT NULL GROUP BY TargetIndexInnerCode) as A
    LEFT JOIN secumain as B ON A.TargetIndexInnerCode=B.InnerCode) 
    as C INNER JOIN (select * from lc_indexderivative where TradingDay <= '{}' and TradingDay > '{}') as D ON C.TargetIndexInnerCode=D.IndexCode ORDER BY SecuCode,TradingDay '''.format(
        isValid, createTime, dataState,max_trade_date,index_pe_max_trade_date)

cur2.execute(select_sql)
add_infos = cur2.fetchall()
add_data_num = len(add_infos)

print(max_trade_date,index_pe_max_trade_date,add_data_num)

# 判断是否有新数据
if add_data_num == 0:
    print('There is no new data!')
    os._exit(0)

# 写入129数据库
insert_sql = '''insert into sy_ctxt_index_pe(chiName,secuCode,secuAbbr,tradingDay,pe,pb,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
cur.executemany(insert_sql, add_infos)
mysql_conn.commit()
print('插入表sy_ctxt_index_pe数据成功')

if 120 > add_data_num or add_data_num > 200:
    print('ERORR数据入库数量异常')

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
