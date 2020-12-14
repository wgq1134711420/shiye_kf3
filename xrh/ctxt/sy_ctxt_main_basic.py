# coding:utf-8

'''
文件名：sy_ctxt_main_basic.py
功能：长投学堂证券主表增量数据处理
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
table_name = 'sy_ctxt_main_basic'

# 写日志的表名
mysql_log_name = 'sy_ctxt_data_log'


# 获取数据源最大交易日期
max_trade_date_sql = 'select max(tradeDate) from sq_qt_skdailyprice'
cur2.execute(max_trade_date_sql)
max_trade_date_info = cur2.fetchone()
max_trade_date = max_trade_date_info[0]
max_time = time.strptime(max_trade_date,'%Y%m%d')
max_time = int(time.mktime(max_time))
# 注意转时间格式

max_lc_trade_date_sql = 'select max(TradingDay) from lc_dindicesforvaluation'
cur2.execute(max_lc_trade_date_sql)
max_lc_trade_date_info = cur2.fetchone()
max_lc_trade_date = max_lc_trade_date_info[0]
max_lc_trade_date = max_lc_trade_date.strftime('%Y%m%d')
max_lc_time = time.strptime(max_lc_trade_date,'%Y%m%d')
max_lc_time = int(time.mktime(max_lc_time))

# 获取目标表最大交易日期
basic_max_trade_date_sql = 'select max(tradeDate) from sy_ctxt_main_basic '
cur.execute(basic_max_trade_date_sql)
basic_max_trade_date_info = cur.fetchone()
basic_max_trade_date = basic_max_trade_date_info[0]

if max_lc_trade_date > max_trade_date:
    max_trade_date = max_trade_date

if max_lc_trade_date < max_trade_date:
    max_trade_date = max_lc_trade_date


# 查询增量数据
select_sql = '''SELECT I.ChiName,I.SecuAbbr,I.InnerCode, I.SecuCode, I.SecuMarket,
    I.SecuCategory,I.ListedDate, I.ListedSector,I.ListedState, H.level1Name, H.level2Name
    ,(case when I.DividendRatioLYR is null then 0 else I.DividendRatioLYR*100 end) as DividendRatioLYR, I.tradeDate, I.tOpen,I.tClose, I.pchg,'{}','{}','{}'
    from (select * from (SELECT E.ChiName,E.SecuAbbr,E.binnerCode as InnerCode, E.SecuCode, E.SecuMarket, E.SecuCategory,E.ListedDate, E.ListedSector,E.ListedState,F.DividendRatioLYR, E.tradeDate, E.tOpen,E.tClose, E.pchg FROM (
    SELECT C.InnerCode,C.ChiName,C.SecuAbbr,C.binnerCode, C.SecuCode, C.SecuMarket, C.SecuCategory,C.ListedDate, C.ListedSector,C.ListedState,D.tradeDate, D.tOpen,D.tClose, D.pchg  FROM (select A.*,B.CompanyCode, B.innerCode as binnerCode from (SELECT InnerCode,ChiName,SecuAbbr,SecuCode,SecuMarket,SecuCategory,ListedDate,ListedSector,ListedState 
    from secumain WHERE SecuCategory = 1 and SecuMarket in (83,90) and ListedSector in (1,2,6,7) and ListedState in (1,3,5)) as A left join sq_sk_basicinfo as  B on A.SecuCode=B.SecuCode ) as C left join
    sq_qt_skdailyprice as D on D.companyCode=C.CompanyCode where tradeDate <= '{}' and tradeDate > '{}') as E left JOIN lc_dindicesforvaluation as F ON E.InnerCode=F.InnerCode and F.TradingDay=E.tradeDate)as G WHERE G.tradeDate is not NULL)as I LEFT JOIN (SELECT * from sq_comp_pub_industry as a WHERE indClassCode in (2202) and PublishDate=(SELECT max(PublishDate) FROM sq_comp_pub_industry WHERE indClassCode in (2202) and a.InnerCode=innerCode)) as H on I.innerCode=H.innerCode '''.format(
        isValid, createTime, dataState,max_trade_date,basic_max_trade_date)

cur2.execute(select_sql)
add_infos = cur2.fetchall()
add_data_num = len(add_infos)
print(max_trade_date,basic_max_trade_date,add_data_num)

# 判断是否有新数据
if add_data_num == 0:
    print('There is no new data!')
    with open('/data1/sy_ctxt_main_basic.log','a') as f:
        info = 'INFO'
        diff_time = max_time-max_lc_time
        T = max_trade_date
        table = ''
        if diff_time >= (86400):
            info = 'ERROR'
            T = max_lc_trade_date
            table = 'lc_dindicesforvaluation'

        if diff_time <= (-86400):
            info = 'ERROR'
            T = max_trade_date
            table = 'sq_qt_skdailyprice'
        f.write('sy_ctxt_main_basic没有取到新数据,'+info+','+T+','+table+'\n')

    os._exit(0)

# 写入129数据库
insert_sql = '''insert into sy_ctxt_main_basic(chiName,secuAbbr,innerCode,secuCode,secuMarket,secuCategory,
        listedDate,listedSector,listedState,csrclevel1Name,csrclevel2Name,
        dividendRatioLYR,tradeDate,tOpen,tClose,pchg,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
cur.executemany(insert_sql, add_infos)
mysql_conn.commit()
print('插入表sy_ctxt_main_basic数据成功')



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

