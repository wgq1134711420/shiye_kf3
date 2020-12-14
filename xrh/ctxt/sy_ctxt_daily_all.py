# coding:utf-8

'''
文件名：sy_ctxt_daily_all.py
功能：长投学堂数据处理每日全量
代码历史：20200408，徐荣华
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

# 写日志的表名
mysql_log_name = 'sy_ctxt_data_log'
logging = True

# 证券主表基础信息
def sy_ctxt_basic():
    table_name = 'sy_ctxt_basic'
    select_sql = '''SELECT replace(I.ChiName,' ','') as ChiName,replace(I.SecuAbbr,' ','') as SecuAbbr,I.innerCode, I.SecuCode, I.SecuMarket,
        I.SecuCategory,I.ListedDate, I.ListedSector,I.ListedState, H.level1Name, H.level2Name, '{}', '{}', '{}'
        from (select A.*,B.CompanyCode, B.innerCode from
        (SELECT ChiName,SecuAbbr,SecuCode,SecuMarket,SecuCategory,ListedDate,ListedSector,ListedState
        from secumain WHERE SecuCategory = 1 and SecuMarket in (83,90) and ListedSector in (1,2,6,7) and
        ListedState in (1,3,5)) as A inner join (SELECT * from sq_sk_basicinfo WHERE innerCode is not NULL and InnerCode != '10000014'  ) as  B on A.SecuCode=B.SecuCode) as I LEFT JOIN (SELECT * from sq_comp_pub_industry as a WHERE indClassCode in (2202) and PublishDate=(SELECT max(PublishDate) FROM sq_comp_pub_industry WHERE indClassCode in (2202) and a.InnerCode=innerCode)) as H on I.innerCode=H.innerCode'''.format(
        isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_basic'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_basic数据成功')

        insert_sql = '''insert into sy_ctxt_basic(chiName,secuAbbr,innerCode,secuCode,secuMarket,secuCategory,
        listedDate,listedSector,listedState,csrclevel1Name,csrclevel2Name,
        isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        add_data_num = len(infos)
        print('插入表sy_ctxt_basic数据成功',len(infos))
        if logging:
            max_trade_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cursor.execute(sql_.format(mysql_log_name),(table_name,
                                                        max_trade_date, 
                                                        str(add_data_num)
                                                        ))
            db.commit()

    except Exception as e:
        print(e)


# 财务指标
def sy_ctxt_promain():
    table_name = 'sy_ctxt_promain'
    select_sql = '''SELECT C.ChiName,C.InnerCode,C.SecuCode,D.endDate,D.mainBusiIncome,
    (CASE WHEN D.netProfit is not NULL THEN D.netProfit*10000 ELSE D.netProfit END) as netProfit,D.mainbusIncGrowRate,
    D.netIncGrowRate,D.roeWeighted,D.fcff,'{}','{}','{}'    
    FROM (select B.innerCode, A.chiName, A.secuCode from (SELECT SecuCode,InnerCode, ChiName FROM secumain 
    WHERE SecuCategory = 1 and SecuMarket in (83,90) and ListedSector in (1,2,6,7) and ListedState in (1,3,5))as A
    LEFT JOIN  sq_sk_basicinfo as B ON A.SecuCode=B.secuCode) as C INNER JOIN (select * FROM
    sq_fin_promain WHERE endDate >="20130331")as D ON C.innerCode=D.innerCode'''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_promain'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_promain数据成功')
        insert_sql = '''insert into sy_ctxt_promain(chiName,innerCode,secuCode,endDate,mainBusiIncome,netProfit,mainbusIncGrowRate,
        netIncGrowRate,roeWeighted,fcff,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        add_data_num = len(infos)
        print('插入表sy_ctxt_promain数据成功',len(infos))
        if logging:
            max_trade_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cursor.execute(sql_.format(mysql_log_name),(table_name,
                                                        max_trade_date, 
                                                        str(add_data_num)
                                                        ))
            db.commit()

    except Exception as e:
        print(e)


# 基金概况表
def sy_ctxt_fund_basic():
    table_name = 'sy_ctxt_fund_basic'
    select_sql = '''SELECT InnerCode, SecurityCode, MainCode,Type,FundType,InvestmentType,InvestStyle,
    EstablishmentDate,ListedDate,StartDate,ExpireDate,FoundedSize,'{}','{}','{}' FROM mf_fundarchives '''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_fund_basic'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_fund_basic数据成功')
        insert_sql = '''insert into sy_ctxt_fund_basic(innerCode, securityCode, mainCode,type,fundType,investmentType,
        investStyle,establishmentDate,listedDate,startDate,expireDate,foundedSize,isValid, createTime, dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        add_data_num = len(infos)
        print('插入表sy_ctxt_fund_basic数据成功',len(infos))
        if logging:
            max_trade_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cursor.execute(sql_.format(mysql_log_name),(table_name,
                                                        max_trade_date, 
                                                        str(add_data_num)
                                                        ))
            db.commit()

    except Exception as e:
        print(e)


# 基金产品名称表
def sy_ctxt_fund_name():
    table_name = 'sy_ctxt_fund_name'
    select_sql = '''SELECT InnerCode,InfoPublDate,InfoSource,InfoType,DisclName,EffectiveDate,ExpiryDate,
    IfEffected,'{}','{}','{}' FROM MF_FundProdName '''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_fund_name'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_fund_name数据成功')
        insert_sql = '''insert into sy_ctxt_fund_name(innerCode,infoPublDate,infoSource,infoType,disclName,
        effectiveDate,expiryDate,ifEffected,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        add_data_num = len(infos)
        print('插入表sy_ctxt_fund_name数据成功',len(infos))
        if logging:
            max_trade_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cursor.execute(sql_.format(mysql_log_name),(table_name,
                                                        max_trade_date, 
                                                        str(add_data_num)
                                                        ))
            db.commit()

    except Exception as e:
        print(e)


# 基金费率表
def sy_ctxt_fund_rate():
    table_name = 'sy_ctxt_fund_rate'
    select_sql = '''SELECT InnerCode,InfoPublDate,ChargeRateDes,DivIntervalDes,
    Notes,ChargeRateType,MinChargeRate,MaxChargeRate,'{}','{}','{}' FROM mf_chargeratenew '''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_fund_rate'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_fund_rate数据成功')
        insert_sql = '''insert into sy_ctxt_fund_rate(innerCode,infoPublDate,chargeRateDes,divIntervalDes,
    notes,chargeRateType,minChargeRate,maxChargeRate,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        add_data_num = len(infos)
        print('插入表sy_ctxt_fund_rate数据成功',len(infos))
        if logging:
            max_trade_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cursor.execute(sql_.format(mysql_log_name),(table_name,
                                                        max_trade_date, 
                                                        str(add_data_num)
                                                        ))
            db.commit()

    except Exception as e:
        print(e)


# 基金经理表
def sy_ctxt_fund_manager():
    table_name = 'sy_ctxt_fund_manager'
    select_sql = '''SELECT InnerCode,InfoPublDate,InfoSource,Name,Incumbent,
    AccessionDate,DimissionDate,ManagementTime,'{}','{}','{}' FROM mf_fundmanagernew '''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_fund_manager'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_fund_manager数据成功')
        insert_sql = '''insert into sy_ctxt_fund_manager(innerCode,infoPublDate,infoSource,name,incumbent,
    accessionDate,dimissionDate,managementTime,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        add_data_num = len(infos)
        print('插入表sy_ctxt_fund_manager数据成功',len(infos))
        if logging:
            max_trade_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cursor.execute(sql_.format(mysql_log_name),(table_name,
                                                        max_trade_date, 
                                                        str(add_data_num)
                                                        ))
            db.commit()


    except Exception as e:
        print(e)


# 指数基金与指数估值对照表
def sy_ctxt_index_contrast():
    table_name = 'sy_ctxt_index_contrast'
    select_sql = '''SELECT C.InnerCode,C.ChiName,C.SecuCode,C.SecuAbbr,D.ChiName,D.SecuCode,D.SecuAbbr,'{}','{}','{}'
    from (SELECT A.InnerCode,A.TargetIndexInnerCode, B.SecuCode, B.ChiName, B.SecuAbbr 
    FROM (SELECT * from mf_etfprlist WHERE InnerCode IS NOT NULL GROUP BY InnerCode) as A
    LEFT JOIN secumain as B ON A.InnerCode=B.InnerCode) as C LEFT JOIN secumain as D ON C.TargetIndexInnerCode=D.InnerCode'''.format(isValid, createTime,
                                                                                            dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_index_contrast'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_index_contrast数据成功')
        insert_sql = '''insert into sy_ctxt_index_contrast(innerCode,indexName,indexSecuCode,indexAbbr,fundName,
        fundSecuCode,fundAbbr,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        add_data_num = len(infos) 
        print('插入表sy_ctxt_index_contrast数据成功',len(infos))
        if logging:
            max_trade_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cursor.execute(sql_.format(mysql_log_name),(table_name,
                                                        max_trade_date, 
                                                        str(add_data_num)
                                                        ))
            db.commit()

    except Exception as e:
        print(e)




if __name__ == "__main__":
    sy_ctxt_basic()     # 证券主表基础信息 
    sy_ctxt_promain()   # 财务指标 
    sy_ctxt_fund_basic()    # 基金概况表
    sy_ctxt_fund_name()     # 基金产品名称表
    sy_ctxt_fund_rate()     # 基金费率
    sy_ctxt_fund_manager()  # 表基金经理表
    sy_ctxt_index_contrast()    # 指数基金与指数估值对照表

    # 关闭数据库
    db.close() 
    mysql_conn.close()
    mysql_conn2.close()
