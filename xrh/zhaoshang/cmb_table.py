'''
文件名：cmb_table.py
功能：招商项目数据同步
代码历史：20200728 邢冬梅
'''

import pymysql
import datetime
import time
import json
from DBUtils.PooledDB import PooledDB
# 读取连接数据库配置文件
load_config='/home/seeyii/increase_nlp/db_config.json'
with open(load_config, 'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)

# 129sy_project_raw，写数据
pool2 = PooledDB(creator=pymysql, **config_local['local_sql_project_raw_pool'])
mysql_conn2 = pool2.connection()
cur2 = mysql_conn2.cursor()

# 129csc_risk，写log
pool3 = PooledDB(creator=pymysql, **config_local['local_sql_csc_pool'])
mysql_conn3 = pool3.connection()
cur3 = mysql_conn3.cursor()

def conn_mysql(dbName):
    mysql_conn = pymysql.connect(
        host='0.0.0.0',
        port=33006,
        user='root',
        passwd='123456',
        db=dbName,
        charset='utf8'
    )

    return mysql_conn

isValid = 1  # 是否有效，0：否，1：是
createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # sql操作时间
dataState = 1  # 数据状态:1->新增;2->更新;3->删除
logging = True
mysql_log_name = "dwd_ms_data_log"

# 同步数据A谷
def dwd_ms_acomp_manager_all():
    table_name = "dwd_ms_acomp_manager_all"
    # docker 连接方式
    mysql_conn = conn_mysql('CMB')
    cur = mysql_conn.cursor()
    # 交易日
    trade_sql = '''select tradeDate from sq_qt_skdailyprice group by tradeDate order by tradeDate DESC LIMIT 1'''
    cur.execute(trade_sql)
    tradeDate = cur.fetchone()[0]

    select_sql = "select CName,number,companyName,secuAbbr,SocialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companytype,secuCode,totmktcap,guoqi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sum_price,CHGMONEY,total_price,lastdate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd, fiveYearSumTotalMomey,lastYeardivPublishDate,lastYeartoAccountDate, lastYearTotalMoney, holderMoneySource,'{}','{}','{}' from tmp_sq_comp_manager_main_27".format(isValid,  dataState, createTime)
    try:  
        cur.execute(select_sql)
        infos = cur.fetchall()
        insert_sql = '''insert into dwd_ms_acomp_manager_all (cName,number,companyName,secuAbbr,socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,isValid,dataStatus,createTime)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur2.executemany(val, infos)
        mysql_conn2.commit()
        print('A股同步成功')
        add_data_num = len(infos)
        if logging:
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cur3.execute(sql_.format(mysql_log_name),(table_name,
                                                        tradeDate,
                                                        str(add_data_num)
                                                        ))
            mysql_conn3.commit()
    except Exception as e:
        print(e)

    finally:
        mysql_conn.close()
        mysql_conn2.close()
        mysql_conn3.close()

# 同步数据A股解禁
def dwd_ms_abcomp_manager_all():
    table_name = "dwd_ms_abcomp_manager_all"
    # docker
    mysql_conn = conn_mysql('CMB')
    cur = mysql_conn.cursor()
    # 交易日
    trade_sql = '''select tradeDate from sq_qt_skdailyprice group by tradeDate order by tradeDate DESC LIMIT 1'''
    cur.execute(trade_sql)
    tradeDate = cur.fetchone()[0]

    select_sql = "SELECT CName,number,companyName,secuAbbr,SocialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companytype,secuCode,totmktcap,guoqi,    actdutyName, beginDate,  endDate,    dimReason,  gender, birthday,   age,    school, degree, titles, memo,   holderMoney,    sum_price,  listDate,   newListingSKAmt,    limskMoney, sumLimskMoney, holderMoneySource,  '{}', '{}', '{}' FROM tmp_sq_comp_manager_main_26_1 ".format(isValid,  dataState, createTime)
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        insert_sql = '''insert into dwd_ms_abcomp_manager_all (cName,number, companyName, secuAbbr,socialUnifiedCreditCode,registeredProvinces, registeredCity, operationProvinces, operationCity, companyType,secuCode,totmktcap,guoQi,  actdutyName,beginDate,endDate, dimReason,  gender, birthday,   age,    school, degree, titles, memo,   holderMoney,    sumPrice,   listDate,   newListingSKAmt,    limskMonrey,    sumLimskMoney,holderMoneySource, isValid,dataStatus,createTime)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur2.executemany(val, infos)
        mysql_conn2.commit()
        print('A股解禁数据同步成功')
        add_data_num = len(infos)
        if logging:
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cur3.execute(sql_.format(mysql_log_name),(table_name,
                                                        tradeDate,
                                                        str(add_data_num)
                                                        ))
            mysql_conn3.commit()
    except Exception as e:
        print(e)

    finally:
        mysql_conn.close()
        mysql_conn2.close()
        mysql_conn3.close()

# 同步数据港股数据
def dwd_ms_hcomp_manager_all():
    table_name = "dwd_ms_hcomp_manager_all"
    # docker 连接方式
    mysql_conn = conn_mysql('cmb_ganggu')
    cur = mysql_conn.cursor()
    # 交易日
    trade_sql = '''select TradingDay from qt_hkdailyquoteindex group by TradingDay order by TradingDay DESC LIMIT 1'''
    cur.execute(trade_sql)
    tradeDate = cur.fetchone()[0]

    select_sql = "SELECT cName,number,companyName,secuAbbr,socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,'{}','{}','{}' FROM cmb_ganggu_result_1".format(isValid,  dataState, createTime)
    try:  
        cur.execute(select_sql)
        infos = cur.fetchall()
        insert_sql = '''insert into dwd_ms_hcomp_manager_all (cName,number,companyName,secuAbbr,socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,isValid,dataStatus,createTime)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur2.executemany(val, infos)
        mysql_conn2.commit()
        print('港股同步成功')
        add_data_num = len(infos)
        if logging:
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cur3.execute(sql_.format(mysql_log_name),(table_name,
                                                        tradeDate,
                                                        str(add_data_num)
                                                        ))
            mysql_conn3.commit()
    except Exception as e:
        print(e)

    finally:
        mysql_conn.close()
        mysql_conn2.close()
        mysql_conn3.close()

# 同步数据三板数据
def dwd_ms_fcomp_manager_all():
    table_name = "dwd_ms_fcomp_manager_all"
    # docker 连接方式
    mysql_conn = conn_mysql('cmb_sanban')
    cur = mysql_conn.cursor()
    # 交易日
    trade_sql = '''select TradingDay from nq_dailyquote group by TradingDay order by TradingDay DESC LIMIT 1'''
    cur.execute(trade_sql)
    tradeDate = cur.fetchone()[0]

    select_sql = "SELECT LeaderName,number,ChiName, SecuAbbr,CreditCode,RegAddr,RegCity,OfficeAddr,OfficeCity,'三板创新层' as companyType,SecuCode,MarketValue,StateComp,PosnName,BeginDate,EndDate,ChangeType,Gender,BirthDate,age,school,EducationLevel,person_title,Background,holderMoney,fiveYearSumMoney,TransValue,Last_TransValue, Last_TransDate,IfPledgeHoldSum, 0.0 as fiveYearAvgAnnualRewArd,0.0 as oneYearAnnualRewArd, 0.0 as oneYearCompAnnualRewArd,fiveYearBonusMoney,sMDeciPublDate, toAccountDate, oneYearBonusMoney,holderMoneySource, '{}', '{}', '{}' from tmp_sq_comp_manager_main_13   ".format(isValid,  dataState, createTime)
    try:  
        cur.execute(select_sql)
        infos = cur.fetchall()
        insert_sql = '''insert into dwd_ms_fcomp_manager_all (cName,number,companyName,secuAbbr,socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,isValid,dataStatus,createTime)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur2.executemany(val, infos)
        mysql_conn2.commit()
        print('三板同步成功')
        add_data_num = len(infos)
        if logging:
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cur3.execute(sql_.format(mysql_log_name),(table_name,
                                                        tradeDate,
                                                        str(add_data_num)
                                                        ))
            mysql_conn3.commit()
    except Exception as e:
        print(e)

    finally:
        mysql_conn.close()
        mysql_conn2.close()
        mysql_conn3.close()

# 同步数据IPO数据
def dwd_ms_icomp_manager_all():
    mysql_conn2 = pool2.connection()
    cur2 = mysql_conn2.cursor()
    
    mysql_conn3 = pool3.connection()
    cur3 = mysql_conn3.cursor()
    table_name = "dwd_ms_icomp_manager_all"
    Last_tradeDate = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400))
    tradeDate = time.strftime("%Y-%m-%d", time.localtime(time.time()))

    m_sql = "select max(add_date) from dwd_ms_data_log where table_= '{}' ".format(str(table_name))
    cur3.execute(m_sql)
    modifyTime = cur3.fetchone()[0]
    if modifyTime == tradeDate:
        print("无新数据")
        return
    try:
        select_sql = """select cName,number,companyName,secuAbbr,\
        socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,\
        operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,\
        endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,\
        sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,\
        oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,\
        lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,isValid,dataStatus,createTime\
         from dwd_ms_icomp_manager_all where modifyTime > '{}'  and  modifyTime < '{}'  """.format(str(Last_tradeDate),str(tradeDate))
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        insert_sql = '''insert into dwd_ms_icomp_manager_all (cName,number,companyName,secuAbbr,socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,isValid,dataStatus,createTime)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur2.executemany(val, infos)
        mysql_conn2.commit()
        add_data_num = len(infos)
        print("dwd_ms_icomp_manager_all,更新数据成功")
        if logging:
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cur3.execute(sql_.format(mysql_log_name),(table_name,
                                                        tradeDate,
                                                        str(add_data_num)
                                                        ))
            mysql_conn3.commit()


    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn2.close()
        mysql_conn3.close()

# 同步数据美股数据
def dwd_ms_amcomp_manager_all():
    mysql_conn2 = pool2.connection()
    cur2 = mysql_conn2.cursor()

    mysql_conn3 = pool3.connection()
    cur3 = mysql_conn3.cursor()
    table_name = "dwd_ms_amcomp_manager_all"
    Last_tradeDate = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400))
    tradeDate = time.strftime("%Y-%m-%d", time.localtime(time.time()))

    m_sql = "select max(add_date) from dwd_ms_data_log where table_= '{}' ".format(str(table_name))
    cur3.execute(m_sql)
    modifyTime = cur3.fetchone()[0]
    if modifyTime == tradeDate:
        print("无新数据")
        return

    try:
        select_sql = """select cName,number,companyName,secuAbbr,\
        socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,\
        operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,\
        endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,\
        sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,\
        oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,\
        lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,isValid,dataStatus,createTime\
         from dwd_ms_amcomp_manager_all where modifyTime >'{}' and modifyTime <'{}' """.format(str(Last_tradeDate),str(tradeDate))
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        insert_sql = '''insert into dwd_ms_amcomp_manager_all (cName,number,companyName,secuAbbr,socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,isValid,dataStatus,createTime)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur2.executemany(val, infos)
        mysql_conn2.commit()
        add_data_num = len(infos)
        print("dwd_ms_amcomp_manager_all,更新数据成功")
        if logging:
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cur3.execute(sql_.format(mysql_log_name),(table_name,
                                                        tradeDate,
                                                        str(add_data_num)
                                                        ))
            mysql_conn3.commit()


    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn2.close()
        mysql_conn3.close()

# 同步数据民营独角兽数据
def dwd_ms_ucomp_manager_all():
    mysql_conn2 = pool2.connection()
    cur2 = mysql_conn2.cursor()
    
    mysql_conn3 = pool3.connection()
    cur3 = mysql_conn3.cursor()
    table_name = "dwd_ms_ucomp_manager_all"
    Last_tradeDate = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400))
    tradeDate = time.strftime("%Y-%m-%d", time.localtime(time.time()))

    m_sql = "select max(add_date) from dwd_ms_data_log where table_= '{}' ".format(str(table_name))
    cur3.execute(m_sql)
    modifyTime = cur3.fetchone()[0]
    if modifyTime == tradeDate:
        print("无新数据")
        return

    try:
        select_sql = """select cName,number,companyName,secuAbbr,\
        socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,\
        operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,\
        endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,\
        sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,\
        oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,\
        lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,isValid,dataStatus,createTime\
         from dwd_ms_ucomp_manager_all where modifyTime > '{}' and  modifyTime < '{}'  """.format(str(Last_tradeDate),str(tradeDate))
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        add_data_num = len(infos)
        if add_data_num == 0:
            print("无新数据")
            return
        insert_sql = '''insert into dwd_ms_ucomp_manager_all (cName,number,companyName,secuAbbr,socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,isValid,dataStatus,createTime)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur2.executemany(val, infos)
        mysql_conn2.commit()
        
        print("dwd_ms_ucomp_manager_all,更新数据成功")
        if logging:
            sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
            cur3.execute(sql_.format(mysql_log_name),(table_name,
                                                        tradeDate,
                                                        str(add_data_num)
                                                        ))
            mysql_conn3.commit()


    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn2.close()
        mysql_conn3.close()


if __name__ == "__main__":
    dwd_ms_ucomp_manager_all()
    dwd_ms_amcomp_manager_all()
    dwd_ms_icomp_manager_all()