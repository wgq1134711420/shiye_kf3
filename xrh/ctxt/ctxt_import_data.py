# -*- coding:utf-8 -*-

'''
文件名：ctxt_import_data.py
功能：长投项目数据传输
代码历史：20200420，徐荣华
'''

import pymysql
import datetime
import time

def conn_mysql():
    mysql_conn = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='shiye',
        db='sy_project_raw',
        charset='utf8'
    )

    return mysql_conn

def conn_mysql2():
    mysql_conn = pymysql.connect(
        host='0.0.0.0',
        port=33006,
        user='root',
        passwd='123456',
        db='CMB',
        charset='utf8'
    )

    return mysql_conn

createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # sql操作时间


# 估值指标表
def sy_ctxt_main_assess_value():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    # 取数据库所有的innerCode，按innerCode导入数据
    inner_code_list = []
    inner_code_sql = '''SELECT innerCode from sy_ctxt_main_assess_value_five GROUP BY innerCode'''
    cur.execute(inner_code_sql)
    inner_code_infos = cur.fetchall()
    for inner_code_info in inner_code_infos:
        inner_code_list.append(inner_code_info[0])

    # 取目标数据库所有的innerCode，导入过的数据不再重复导入
    inner_code_list2 = []
    # inner_code_sql2 = '''SELECT innerCode from sy_ctxt_main_assess_value_five GROUP BY innerCode'''
    # cur2.execute(inner_code_sql2)
    # inner_code_infos = cur2.fetchall()
    # for inner_code_info2 in inner_code_infos2:
    #     inner_code_list2.append(inner_code_info2[0])

    # 导入数据，按innerCode顺序导入
    for innerCode in inner_code_list[0:10]:
      if innerCode in inner_code_list2:
        continue
      select_sql = '''SELECT chiName,innerCode,secuCode,tradeDate,totmktcap,
                              pe,peFiveQuantile,peTenQuantile,peQuantile,pb,pbFiveQuantile,pbTenQuantile,pbQuantile,
                              pbb,pbbFiveQuantile,pbbTenQuantile,pbbQuantile,isValid,createTime,dataState
                              from sy_ctxt_main_assess_value_five where innerCode = '{}' '''.format(innerCode)
      try:
          cur.execute(select_sql)
          infos = cur.fetchall()
          insert_sql = '''insert into sy_ctxt_main_assess_value(chiName,innerCode,secuCode,tradeDate,totmktcap,
                              pe,peFiveQuantile,peTenQuantile,peQuantile,pb,pbFiveQuantile,pbTenQuantile,pbQuantile,
                              pbb,pbbFiveQuantile,pbbTenQuantile,pbbQuantile,isValid,createTime,dataState)
                              value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
          cur2.executemany(insert_sql, infos)
          mysql_conn2.commit()
          print('导入sy_ctxt_main_assess_value数据成功:', innerCode)

      except Exception as e:
          print(e)
    mysql_conn2.close()
    mysql_conn.close()

# 证券主表
def sy_ctxt_main_basic():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    # 取数据库所有的secuCode，按secuCode导入数据
    secu_code_list = []
    secu_code_sql = '''SELECT secuCode from sy_ctxt_main_basic GROUP BY secuCode'''
    cur.execute(secu_code_sql)
    secu_code_infos = cur.fetchall()
    for secu_code_info in secu_code_infos:
        secu_code_list.append(secu_code_info[0])

    # 取目标数据库所有的secuCode，导入过的数据不再重复导入
    secu_code_list2 = []
    # secu_code_sql2 = '''SELECT secuCode from sy_ctxt_main_basic GROUP BY secuCode'''
    # cur2.execute(secu_code_sql2)
    # secu_code_infos = cur2.fetchall()
    # for secu_code_info2 in secu_code_infos2:
    #     secu_code_list2.append(secu_code_info2[0])

    # 导入数据，按secuCode顺序导入
    for secuCode in secu_code_list[0:10]:
      if secuCode in secu_code_list2:
        continue
      select_sql = '''SELECT chiName,secuAbbr,innerCode, secuCode, secuMarket,
                          secuCategory,listedDate, listedSector,listedState,csrclevel1Name, csrclevel2Name
                          ,dividendRatioLYR, tradeDate, tOpen,tClose, pchg,isValid,createTime,dataState
                              from sy_ctxt_main_basic where secuCode = '{}' '''.format(secuCode)
      try:
          cur.execute(select_sql)
          infos = cur.fetchall()
          insert_sql = '''insert into sy_ctxt_main_basic(chiName,secuAbbr,innerCode, secuCode, secuMarket,
                          secuCategory,listedDate, listedSector,listedState,csrclevel1Name, csrclevel2Name
                          ,dividendRatioLYR, tradeDate, tOpen,tClose, pchg,isValid,createTime,dataState)
                          value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
          cur2.executemany(insert_sql, infos)
          mysql_conn2.commit()
          print('导入sy_ctxt_main_basic数据成功:', secuCode)

      except Exception as e:
          print(e)
    mysql_conn2.close()
    mysql_conn.close()

# 财务指标表
def sy_ctxt_promain():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    # 取数据库所有的secuCode，按secuCode导入数据
    secu_code_list = []
    secu_code_sql = '''SELECT secuCode from sy_ctxt_promain GROUP BY secuCode'''
    cur.execute(secu_code_sql)
    secu_code_infos = cur.fetchall()
    for secu_code_info in secu_code_infos:
        secu_code_list.append(secu_code_info[0])

    # 取目标数据库所有的secuCode，导入过的数据不再重复导入
    secu_code_list2 = []
    # secu_code_sql2 = '''SELECT secuCode from sy_ctxt_promain GROUP BY secuCode'''
    # cur2.execute(secu_code_sql2)
    # secu_code_infos = cur2.fetchall()
    # for secu_code_info2 in secu_code_infos2:
    #     secu_code_list2.append(secu_code_info2[0])

    # 导入数据，按secuCode顺序导入
    for secuCode in secu_code_list[0:10]:
      if secuCode in secu_code_list2:
        continue
      select_sql = '''SELECT chiName,innerCode,secuCode,endDate,mainBusiIncome,netProfit,
          mainbusIncGrowRate,netIncGrowRate,roeWeighted,isValid,createTime,dataState
                              from sy_ctxt_promain where secuCode = '{}' '''.format(secuCode)
      try:
          cur.execute(select_sql)
          infos = cur.fetchall()
          insert_sql = '''insert into sy_ctxt_promain(chiName,innerCode,secuCode,endDate,mainBusiIncome,netProfit,
          mainbusIncGrowRate,netIncGrowRate,roeWeighted,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
          cur2.executemany(insert_sql, infos)
          mysql_conn2.commit()
          print('导入sy_ctxt_promain数据成功:', secuCode)

      except Exception as e:
          print(e)
    mysql_conn2.close()
    mysql_conn.close()

# 指数估值指标表
def sy_ctxt_index():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    # 取数据库所有的secuCode，按secuCode导入数据
    secu_code_list = []
    secu_code_sql = '''SELECT secuCode from sy_ctxt_index GROUP BY secuCode'''
    cur.execute(secu_code_sql)
    secu_code_infos = cur.fetchall()
    for secu_code_info in secu_code_infos:
        secu_code_list.append(secu_code_info[0])

    # 取目标数据库所有的secuCode，导入过的数据不再重复导入
    secu_code_list2 = []
    # secu_code_sql2 = '''SELECT secuCode from sy_ctxt_index GROUP BY secuCode'''
    # cur2.execute(secu_code_sql2)
    # secu_code_infos = cur2.fetchall()
    # for secu_code_info2 in secu_code_infos2:
    #     secu_code_list2.append(secu_code_info2[0])

    # 导入数据，按secuCode顺序导入
    for secuCode in secu_code_list[0:10]:
      if secuCode in secu_code_list2:
        continue
      select_sql = '''SELECT chiName,secuCode,secuAbbr,tradingDay,indicatorType,statisPeriod,
                          indiPercentile,isValid,createTime,dataState
                              from sy_ctxt_index where secuCode = '{}' '''.format(secuCode)
      try:
          cur.execute(select_sql)
          infos = cur.fetchall()
          insert_sql = '''insert into sy_ctxt_index(chiName,secuCode,secuAbbr,tradingDay,indicatorType,statisPeriod,
                          indiPercentile,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
          cur2.executemany(insert_sql, infos)
          mysql_conn2.commit()
          print('导入sy_ctxt_index数据成功:', secuCode)

      except Exception as e:
          print(e)
    mysql_conn2.close()
    mysql_conn.close()

# 指数估值分位数表
def sy_ctxt_index_pe():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    # 取数据库所有的secuCode，按secuCode导入数据
    secu_code_list = []
    secu_code_sql = '''SELECT secuCode from sy_ctxt_index_pe GROUP BY secuCode'''
    cur.execute(secu_code_sql)
    secu_code_infos = cur.fetchall()
    for secu_code_info in secu_code_infos:
        secu_code_list.append(secu_code_info[0])

    # 取目标数据库所有的secuCode，导入过的数据不再重复导入
    secu_code_list2 = []
    # secu_code_sql2 = '''SELECT secuCode from sy_ctxt_index_pe GROUP BY secuCode'''
    # cur2.execute(secu_code_sql2)
    # secu_code_infos = cur2.fetchall()
    # for secu_code_info2 in secu_code_infos2:
    #     secu_code_list2.append(secu_code_info2[0])

    # 导入数据，按secuCode顺序导入
    for secuCode in secu_code_list[0:10]:
      if secuCode in secu_code_list2:
        continue
      select_sql = '''SELECT chiName,secuCode,secuAbbr,tradingDay,pe,pb,isValid,createTime,dataState
                              from sy_ctxt_index_pe where secuCode = '{}' '''.format(secuCode)
      try:
          cur.execute(select_sql)
          infos = cur.fetchall()
          insert_sql = '''insert into sy_ctxt_index_pe(chiName,secuCode,secuAbbr,tradingDay,pe,pb,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
          cur2.executemany(insert_sql, infos)
          mysql_conn2.commit()
          print('导入sy_ctxt_index_pe数据成功:', secuCode)

      except Exception as e:
          print(e)
    mysql_conn2.close()
    mysql_conn.close()


# 获取表列名
def columns_message():
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    sql = '''SELECT COLUMN_NAME  FROM information_schema.columns
WHERE TABLE_NAME = 'sq_sk_incentiveobjlist' '''
    try:
        cur2.execute(sql)
        info = cur2.fetchall()
        l = []
        for i in info:
            l.append(i[0])
        s = ','.join(l)
        print(len(l))
        print(s)


    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()


if __name__ == "__main__":
    # sy_ctxt_main_assess_value()
    # sy_ctxt_main_basic()
    # sy_ctxt_promain()
    # sy_ctxt_index()
    sy_ctxt_index_pe()
    # columns_message()