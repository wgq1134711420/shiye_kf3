'''
文件名：copyAGuSourceData.py
功能：招商项目A股数据源表更新
代码历史：20190522，徐荣华
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
        host='172.17.23.128',
        port=6033,
        user='seeyiidata',
        passwd='shiye1805A',
        db='db_seeyii',
        charset='utf8'
    )

    return mysql_conn

createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # sql操作时间

def copySourceData1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT innerCode,endDate,netProfit,totsharEqui from sq_fin_promain where innerCode is not null '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sq_fin_promain'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功1')
        insert_sql = '''insert into sq_fin_promain(innerCode,endDate,netProfit,totsharEqui)
                        value (%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功1')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def create_db():
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    sql_1 ='''
            CREATE TABLE `sy_ctxt_main_totmktcap` (
              `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
              `chiName` varchar(200) DEFAULT NULL COMMENT '公司名称',
              `secuAbbr` varchar(100) DEFAULT NULL COMMENT '证券简称',
              `secuCode` varchar(10) NOT NULL COMMENT '证券代码',
              `innerCode` varchar(100) DEFAULT NULL COMMENT '关联代码',
              `companyCode` varchar(100) DEFAULT NULL COMMENT '公司代码',
              `tradeDate`  varchar(8) DEFAULT NULL COMMENT '交易日期',
              `totmktcap` decimal(19, 4) DEFAULT NULL COMMENT '总市值 元',
              `modifyUpdateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
              PRIMARY KEY (`id`, `secuCode`),
              KEY `indexName` (`secuCode`)
            ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
            '''
    

    sql_2 ='''
        CREATE TABLE `sy_ctxt_main_pe` (
          `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
          `chiName` varchar(200) DEFAULT NULL COMMENT '公司名称',
          `secuAbbr` varchar(100) DEFAULT NULL COMMENT '证券简称',
          `secuCode` varchar(10) NOT NULL COMMENT '证券代码',
          `innerCode` varchar(100) DEFAULT NULL COMMENT '关联代码',
          `companyCode` varchar(100) DEFAULT NULL COMMENT '公司代码',
          `tradeDate`  varchar(8) DEFAULT NULL COMMENT '交易日期',
          `totmktcap` decimal(19, 4) DEFAULT NULL COMMENT '总市值 元',
          `pe` decimal(19, 4) DEFAULT NULL COMMENT 'pe值',
          `modifyUpdateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
          PRIMARY KEY (`id`, `secuCode`),
          KEY `indexName` (`secuCode`)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
        '''

    sql_3 ='''
        CREATE TABLE `sy_ctxt_main_pb` (
          `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
          `chiName` varchar(200) DEFAULT NULL COMMENT '公司名称',
          `secuAbbr` varchar(100) DEFAULT NULL COMMENT '证券简称',
          `secuCode` varchar(10) NOT NULL COMMENT '证券代码',
          `innerCode` varchar(100) DEFAULT NULL COMMENT '关联代码',
          `companyCode` varchar(100) DEFAULT NULL COMMENT '公司代码',
          `tradeDate`  varchar(8) DEFAULT NULL COMMENT '交易日期',
          `totmktcap` decimal(19, 4) DEFAULT NULL COMMENT '总市值 元',
          `pe` decimal(19, 4) DEFAULT NULL COMMENT 'pe值',
          `totsharEqui` decimal(19, 4) DEFAULT NULL COMMENT '净资产 元',
          `pb` decimal(19, 4) DEFAULT NULL COMMENT 'pb值',
          `modifyUpdateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
          PRIMARY KEY (`id`, `secuCode`),
          KEY `indexName` (`secuCode`)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
        '''

    sql_4 ='''
        CREATE TABLE `sy_ctxt_main_pbb` (
          `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
          `chiName` varchar(200) DEFAULT NULL COMMENT '公司名称',
          `secuAbbr` varchar(100) DEFAULT NULL COMMENT '证券简称',
          `secuCode` varchar(10) NOT NULL COMMENT '证券代码',
          `innerCode` varchar(100) DEFAULT NULL COMMENT '关联代码',
          `companyCode` varchar(100) DEFAULT NULL COMMENT '公司代码',
          `tradeDate`  varchar(8) DEFAULT NULL COMMENT '交易日期',
          `totmktcap` decimal(19, 4) DEFAULT NULL COMMENT '总市值 元',
          `pe` decimal(19, 4) DEFAULT NULL COMMENT 'pe值',
          `totsharEqui` decimal(19, 4) DEFAULT NULL COMMENT '净资产 元',
          `pb` decimal(19, 4) DEFAULT NULL COMMENT 'pb值',
          `pbb` decimal(19, 4) DEFAULT NULL COMMENT 'pb值(不含商誉)',
          `modifyUpdateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
          PRIMARY KEY (`id`, `secuCode`),
          KEY `indexName` (`secuCode`)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
        '''

    cur2.execute(sql_4)
    mysql_conn2.close()

# cur2.execute(sql_3)
# mysql_conn2.close()

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
    copySourceData1()
    # create_db()
    # columns_message()