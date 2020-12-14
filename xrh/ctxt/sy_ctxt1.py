# coding:utf-8

'''
文件名：sy_ctxt.py
功能：长投学堂数据处理
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
modifyUpdateTime = createTime  # 最后修改时间


five_trade_date_list = []
ten_trade_date_list = []
all_trade_date_list = []

inner_code_list = []
inner_code_sql = '''SELECT innerCode from sy_ctxt_main_assess_value GROUP BY innerCode'''
cur.execute(inner_code_sql)
inner_code_infos = cur.fetchall()
for inner_code_info in inner_code_infos:
    inner_code_list.append(inner_code_info[0])

inner_code_sql2 = '''SELECT innerCode from sy_ctxt_main_assess_value2 GROUP BY innerCode'''
cur.execute(inner_code_sql2)
inner_code_infos2 = cur.fetchall()
for inner_code_info2 in inner_code_infos2:
    inner_code_list.append(inner_code_info2[0])

# find_trade_date_sql = '''SELECT a.innerCode,a.maxTradeDate,b.minTradeDate, DATEDIFF( STR_TO_DATE(a.tenTradeDate,'%Y%m%d'),STR_TO_DATE(b.minTradeDate,'%Y%m%d')) as ten,
# DATEDIFF( STR_TO_DATE(a.fiveTradeDate,'%Y%m%d'),STR_TO_DATE(b.minTradeDate,'%Y%m%d')) as five
# from(SELECT CONCAT((CONVERT(left(Max(tradeDate),4),SIGNED) - 10),right(Max(tradeDate),4)) as tenTradeDate,
# CONCAT((CONVERT(left(Max(tradeDate),4),SIGNED) - 5),right(Max(tradeDate),4)) as fiveTradeDate,innerCode,Max(tradeDate) as maxTradeDate from sy_ctxt_main_pbb GROUP BY innerCode)as a INNER JOIN
# (SELECT Min(tradeDate) as minTradeDate,innerCode from sy_ctxt_main_pbb GROUP BY innerCode) as b on a.innerCode = b.innerCode ORDER BY innerCode'''
# cur.execute(find_trade_date_sql)
# find_trade_date_infos = cur.fetchall()


# for trade_date_info in find_trade_date_infos:
#     innerCode = trade_date_info[0]
#     if innerCode in inner_code_list:
#         continue
#     five_trade_date = trade_date_info[4]
#     ten_trade_date = trade_date_info[3]
#     if ten_trade_date > 0:
#         all_trade_date_list.append(innerCode)
#         continue
#     if ten_trade_date <= 0 and five_trade_date > 0:
#         ten_trade_date_list.append(innerCode)
#         continue
#     if five_trade_date <= 0:
#         five_trade_date_list.append(innerCode)

# print(all_trade_date_list[0:10],len(all_trade_date_list))

all_inner_code_list = []
with open('/home/seeyii/xrh/ctxt/innerCode.txt', 'r', encoding="utf-8") as f:
    reader = f.readlines()
    for i in reader[1000:1100]:
        inner_code = i.replace('\n', '')
        if inner_code in inner_code_list:
        	continue
        all_inner_code_list.append(inner_code)

print(all_inner_code_list,len(all_inner_code_list))

if len(all_inner_code_list) == 0:
    print('There is no new data!')
    os._exit(0)

# 对每个交易日的pe,pb,pbb排序，并计算对应的分为点
i = 0
for innerCode in all_inner_code_list:
    i += 1
    select_sql = '''SELECT chiName,innerCode,secuCode,tradeDate,totmktcap,pe,pb,pbb from sy_ctxt_main_pbb where innerCode = '{}' '''.format(innerCode)
    try:
        # 历史所有分为点计算
        peFiveQuantile = None
        peTenQuantile = None
        pbFiveQuantile = None
        pbTenQuantile = None
        pbbFiveQuantile = None
        pbbTenQuantile = None
        cur.execute(select_sql)
        infos = cur.fetchall()
        for info in infos:
            chiName = info[0]
            # secuAbbr = info[1]
            innerCode = info[1]
            secuCode = info[2]
            tradeDate = info[3]
            totmktcap = info[4]
            if totmktcap:
                totmktcap = round(totmktcap*10000, 4)
            pe = info[5]
            pb = info[6]
            pbb = info[7]
            count_sql = '''SELECT count(*) as count,secuCode from sy_ctxt_main_pbb
            WHERE tradeDate <= '{}' and secuCode = '{}' '''.format(tradeDate, secuCode)
            cur.execute(count_sql)
            count_info = cur.fetchone()
            count = count_info[0]
            pe_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
   pe
    from
   ( 
        select * from `sy_ctxt_main_pbb` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and secuCode = '{}' order by pe  

    ) as A) as c on b.secuCode = c.secuCode and b.pe = c.pe '''.format(tradeDate, secuCode, tradeDate, secuCode)
            cur.execute(pe_rn_sql)
            pe_rn_info = cur.fetchone()
            if pe_rn_info:
                pe_rn = pe_rn_info[0]
                if count > 1:
                    peQuantile = (pe_rn - 1) / (count - 1)
                    peQuantile = round(peQuantile * 100, 4)
                else:
                    peQuantile = 0
            else:
                peQuantile = None
            pb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
   pb
    from
   ( 
        select * from `sy_ctxt_main_pbb` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and secuCode = '{}' order by pb  

    ) as A) as c on b.secuCode = c.secuCode and b.pb = c.pb '''.format(tradeDate, secuCode, tradeDate, secuCode)
            cur.execute(pb_rn_sql)
            pb_rn_info = cur.fetchone()
            if pb_rn_info:
                pb_rn = pb_rn_info[0]
                if count > 1:
                    pbQuantile = (pb_rn - 1) / (count - 1)
                    pbQuantile = round(pbQuantile * 100, 4)
                else:
                    pbQuantile = 0
            else:
                pbQuantile = None
            pbb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
   pbb
    from
   ( 
        select * from `sy_ctxt_main_pbb` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and secuCode = '{}' order by pbb  

    ) as A) as c on b.secuCode = c.secuCode and b.pbb = c.pbb '''.format(tradeDate, secuCode, tradeDate, secuCode)
            cur.execute(pbb_rn_sql)
            pbb_rn_info = cur.fetchone()
            if pbb_rn_info:
                pbb_rn = pbb_rn_info[0]
                if count > 1:
                    pbbQuantile = (pbb_rn - 1) / (count - 1)
                    pbbQuantile = round(pbbQuantile * 100, 4)
                else:
                    pbbQuantile = 0
            else:
                pbbQuantile = None
            
            # 五年分为点计算
            five_trade_date = str(int(tradeDate[0:4]) - 5) + tradeDate[4:8]
            five_count_sql = '''SELECT count(*) as count,secuCode from sy_ctxt_main_pbb
            WHERE tradeDate >= '{}' and tradeDate <= '{}' and secuCode = '{}' '''.format(five_trade_date,tradeDate, secuCode)
            cur.execute(five_count_sql)
            five_count_info = cur.fetchone()
            five_count = five_count_info[0]
            five_pe_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
   pe
    from
   ( 
        select * from `sy_ctxt_main_pbb` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pe  

    ) as A) as c on b.secuCode = c.secuCode and b.pe = c.pe '''.format(tradeDate, secuCode, tradeDate,five_trade_date, secuCode)
            cur.execute(five_pe_rn_sql)
            five_pe_rn_info = cur.fetchone()
            if five_pe_rn_info:
                five_pe_rn = five_pe_rn_info[0]
                if five_count > 1:
                    peFiveQuantile = (five_pe_rn - 1) / (five_count - 1)
                    peFiveQuantile = round(peFiveQuantile * 100, 4)
                else:
                    peFiveQuantile = 0
            else:
                peFiveQuantile = None
            five_pb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
   pb
    from
   ( 
        select * from `sy_ctxt_main_pbb` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pb  

    ) as A) as c on b.secuCode = c.secuCode and b.pb = c.pb '''.format(tradeDate, secuCode, tradeDate,five_trade_date, secuCode)
            cur.execute(five_pb_rn_sql)
            five_pb_rn_info = cur.fetchone()
            if five_pb_rn_info:
                five_pb_rn = five_pb_rn_info[0]
                if five_count > 1:
                    pbFiveQuantile = (five_pb_rn - 1) / (five_count - 1)
                    pbFiveQuantile = round(pbFiveQuantile * 100, 4)
                else:
                    pbFiveQuantile = 0
            else:
                pbFiveQuantile = None
            five_pbb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
   pbb
    from
   ( 
        select * from `sy_ctxt_main_pbb` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pbb  

    ) as A) as c on b.secuCode = c.secuCode and b.pbb = c.pbb '''.format(tradeDate, secuCode, tradeDate,five_trade_date, secuCode)
            cur.execute(five_pbb_rn_sql)
            five_pbb_rn_info = cur.fetchone()
            if five_pbb_rn_info:
                five_pbb_rn = five_pbb_rn_info[0]
                if five_count > 1:
                    pbbFiveQuantile = (five_pbb_rn - 1) / (five_count - 1)
                    pbbFiveQuantile = round(pbbFiveQuantile * 100, 4)
                else:
                    pbbFiveQuantile = 0
            else:
                pbbFiveQuantile = None

            # 十年分为点计算
            ten_trade_date = str(int(tradeDate[0:4]) - 10) + tradeDate[4:8]
            ten_count_sql = '''SELECT count(*) as count,secuCode from sy_ctxt_main_pbb
            WHERE tradeDate >= '{}' and tradeDate <= '{}' and secuCode = '{}' '''.format(ten_trade_date,tradeDate, secuCode)
            cur.execute(ten_count_sql)
            ten_count_info = cur.fetchone()
            ten_count = ten_count_info[0]
            ten_pe_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
   pe
    from
   ( 
        select * from `sy_ctxt_main_pbb` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pe  

    ) as A) as c on b.secuCode = c.secuCode and b.pe = c.pe '''.format(tradeDate, secuCode, tradeDate,ten_trade_date, secuCode)
            cur.execute(ten_pe_rn_sql)
            ten_pe_rn_info = cur.fetchone()
            if ten_pe_rn_info:
                ten_pe_rn = ten_pe_rn_info[0]
                if ten_count > 1:
                    peTenQuantile = (ten_pe_rn - 1) / (ten_count - 1)
                    peTenQuantile = round(peTenQuantile * 100, 4)
                else:
                    peTenQuantile = 0
            else:
                peTenQuantile = None
            ten_pb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
   pb
    from
   ( 
        select * from `sy_ctxt_main_pbb` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pb  

    ) as A) as c on b.secuCode = c.secuCode and b.pb = c.pb '''.format(tradeDate, secuCode, tradeDate,ten_trade_date, secuCode)
            cur.execute(ten_pb_rn_sql)
            ten_pb_rn_info = cur.fetchone()
            if ten_pb_rn_info:
                ten_pb_rn = ten_pb_rn_info[0]
                if ten_count > 1:
                    pbTenQuantile = (ten_pb_rn - 1) / (ten_count - 1)
                    pbTenQuantile = round(pbTenQuantile * 100, 4)
                else:
                    pbTenQuantile = 0
            else:
                pbTenQuantile = None
            ten_pbb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
   pbb
    from
   ( 
        select * from `sy_ctxt_main_pbb` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pbb  

    ) as A) as c on b.secuCode = c.secuCode and b.pbb = c.pbb '''.format(tradeDate, secuCode, tradeDate,ten_trade_date, secuCode)
            cur.execute(ten_pbb_rn_sql)
            ten_pbb_rn_info = cur.fetchone()
            if ten_pbb_rn_info:
                ten_pbb_rn = ten_pbb_rn_info[0]
                if ten_count > 1:
                    pbbTenQuantile = (ten_pbb_rn - 1) / (ten_count - 1)
                    pbbTenQuantile = round(pbbTenQuantile * 100, 4)
                else:
                    pbbTenQuantile = 0
            else:
                pbbTenQuantile = None
            insert_sql = '''insert into sy_ctxt_main_assess_value(chiName,innerCode,secuCode,tradeDate,totmktcap,
                            pe,peFiveQuantile,peTenQuantile,peQuantile,pb,pbFiveQuantile,pbTenQuantile,pbQuantile,
                            pbb,pbbFiveQuantile,pbbTenQuantile,pbbQuantile,isValid,createTime,dataState)
                            value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cur.execute(insert_sql, (chiName,innerCode,secuCode,tradeDate,totmktcap,
                            pe,peFiveQuantile,peTenQuantile,peQuantile,pb,pbFiveQuantile,pbTenQuantile,pbQuantile,
                            pbb,pbbFiveQuantile,pbbTenQuantile,pbbQuantile,isValid,createTime,dataState))
            mysql_conn.commit()

        print('插入表sy_ctxt_main_assess_value数据成功')
        print(i,innerCode,secuCode)

    except Exception as e:
        print(e)
        print('插入表数据失败：',i,innerCode,secuCode)
        break

mysql_conn2.close()
mysql_conn.close()
