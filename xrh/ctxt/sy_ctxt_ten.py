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


isValid = 1  # 是否有效，0：否，1：是
createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # sql操作时间
dataState = 1  # 数据状态:1->新增;2->更新;3->删除
modifyUpdateTime = createTime  # 最后修改时间


# # 获取A股公司所有公司内码代码
# main_innerCode_list = []
# main_innerCode_sql = '''SELECT DISTINCT innerCode from sy_ctxt_main_totmktcap'''
# mysql_conn2 = conn_mysql2()
# cur2 = mysql_conn2.cursor()
# cur2.execute(main_innerCode_sql)
# main_innerCode_infos = cur2.fetchall()
# for main_innerCode_info in main_innerCode_infos:
#     main_innerCode_list.append(main_innerCode_info[0])
# mysql_conn2.close()
# print(main_innerCode_list[0:10], len(main_innerCode_list))

mysql_conn = conn_mysql()
cur = mysql_conn.cursor()
mysql_conn2 = conn_mysql2()
cur2 = mysql_conn2.cursor()

five_trade_date_list = []
ten_trade_date_list = []
all_trade_date_list = []

inner_code_list = []
inner_code_sql = '''SELECT innerCode from sy_ctxt_main_assess_value_ten2 GROUP BY innerCode'''
cur.execute(inner_code_sql)
inner_code_infos = cur.fetchall()
for inner_code_info in inner_code_infos:
    inner_code_list.append(inner_code_info[0])

inner_code_sql2 = '''SELECT innerCode from sy_ctxt_main_assess_value_ten GROUP BY innerCode'''
cur.execute(inner_code_sql2)
inner_code_infos2 = cur.fetchall()
for inner_code_info2 in inner_code_infos2:
    inner_code_list.append(inner_code_info2[0])

find_trade_date_sql = '''SELECT a.innerCode,a.maxTradeDate,b.minTradeDate, DATEDIFF( STR_TO_DATE(a.tenTradeDate,'%Y%m%d'),STR_TO_DATE(b.minTradeDate,'%Y%m%d')) as ten,
DATEDIFF( STR_TO_DATE(a.fiveTradeDate,'%Y%m%d'),STR_TO_DATE(b.minTradeDate,'%Y%m%d')) as five
from(SELECT CONCAT((CONVERT(left(Max(tradeDate),4),SIGNED) - 10),right(Max(tradeDate),4)) as tenTradeDate,
CONCAT((CONVERT(left(Max(tradeDate),4),SIGNED) - 5),right(Max(tradeDate),4)) as fiveTradeDate,innerCode,Max(tradeDate) as maxTradeDate from sy_ctxt_main_pbb GROUP BY innerCode)as a INNER JOIN
(SELECT Min(tradeDate) as minTradeDate,innerCode from sy_ctxt_main_pbb GROUP BY innerCode) as b on a.innerCode = b.innerCode ORDER BY innerCode'''
cur.execute(find_trade_date_sql)
find_trade_date_infos = cur.fetchall()


for trade_date_info in find_trade_date_infos:
    innerCode = trade_date_info[0]
    if innerCode in inner_code_list:
        continue
    five_trade_date = trade_date_info[4]
    ten_trade_date = trade_date_info[3]
    if ten_trade_date > 0:
        all_trade_date_list.append(innerCode)
        continue
    if ten_trade_date <= 0 and five_trade_date > 0:
        ten_trade_date_list.append(innerCode)
        continue
    if five_trade_date <= 0:
        five_trade_date_list.append(innerCode)

print(ten_trade_date_list,len(ten_trade_date_list))

if len(ten_trade_date_list) == 0:
    print('There is no new data!')
    os._exit(0)

# 对每个交易日的pe,pb,pbb排序，并计算对应的分为点
i = 0
for innerCode in ten_trade_date_list:
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
            peTenQuantile = peQuantile
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
            pbTenQuantile = pbQuantile
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
            pbbTenQuantile = pbbQuantile
            
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
                if count > 1:
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
                if count > 1:
                    pbbFiveQuantile = (five_pbb_rn - 1) / (five_count - 1)
                    pbbFiveQuantile = round(pbbFiveQuantile * 100, 4)
                else:
                    pbbFiveQuantile = 0
            else:
                pbbFiveQuantile = None
            insert_sql = '''insert into sy_ctxt_main_assess_value_ten(chiName,innerCode,secuCode,tradeDate,totmktcap,
                            pe,peFiveQuantile,peTenQuantile,peQuantile,pb,pbFiveQuantile,pbTenQuantile,pbQuantile,
                            pbb,pbbFiveQuantile,pbbTenQuantile,pbbQuantile,isValid,createTime,dataState)
                            value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cur.execute(insert_sql, (chiName,innerCode,secuCode,tradeDate,totmktcap,
                            pe,peFiveQuantile,peTenQuantile,peQuantile,pb,pbFiveQuantile,pbTenQuantile,pbQuantile,
                            pbb,pbbFiveQuantile,pbbTenQuantile,pbbQuantile,isValid,createTime,dataState))
            mysql_conn.commit()

        print(secuCode)

    except Exception as e:
        print(e)
    print('插入表sy_ctxt_main_assess_value_ten数据成功')
    print(i, innerCode)

mysql_conn2.close()
mysql_conn.close()
