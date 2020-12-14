'''
文件名：cmbAGu.py
功能：招商项目A股需求处理
代码历史：20190522，徐荣华
'''

import pymysql
import datetime
import time
import json 
from DBUtils.PooledDB import PooledDB  
import addCopyAGuSourceData

# 读取连接数据库配置文件
load_config='/home/seeyii/increase_nlp/db_config.json'
with open(load_config, 'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)
isValid = 1  # 是否有效，0：否，1：是
createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # sql操作时间
dataState = 1  # 数据状态:1->新增;2->更新;3->删除

logging = True
mysql_log_name = "dwd_ms_data_log"
table_name1 = "dwd_ms_acomp_manager_all"
table_name2 = "dwd_ms_abcomp_manager_all"

def conn_mysql():
    mysql_conn = pymysql.connect(
        host='0.0.0.0',
        port=33006,
        user='root',
        passwd='123456',
        db='CMB',
        charset='utf8'
    )

    return mysql_conn

# 129sy_project_raw，写数据
pool2 = PooledDB(creator=pymysql, **config_local['local_sql_project_raw_pool'])
mysql_conn172 = pool2.connection()

# 129EI_BDP，写数据
# pool2_BDP = PooledDB(creator=pymysql, **config_local['local_sql_eibdp_pool'])
# mysql_conn_BDP = pool2_BDP.connection()

# 129csc_risk，写log
pool = PooledDB(creator=pymysql, **config_local['local_sql_csc_pool'])
db = pool.connection()
cursor = db.cursor()

# 取企业信息
def tmp_sq_comp_info():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()

    try:
        select_sql = '''
                        SELECT
            E.SecuCode,
            E.SecuAbbr,
            E.ChiName,
            E.CreditCode,
            E.RegCity,
            E.AreaChiName,
            E.ParentName,
            E.ContactCity,
            F.AreaChiName AS contactCityName,
            F.ParentName AS contactCityParentName,
            E.CompanyCval 
            FROM
                (
            SELECT
                D.AreaChiName,
                D.ParentName,
                C.* 
            FROM
                (
            SELECT
                B.SecuCode,
                B.SecuAbbr,
                A.CreditCode,
                A.RegCity,
                A.ContactCity,
                A.ChiName,
                ( CASE WHEN A.CompanyCval IN ( 1, 2, 3, 10, 11, 12 ) THEN "是" ELSE "否" END ) AS CompanyCval 
            FROM
                lc_instiarchive AS A,
                ( SELECT * FROM secumain WHERE SecuCategory = 1 ) AS B 
            WHERE
                A.CompanyCode = B.CompanyCode 
                ) AS C
                LEFT JOIN lc_areacode AS D ON C.RegCity = D.AreaCode 
                ) AS E
                LEFT JOIN lc_areacode AS F ON E.ContactCity = F.AreaCode 
            '''
        truncate_table_sql = '''truncate table tmp_sq_comp_info'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功企业信息表')

        cur.execute(select_sql)
        infos = cur.fetchall()
        insert_sql = '''insert into tmp_sq_comp_info (secuCode,secuAbbr,companyName,SocialUnifiedCreditCode,regCity,registeredCity,registeredProvinces,contactCity,operationCity,operationProvinces,guoqi)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur.executemany(val, infos)
        mysql_conn.commit()
        print('插入数据成功企业信息表')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()

# 招行A股主表处理
def tmp_sq_comp_manager_main_01():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # mysql_conn_BDP = pool2_BDP.connection()
    # curBDP = mysql_conn_BDP.cursor()

    select_sql = '''SELECT
    A.*, B.companyCode,
    B.companyName,
    B.secuCode,
    B.secuAbbr
FROM
    (
        SELECT
            innerCode,
            personalCode,
            replace(replace(CName,'（','('),'）',')') as CName,
            GROUP_CONCAT(beginDate) AS beginDate,
            GROUP_CONCAT(endDate) AS endDate,
            GROUP_CONCAT(dimReason) AS dimReason,
            GROUP_CONCAT(actdutyName) AS actdutyName
        FROM
            sq_comp_manager
        WHERE
            nowstatus = '2'
        AND postype IN ('1', '2', '3', '5')
        AND isValid = '1'
        GROUP BY
            innerCode ASC,
            personalCode,
            CName
    ) AS A,
    (
        SELECT
            innerCode,
            companyCode,
            companyName,
            secuCode,
            secuAbbr
        FROM
            sq_sk_basicinfo
        WHERE
            setype = '101'
        AND listStatus = '1'
    ) AS B
WHERE
    A.innerCode = B.innercode'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()

        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_01'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功1')

        # truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_01'''
        # curBDP.execute(truncate_table_sql)
        # mysql_conn_BDP.commit()
        # print('EI_BDP清空 tmp_sq_comp_manager_main_01数据成功')

        insert_sql = '''insert into tmp_sq_comp_manager_main_01(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功1')

        # curBDP.executemany(insert_sql, infos)
        # mysql_conn_BDP.commit()
        # print('插入tmp_sq_comp_manager_main_01数据成功')


    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# A股高管关联5%以上股东
def tmp_sq_comp_manager_main_02():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # todo enddate的日期问王浩是否要根据年份改
    select_sql = '''SELECT
    A.*, B.actdutyName01,
    B.innerCode AS innerCode01,
    B.shHolderName
FROM
    tmp_sq_comp_manager_main_01 AS A
LEFT OUTER JOIN (
    SELECT DISTINCT
        innerCode,
        shHolderCode,
        replace(replace(shHolderName,'（','('),'）',')' )as shHolderName,
        "5%以上股东" AS actdutyName01
    FROM
        sq_sk_shareholder
    WHERE
        HOLDERRTO >= 5
    AND shHolderType = '5'
    AND endDate = '20191231'
) AS B ON A.innerCode = B.innerCode
AND A.CName = B.shHolderName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_02'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功2')
        insert_sql = '''insert into tmp_sq_comp_manager_main_02(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,actdutyName01,innerCode01,shHolderName) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功2')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# A股5 % 以上全部股东
def tmp_sq_comp_manager_main_03():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # todo enddate的日期问王浩是否要根据年份改
    select_sql = '''SELECT
    A.*, B.actdutyName01,
    B.innerCode AS innerCode01,
    B.shHolderCode,
    B.shHolderName
FROM
    (
        SELECT DISTINCT
            innerCode,
            companyCode,
            companyName,
            secuCode,
            secuAbbr
        FROM
            tmp_sq_comp_manager_main_01
    ) AS A,
    (
        SELECT DISTINCT
            innerCode,
            shHolderCode,
            replace(replace(shHolderName,'（','('),'）',')' )as shHolderName,
            "5%以上股东" AS actdutyName01
        FROM
            sq_sk_shareholder
        WHERE
            HOLDERRTO >= 5
        AND shHolderType = '5'
        AND endDate = '20191231'
    ) AS B
WHERE
    A.innerCode = B.innerCode'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_03'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功3')
        insert_sql = '''insert into tmp_sq_comp_manager_main_03(innerCode,companyCode,companyName,secuCode,secuAbbr,
            actdutyName01,innerCode01,shHolderCode,shHolderName) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功3')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# A股5 % 以上全部股东且非管理层
def tmp_sq_comp_manager_main_03_01():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT
        *
    FROM
        tmp_sq_comp_manager_main_03
    WHERE
        CONCAT(innerCode, shHolderName) NOT IN (
            SELECT
                CONCAT(innerCode, shHolderName)
            FROM
                tmp_sq_comp_manager_main_02
            WHERE
                actdutyName01 IS NOT NULL
        )'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_03_01'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功3_1')
        insert_sql = '''insert into tmp_sq_comp_manager_main_03_01(innerCode,companyCode,companyName,secuCode,secuAbbr,
            actdutyName01,innerCode01,shHolderCode,shHolderName) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功3_1')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 取万得实控人数据
# TODO 新数据需要王浩提供，否则就直接用当前
def wd_a_controllers_01():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''select distinct secucode,company_name,company_sname,
       substring_index(substring_index(a.controller,',',b.help_topic_id+1),',',-1) as controller   
    from WD_A_controllers as a  join  mysql.help_topic as b  
    on b.help_topic_id < (length(a.controller) - length(replace(a.controller,',',''))+1)'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table WD_A_controllers_01'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功wd_01')
        insert_sql = '''insert into WD_A_controllers_01(secucode,company_name,company_sname,controller) value 
            (%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功wd_01')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 取万得实控人数据
def tmp_sq_comp_manager_main_04():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT
            C.*, D.actdutyName02,
            D.CompanyCode AS CompanyCode02,
            D.controllerCode,
            D.controllerName
        FROM
            (
                SELECT DISTINCT
                    innerCode,
                    companyCode,
                    companyName,
                    secuCode,
                    secuAbbr
                FROM
                    tmp_sq_comp_manager_main_01
            ) AS C,
            (
                SELECT
                    A.*, B.innerCode,
                    B.secuCode,
                    secuAbbr,
                    B.ChiName
                FROM
                    (
                        SELECT DISTINCT
                            secucode as CompanyCode,
                            '' as controllerCode,
                            replace(replace(controller,'（','('),'）',')' )as controllerName,
                            "实际控制人" AS actdutyName02
                        FROM
                            WD_A_controllers_01                     
                    ) AS A
                LEFT JOIN 
           (select * from  secumain where SecuCategory='1'  and ListedState='1') AS B 
           ON A.CompanyCode+0= B.secuCode+0 
            ) AS D
        WHERE
            C.secuCode = D.secuCode'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_04'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功4')
        insert_sql = '''insert into tmp_sq_comp_manager_main_04(innerCode,companyCode,companyName,secuCode,secuAbbr,
            actdutyName02,CompanyCode02,controllerCode,controllerName) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功4')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 高管主表关联实控人信息
def tmp_sq_comp_manager_main_05():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT
                A.*, B.actdutyName02,
                B.innerCode AS innerCode02,
                B.secuCode AS secuCode02,
                B.controllercode,
                B.controllerName
            FROM
                tmp_sq_comp_manager_main_02 AS A
            LEFT OUTER JOIN tmp_sq_comp_manager_main_04 AS B ON A.innerCode = B.innerCode
            AND A.CName = B.controllerName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_05'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功5')
        insert_sql = '''insert into tmp_sq_comp_manager_main_05(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,actdutyName01,innerCode01,shHolderName,
            actdutyName02,innerCode02,secuCode02,controllercode,controllerName) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功5')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 非管理层且实控人
def tmp_sq_comp_manager_main_06():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT * FROM
                    tmp_sq_comp_manager_main_04
                WHERE
                    CONCAT(innerCode, controllerName) NOT IN (
                        SELECT
                            CONCAT(innerCode, controllerName)
                        FROM
                            tmp_sq_comp_manager_main_05
                        WHERE
                            actdutyName02 IS NOT NULL)'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_06'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功6')
        insert_sql = '''insert into tmp_sq_comp_manager_main_06(innerCode,companyCode,companyName,secuCode,secuAbbr,
            actdutyName02,CompanyCode02,controllerCode,controllerName) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功6')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 非管理层且实控人且5%以上股东
def tmp_sq_comp_manager_main_06_03_01():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''select a.innerCode, a.companyCode, a.companyName, a.secuCode, a.secuAbbr,a.actdutyName02,
              b.actdutyName01, b.shHolderCode, b.shHolderName  from 
              tmp_sq_comp_manager_main_06 as a ,tmp_sq_comp_manager_main_03_01 as b WHERE a.innerCode=b.innerCode and b.shHolderName=a.controllerName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_06_03_01'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功06_03_01')
        insert_sql = '''insert into tmp_sq_comp_manager_main_06_03_01(innerCode,companyCode,companyName,secuCode,secuAbbr,
            actdutyName02,actdutyName01,shHolderCode,shHolderName) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功06_03_01')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 插入非管理层的5%股东和实控人
def tmp_sq_comp_manager_main_07():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # 插入管理层数据
    select_sql = '''SELECT * FROM tmp_sq_comp_manager_main_05'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_07'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功7')
        insert_sql = '''insert into tmp_sq_comp_manager_main_07(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,actdutyName01,innerCode01,shHolderName,
            actdutyName02,innerCode02,secuCode02,controllercode,controllerName) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功7')

        # 插入非管理层5%股东和控制人
        insert_sql2 = '''INSERT INTO tmp_sq_comp_manager_main_07 (
                innerCode, 
                personalCode,
                CName,
                beginDate,
                endDate,
                dimReason,
                actdutyName,
                companyCode,
                companyName,
                secuCode,
                secuAbbr,
                actdutyName01,
                innerCode01,
                shHolderName,
                actdutyName02,
                innerCode02,
                secuCode02,
                controllercode,
                controllerName
            ) SELECT
                innerCode,
                shHolderCode,
                shHolderName,
                '',
                '',
                '',
                actdutyName01,
                companyCode,
                companyName,
                secuCode,
                secuAbbr,
                '',
                '',
                '',
                actdutyName02,
                '',
                '',
                '',
                ''
            FROM
                tmp_sq_comp_manager_main_06_03_01'''
        cur.execute(insert_sql2)
        mysql_conn.commit()
        print('插入非管理层5%股东和控制人数据成功')

        # 插入非管理层的5%股东数据
        insert_sql2 = '''INSERT INTO tmp_sq_comp_manager_main_07 (
            innerCode,
            personalCode,
            CName,
            beginDate,
            endDate,
            dimReason,
            actdutyName,
            companyCode,
            companyName,
            secuCode,
            secuAbbr,
            actdutyName01,
            innerCode01,
            shHolderName,
            actdutyName02,
            innerCode02,
            secuCode02,
            controllercode,
            controllerName
        ) SELECT
            C.innerCode,
            C.shHolderCode,
            C.shHolderName,
            '',
            '',
            '',
            C.actdutyName01,
            C.companyCode,
            C.companyName,
            C.secuCode,
            C.secuAbbr,
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            ''
        FROM
            (
            SELECT A.* FROM tmp_sq_comp_manager_main_03_01 as A LEFT JOIN tmp_sq_comp_manager_main_06_03_01 as B ON A.innerCode=B.innerCode and A.shHolderName=B.shHolderName WHERE B.innerCode is NULL and B.shHolderName is NULL
            ) as C'''
        cur.execute(insert_sql2)
        mysql_conn.commit()
        print('插入非管理层的5%股东数据成功')
        # 插入非管理层的实控人数据
        insert_sql3 = '''INSERT INTO tmp_sq_comp_manager_main_07 (
                    innerCode,
                    personalCode,
                    CName,
                    beginDate,
                    endDate,
                    dimReason,
                    actdutyName,
                    companyCode,
                    companyName,
                    secuCode,
                    secuAbbr,
                    actdutyName01,
                    innerCode01,
                    shHolderName,
                    actdutyName02,
                    innerCode02,
                    secuCode02,
                    controllercode,
                    controllerName
                ) SELECT
                    C.innerCode,
                    C.controllercode,
                    C.controllerName,
                    '',
                    '',
                    '',
                    C.actdutyName02,
                    C.companyCode,
                    C.companyName,
                    C.secuCode,
                    C.secuAbbr,
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    C.controllercode,
                    C.controllerName
                FROM
                    (
                    SELECT A.* FROM tmp_sq_comp_manager_main_06 as A LEFT JOIN tmp_sq_comp_manager_main_06_03_01 as B ON A.innerCode=B.innerCode and A.controllerName=B.shHolderName WHERE B.innerCode is NULL and B.shHolderName is NULL
                    ) as C'''
        cur.execute(insert_sql3)
        mysql_conn.commit()
        print('插入非管理层的实控人数据成功')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 合并5%股东
def tmp_sq_comp_manager_main_08():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT
    *, (
        CASE
        WHEN actdutyName01 IS NOT NULL
        AND beginDate <> '' THEN
            concat(
                actdutyName,
                ',',
                actdutyName01
            )
        ELSE
            actdutyName
        END
    ) AS actdutyName03
FROM
    tmp_sq_comp_manager_main_07'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_08'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功8')
        insert_sql = '''insert into tmp_sq_comp_manager_main_08(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,actdutyName01,innerCode01,shHolderName,
            actdutyName02,innerCode02,secuCode02,controllercode,controllerName,actdutyName03) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功8')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 合并实控人
def tmp_sq_comp_manager_main_09():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT
    *, (
        CASE
        WHEN actdutyName02 IS NOT NULL
        AND beginDate <> '' THEN
            concat(
                actdutyName03,
                ',',
                actdutyName02
            )
        ELSE
            actdutyName03
        END
    ) AS actdutyName04
FROM
    tmp_sq_comp_manager_main_08'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_09'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功9')
        insert_sql = '''insert into tmp_sq_comp_manager_main_09(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,actdutyName01,innerCode01,shHolderName,
            actdutyName02,innerCode02,secuCode02,controllercode,controllerName,actdutyName03,actdutyName04) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功9')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 合并实控人和5%股东
def tmp_sq_comp_manager_main_09_01():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT
    *, (
        CASE
        WHEN actdutyName02 IS NOT NULL and actdutyName02 != '' AND beginDate = ''
        THEN
            concat(
                actdutyName04,
                ',',
                actdutyName02
            )
        ELSE
            actdutyName04
        END
    ) AS actdutyName05
FROM
    tmp_sq_comp_manager_main_09'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_09_01'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功9_01')
        insert_sql = '''insert into tmp_sq_comp_manager_main_09_01(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,actdutyName01,innerCode01,shHolderName,
            actdutyName02,innerCode02,secuCode02,controllercode,controllerName,actdutyName03,actdutyName04,actdutyName05) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功9_01')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()


# 对表tmp_sq_comp_manager_main_09_01瘦身
def tmp_sq_comp_manager_main_10():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT
    innerCode,
    personalCode,
    CName,
    beginDate,
    endDate,
    dimReason,
    actdutyName05 AS actdutyName,
    companyCode,
    companyName,
    secuCode,
    secuAbbr
FROM
    tmp_sq_comp_manager_main_09_01'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_10'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功10')
        insert_sql = '''insert into tmp_sq_comp_manager_main_10(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功10')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 主表和secumain表关联获取secumain表的companyCode
def tmp_sq_comp_manager_main_10_01():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT
    A.*, B.companyCode AS LC_companyCode
FROM
    tmp_sq_comp_manager_main_10 AS A
LEFT JOIN (
    SELECT
        *
    FROM
        secumain
    WHERE
        SecuCategory = '1'
    AND ListedState = '1'
) AS B ON A.SecuCode = B.SecuCode'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_10_01'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功10_01')
        insert_sql = '''insert into tmp_sq_comp_manager_main_10_01(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功10_01')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()


# 找出重复数据进行处理
def tmp_sq_comp_manager_main_11_01():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''select * from tmp_sq_comp_manager_main_10_01 where CONCAT(innerCode,CName) in 
(
select CONCAT(innerCode,CName) from 
(
select innerCode,CName,count(*) as num  from tmp_sq_comp_manager_main_10_01 group by innerCode,CName having num>1
) AS A 
) order by innerCode,CName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_11_01'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功11_01')
        insert_sql = '''insert into tmp_sq_comp_manager_main_11_01(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功11_01')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()


# 找出重复数据进行处理
def tmp_sq_comp_manager_main_11_02():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''select innerCode,CName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,
                 GROUP_CONCAT(personalCode) as personalCode,               
                 GROUP_CONCAT(beginDate) as beginDate,
                 GROUP_CONCAT(endDate) as endDate,
                 GROUP_CONCAT(dimReason) as dimReason,
                 GROUP_CONCAT(actdutyName) as actdutyName
               from tmp_sq_comp_manager_main_11_01 
               group by 
               innerCode,CName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,personalCode'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_11_02'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功11_02')
        insert_sql = '''insert into tmp_sq_comp_manager_main_11_02(innerCode,CName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,
        personalCode,beginDate,endDate,dimReason,actdutyName) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功11_02')
        update_sql1 = "update tmp_sq_comp_manager_main_11_02 set beginDate=REPLACE(beginDate,',','') where beginDate=','"
        cur.execute(update_sql1)
        mysql_conn.commit()
        update_sql2 = "update tmp_sq_comp_manager_main_11_02 set beginDate=REPLACE(beginDate,',',';')"
        cur.execute(update_sql2)
        mysql_conn.commit()
        update_sql3 = "update tmp_sq_comp_manager_main_11_02 set endDate=REPLACE(endDate,',','') where endDate=','"
        cur.execute(update_sql3)
        mysql_conn.commit()
        update_sql4 = "update tmp_sq_comp_manager_main_11_02 set endDate=REPLACE(endDate,',',';')"
        cur.execute(update_sql4)
        mysql_conn.commit()
        update_sql5 = "update tmp_sq_comp_manager_main_11_02 set dimReason=REPLACE(dimReason,',','')"
        cur.execute(update_sql5)
        mysql_conn.commit()
        update_sql6 = "update tmp_sq_comp_manager_main_11_02 set personalCode=substring_index(personalCode,',',1)"
        cur.execute(update_sql6)
        mysql_conn.commit()
        print('更新数据成功')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()


# 主表和secumain表关联获取secumain表的companyCode
def tmp_sq_comp_manager_main_11():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT
    A.*, B.companyCode AS LC_companyCode
FROM
    tmp_sq_comp_manager_main_10 AS A
LEFT JOIN (
    SELECT
        *
    FROM
        secumain
    WHERE
        SecuCategory = '1'
    AND ListedState = '1'
) AS B ON A.SecuCode = B.SecuCode'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_11'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功11')
        insert_sql = '''insert into tmp_sq_comp_manager_main_11(innerCode,personalCode,CName,beginDate,endDate,
            dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode) value 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功11')
        # 删除11表重复数据
        delete_sql = """
delete from tmp_sq_comp_manager_main_11  
         where CONCAT(innerCode,CName) in 
         (select CONCAT(innerCode,CName) from tmp_sq_comp_manager_main_11_02)"""
        cur.execute(delete_sql)
        mysql_conn.commit()
        # 插入清洗后的数据
        insert_sql2 = """insert into tmp_sq_comp_manager_main_11 
(
select 
       innerCode,
       personalCode,
       CName,
       beginDate,
       endDate,
       dimReason,

       actdutyName,
       companyCode,
       companyName,
       secuCode,
       secuAbbr,
       LC_companyCode
       from tmp_sq_comp_manager_main_11_02
)"""
        cur.execute(insert_sql2)
        mysql_conn.commit()
        update_sql3 = "update tmp_sq_comp_manager_main_11 set beginDate=REPLACE(beginDate,',',';')"
        cur.execute(update_sql3)
        mysql_conn.commit()
        update_sql4 = "update tmp_sq_comp_manager_main_11 set endDate=REPLACE(endDate,',',';')"
        cur.execute(update_sql4)
        mysql_conn.commit()
        update_sql5 = "update tmp_sq_comp_manager_main_11 set beginDate=REPLACE(beginDate,'19000101','--')"
        cur.execute(update_sql5)
        mysql_conn.commit()
        update_sql6 = "update tmp_sq_comp_manager_main_11 set endDate=REPLACE(endDate,'19000101','--')"
        cur.execute(update_sql6)
        mysql_conn.commit()
        print('更新数据成功')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()


# 获取上一年个人的报酬信息
def one_year_annualRewArd_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # todo 问王浩EndDate的日期是否需要根据时间而改变
    select_sql1 = '''SELECT A.*,B.* from
(SELECT * from tmp_sq_comp_manager_main_11) as A left JOIN
(select CompanyCode,date(EndDate) as EndDate ,SequenceNumber,LeaderName,sum(AnnualReward) from lc_executivesholdings where date(EndDate) = STR_TO_DATE('20191231','%Y%m%d') group by CompanyCode, LeaderName) as B on A.LC_companyCode = B.CompanyCode and A.CName = B.LeaderName'''
    try:
        cur.execute(select_sql1)
        infos = cur.fetchall()
        i = 0
        L = []
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_12'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功12')
        for info in infos:
            i += 1
            list_AnnualRewArd = []
            LC_companyCode = info[11]
            AnnualRewArd = info[-1]
            # 当年无个人报酬信息，用企业平均报酬代替
            # todo 问王浩EndDate的日期是否需要根据时间而改变
            if not AnnualRewArd:
                select_sql2 = '''select CompanyCode,date(EndDate) as EndDate,
                      TotalYearPay,
                      (case when  High3Directors is null then 0 else High3Directors end ) as High3Directors,
                      (case when  High3Managers is null then 0 else High3Managers end ) as High3Managers,
                      (case when  NumPayManagers is null then 0 else NumPayManagers end ) as NumPayManagers,
                      round (( 
                 case when NumPayManagers>6 and TotalYearPay>High3Directors+High3Managers
                       then (TotalYearPay-High3Directors-High3Managers)/(NumPayManagers-6) 
                 when NumPayManagers=0 then NumPayManagers
                       else TotalYearPay/NumPayManagers end
                      ),2) as AvgYearPay
                      from lc_rewardstat
                      where date(EndDate) = STR_TO_DATE('20191231','%Y%m%d') and CompanyCode = "{}"
       '''.format(LC_companyCode)
                if cur.execute(select_sql2):
                    infos = cur.fetchone()
                    AnnualRewArd = infos[-1]
                else:
                    AnnualRewArd = None  # 企业上年报酬信息也缺失，用空值代替
            list_AnnualRewArd.append(AnnualRewArd)
            info = info[0:12] + tuple(list_AnnualRewArd)
            L.append(info)
            insert_sql = '''insert into tmp_sq_comp_manager_main_12 (innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,oneYearAnnualRewArd)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            if i % 100 == 0:  # 批量插入，每次一百条
                cur.executemany(insert_sql, L)
                mysql_conn.commit()
                L = []
        cur.executemany(insert_sql, L)
        mysql_conn.commit()
        print('插入数据成功12')


    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()



# 获取上一年企业的报酬信息
def one_year_comp_annualRewArd_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # todo 问王浩EndDate的日期是否需要根据时间而改变
    select_sql1 = '''SELECT A.*,B.* from
(SELECT * from tmp_sq_comp_manager_main_12) as A left JOIN
(select CompanyCode,date(EndDate) as EndDate,
                      TotalYearPay,NumPayManagers,
                      round(TotalYearPay/NumPayManagers,2) as AvgYearPay
                      from lc_rewardstat 
                      where date(EndDate) = STR_TO_DATE('20191231','%Y%m%d') GROUP BY CompanyCode) as B on A.LC_companyCode = B.CompanyCode'''
    try:
        cur.execute(select_sql1)
        infos = cur.fetchall()
        i = 0
        L = []
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_13'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功13')
        for info in infos:
            i += 1
            list_AnnualRewArd = []
            AvgYearPay = info[-1]
            list_AnnualRewArd.append(AvgYearPay)
            info = info[0:13] + tuple(list_AnnualRewArd)
            L.append(info)
            insert_sql = '''insert into tmp_sq_comp_manager_main_13 (innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,oneYearAnnualRewArd,oneYearCompAnnualRewArd)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            if i % 100 == 0:  # 批量插入，每次一百条
                cur.executemany(insert_sql, L)
                mysql_conn.commit()
                L = []
        cur.executemany(insert_sql, L)
        mysql_conn.commit()
        print('插入数据成功13')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()


# 获取个人五年平均报酬信息,个人数据缺失用企业五年平均报酬代替
def five_year_avg_annualRewArd_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # todo 问王浩EndDate的日期是否需要根据时间而改变(截取5年)
    select_sql1 = '''SELECT A.*,B.* from
(SELECT * from tmp_sq_comp_manager_main_13) as A left JOIN
(select CompanyCode,date(EndDate) as EndDate ,SequenceNumber,LeaderName,AVG (AnnualReward) from lc_executivesholdings where date(EndDate) in (STR_TO_DATE('20191231','%Y%m%d'),STR_TO_DATE('20181231','%Y%m%d'),
STR_TO_DATE('20171231','%Y%m%d'),STR_TO_DATE('20161231','%Y%m%d'),STR_TO_DATE('20151231','%Y%m%d')) GROUP BY CompanyCode, LeaderName) as B on A.LC_companyCode = B.CompanyCode and A.CName = B.LeaderName'''
    try:
        cur.execute(select_sql1)
        infos = cur.fetchall()  # 获取五年个人平均报酬信息
        i = 0
        L = []
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_14'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功')
        for info in infos:
            i += 1
            list_AnnualRewArd = []
            LC_companyCode = info[11]
            AnnualRewArd = info[-1]
            # todo 问王浩EndDate的日期是否需要根据时间而改变(截取5年)
            if not AnnualRewArd:  # 当个人五年报酬都缺失，用企业五年平均报酬代替
                select_sql2 = '''select CompanyCode,date(EndDate) as EndDate,
                      TotalYearPay,
                      (case when  High3Directors is null then 0 else High3Directors end ) as High3Directors,
                      (case when  High3Managers is null then 0 else High3Managers end ) as High3Managers,
                      (case when  NumPayManagers is null then 0 else NumPayManagers end ) as NumPayManagers,
                      AVG (round ((
                 case when NumPayManagers>6 and TotalYearPay>High3Directors+High3Managers
                       then (TotalYearPay-High3Directors-High3Managers)/(NumPayManagers-6)
                 when NumPayManagers=0 then NumPayManagers
                       else TotalYearPay/NumPayManagers end
                      ),2)) as AvgYearPay
                      from lc_rewardstat
                      where date(EndDate) in (STR_TO_DATE('20191231','%Y%m%d'),STR_TO_DATE('20181231','%Y%m%d'),
STR_TO_DATE('20171231','%Y%m%d'),STR_TO_DATE('20161231','%Y%m%d'),STR_TO_DATE('20151231','%Y%m%d')) and CompanyCode = "{}"
       '''.format(LC_companyCode)
                if cur.execute(select_sql2):
                    infos = cur.fetchone()
                    AnnualRewArd = infos[-1]
                else:
                    AnnualRewArd = None  # 企业五年报酬信息都缺失用空值代替
            list_AnnualRewArd.append(AnnualRewArd)
            info = info[0:14] + tuple(list_AnnualRewArd)
            L.append(info)
            insert_sql = '''insert into tmp_sq_comp_manager_main_14 (innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearAvgAnnualRewArd)
                                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            if i % 100 == 0:  # 批量插入，每次一百条
                cur.executemany(insert_sql, L)
                mysql_conn.commit()
                L = []
        cur.executemany(insert_sql, L)
        mysql_conn.commit()
        print('插入数据成功')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 获取个人五年报酬总和信息
def five_year_sum_annualRewArd_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # todo 问王浩EndDate的日期是否需要根据时间而改变(截取5年)
    select_sql1 = '''SELECT A.*,B.* from
(SELECT * from tmp_sq_comp_manager_main_14) as A left JOIN
(select CompanyCode,date(EndDate) as EndDate ,SequenceNumber,LeaderName,sum(AnnualReward) from lc_executivesholdings where date(EndDate) in (STR_TO_DATE('20191231','%Y%m%d'),STR_TO_DATE('20181231','%Y%m%d'),
STR_TO_DATE('20171231','%Y%m%d'),STR_TO_DATE('20161231','%Y%m%d'),STR_TO_DATE('20151231','%Y%m%d')) GROUP BY CompanyCode, LeaderName) as B on A.LC_companyCode = B.CompanyCode and A.CName = B.LeaderName'''
    try:
        cur.execute(select_sql1)
        infos = cur.fetchall()  # 获取五年个人平均报酬信息
        i = 0
        L = []
        truncate_table_sql = '''truncate table tmp_five_year_sum_annualRewArd_message'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功')
        for info in infos:
            i += 1
            list_AnnualRewArd = []
            AnnualRewArd = info[-1]
            if AnnualRewArd:
                list_AnnualRewArd.append(AnnualRewArd)
                info = info[0:3] + tuple(list_AnnualRewArd)
                L.append(info)
            insert_sql = '''insert into tmp_five_year_sum_annualRewArd_message(innerCode,personalCode,CName,fiveYearSumAnnualRewArd)
                                        value (%s,%s,%s,%s)'''
            if i % 100 == 0:  # 批量插入，每次一百条
                cur.executemany(insert_sql, L)
                mysql_conn.commit()
                L = []
        cur.executemany(insert_sql, L)
        mysql_conn.commit()
        print('插入数据成功')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 获取上一年个人分红信息
def one_year_dividents_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # todo 问王浩EndDate, diviYear的日期是否需要根据时间而改变
    select_sql1 = '''select C.*,D.*,(C.holderamt*(case when D.aftTaxCashDvcny is null then 0 ELSE D.aftTaxCashDvcny END)) as total_money from 
(
select innerCode as innerCode1 ,shHolderCode,shHolderName,endDate,holderamt,holderRTO,pfHolderamt from sq_sk_shareholder  
                              where shHolderType ='5' and 
                                    enddate = '20191231' 
) as C
,
(
select A.*,B.companyCode,B.companyName,B.secuCode,B.secuAbbr from
(
select innerCode,diviYear, sum(aftTaxCashDvcny/10)as aftTaxCashDvcny
                    -- ,shCapBaseQTY
                     from sq_sk_dividents  
                     where PROJECTTYPE='1' and diviYear='2019' GROUP BY innerCode
                     order by secuCode desc  
) as A
,
(
  select distinct innerCode,companyCode,companyName,secuCode,secuAbbr from tmp_sq_comp_manager_main_11 
 -- select innerCode,companyName,secuAbbr from sq_sk_basicinfo where setype='101' and listStatus='1'

) as B
where  A.innerCode=B.innerCode order by secuAbbr desc 
) as D
where  C.innerCode1=D.innerCode and C.EndDate=(case WHEN D.diviYear = '2019' THEN '20191231' END) GROUP BY innerCode1,shHolderName'''
    try:
        cur.execute(select_sql1)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_one_year_dividents_message'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功')
        for info in infos:  
            innerCode = info[0] 
            CName = info[2]
            total_money = info[-1]
            companyName = info[-4]
            insert_sql = '''insert into tmp_one_year_dividents_message(innerCode,CName,companyName,total_money) 
            value("{}","{}","{}","{}") '''.format(innerCode, CName, companyName, total_money)
            cur.execute(insert_sql)
            mysql_conn.commit()
        print('插入数据成功')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 获取五年个人分红总和信息
def five_year_dividents_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # todo *问王浩EndDate的日期是否需要根据时间而改变(截取5年)
    select_sql1 = '''select C.*,D.*,sum(C.holderamt*(case when D.aftTaxCashDvcny is null then 0 ELSE D.aftTaxCashDvcny END)) as total_money from 
(
select innerCode as innerCode1 ,shHolderCode,shHolderName,endDate,holderamt,holderRTO,pfHolderamt from sq_sk_shareholder  
                              where shHolderType ='5' and 
                                    endDate in ('20191231','20181231','20171231','20161231','20151231') GROUP BY innerCode, endDate,shHolderName 
) as C
,
(
select A.*,B.companyCode,B.companyName,B.secuCode,B.secuAbbr,B.CName from

(
select innerCode,diviYear, sum(aftTaxCashDvcny/10)as aftTaxCashDvcny ,(case WHEN diviYear = '2019' THEN '20191231' WHEN diviYear = '2018' THEN '20181231' 
WHEN diviYear = '2017' THEN '20171231' WHEN diviYear = '2016' THEN '20161231' WHEN diviYear = '2015' THEN '20151231'  END ) as diviDate
                    -- ,shCapBaseQTY
                     from sq_sk_dividents  
                     where PROJECTTYPE='1' and diviYear in ('2019','2018','2017','2016','2015') GROUP BY InnerCode, diviYear
                     order by secuCode desc
) as A
,
(
  select innerCode,companyCode,companyName,secuCode,secuAbbr,CName from tmp_sq_comp_manager_main_11
 -- select innerCode,companyName,secuAbbr from sq_sk_basicinfo where setype='101' and listStatus='1'

) as B
where  A.innerCode=B.innerCode GROUP BY innerCode,CName,diviYear order by secuAbbr desc 
) as D
where  C.innerCode1=D.innerCode and C.endDate=D.diviDate and C.shHolderName=D.CName GROUP BY innerCode1,shHolderName'''
    try:
        cur.execute(select_sql1)
        infos = cur.fetchall()
        i = 0
        L = []
        truncate_table_sql = '''truncate table tmp_five_year_dividents_message'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功')
        for info in infos:
            i += 1
            list_info = []
            innerCode = info[0]
            CName = info[2]
            total_money = info[-1]
            companyName = info[-4]
            list_info.append(innerCode)
            list_info.append(CName)
            list_info.append(companyName)
            list_info.append(total_money)
            info = tuple(list_info)
            L.append(info)
            insert_sql = '''insert into tmp_five_year_dividents_message(innerCode,CName,companyName,total_money) 
            value(%s,%s,%s,%s) '''
            if i % 100 == 0:  # 批量插入，每次一百条
                cur.executemany(insert_sql, L)
                mysql_conn.commit()
                L = []
        cur.executemany(insert_sql, L)
        mysql_conn.commit()
        print('插入数据成功')

    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 主表和上一年分红收入表合并
def comp_one_year_dividents_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql1 = '''SELECT A.*,B.* from
    (SELECT * from tmp_sq_comp_manager_main_14) as A left JOIN
    (select * from tmp_one_year_dividents_message) as B on A.innerCode = B.innerCode and A.CName = B.CName'''
    try:
        cur.execute(select_sql1)
        infos = cur.fetchall()
        i = 0
        L = []
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_15'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功15')
        for info in infos:
            i += 1
            list_total_money = []
            last_year_total_money = info[-1]
            list_total_money.append(last_year_total_money)
            info = info[0:15] + tuple(list_total_money)
            L.append(info)
            insert_sql = '''insert into tmp_sq_comp_manager_main_15(innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearAvgAnnualRewArd,lastYearTotalMoney)
                                                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            if i % 100 == 0:  # 批量插入，每次一百条
                cur.executemany(insert_sql, L)
                mysql_conn.commit()
                L = []
        cur.executemany(insert_sql, L)
        mysql_conn.commit()
        print('插入数据成功15')


    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()

# 主表和五年分红收入表合并
def comp_five_year_dividents_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql1 = '''SELECT A.*,B.* from
    (SELECT * from tmp_sq_comp_manager_main_15) as A left JOIN
    (select * from tmp_five_year_dividents_message) as B on A.innerCode = B.innerCode and A.CName = B.CName'''
    try:
        cur.execute(select_sql1)
        infos = cur.fetchall()
        i = 0
        L = []
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_16'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功16')
        for info in infos:
            i += 1
            list_total_money = []
            five_year_sum_total_money = info[-1]
            list_total_money.append(five_year_sum_total_money)
            info = info[0:16] + tuple(list_total_money)
            L.append(info)
            insert_sql = '''insert into tmp_sq_comp_manager_main_16 (innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearAvgAnnualRewArd,lastYearTotalMoney,fiveYearSumTotalMomey)
                                                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            if i % 100 == 0:  # 批量插入，每次一百条
                cur.executemany(insert_sql, L)
                mysql_conn.commit()
                L = []
        cur.executemany(insert_sql, L)
        mysql_conn.commit()
        print('插入数据成功16')


    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()


def holder_(tradeDate):
    # 截至目前持股数对应市值,添加了新字段 holderMoneySource，激励股权在这里体现
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    try:
        # 股票历史日交易表中最新日期
        # new_date_sql = '''select tradeDate from sq_qt_skdailyprice group by tradeDate order by tradeDate DESC LIMIT 1'''
        # cur.execute(new_date_sql)
        # new_date_infos = cur.fetchall()
        # tradeDate = new_date_infos[0][0]
        print('holder_()---------', tradeDate, '------------')

        # 获取当前日期上一年的年报时间
        timeStamp = int(time.time()) - (1546185600 - 1514736000)
        timeArray = time.localtime(timeStamp)
        enddate = time.strftime("%Y1231", timeArray)

        chigu_sql = '''       
        select D.*,E.tradeDate,E.tClose, 
               round(D.holderamt*E.tClose,2) as holderMoney,E.totmktcap from 
            (
            SELECT B.* from (select innerCode,shHolderCode,shHolderName,MAX(endDate) as endDate from sq_sk_shareholder  
            where shHolderType ='5' GROUP BY innerCode,shHolderName) as A ,
            (select innerCode,shHolderCode, shHolderName,endDate ,holderamt,holderRTO,pfHolderamt
            from sq_sk_shareholder where shHolderType ='5') as B 
            WHERE A.innerCode=B.innerCode and  A.shHolderCode=B.shHolderCode and A.shHolderName=B.shHolderName and A.endDate=B.endDate
             ) as D                          
            ,
            (
            select * from 
            (
            select A.*,B.innerCode,B.companyName,B.secuCode,B.secuAbbr from
            (
            select tradeDate,companyCode,sName,lClose,tClose,totmktcap from sq_qt_skdailyprice 
                                         where tradeDate='{}' and isValid='1' and 
                                               (exchange='001002' or   exchange='001003') 
            ) as A
            left join 
            (
            select distinct innerCode,companyCode,companyName,secuCode,secuAbbr from tmp_sq_comp_manager_main_11 
            ) as B
            on A.companyCode=B.companyCode 
            ) as C
            where C.innerCode is not null
            ) as E
            where  D.innerCode=E.innerCode 
            '''.format(tradeDate)
        cur.execute(chigu_sql)
        holder_chigu_infos = cur.fetchall()

        # truncate_table_sql = '''truncate table holder_market_value'''
        # cur.execute(truncate_table_sql)
        # mysql_conn.commit()
        # print('清除holder_market_value表中数据，已完毕')

        sql = "INSERT INTO holder_market_value(innerCode,shHolderCode,shHolderName,endDate,holderamt,holderRTO,pfHolderamt,tradeDate, tClose,holderMoney, totmktcap)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = holder_chigu_infos
        cur.executemany(sql, val)
        mysql_conn.commit()
        print('holder_market_value批量插入数据完成')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


def sum_():
    # 过往减持股票对应金额合计(做一个update操作)
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()

    endDater = time.strftime("%Y1231", time.localtime(int(time.time()) - (1546185600 - 1514736000)))
    endDatel = time.strftime("%Y%m%d", time.localtime(int(time.time()) - (1546185600 - 1388332800)))

    try:
        truncate_table_sql = '''truncate table sum_five_value'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清除sum_five_value表中数据，已完毕')

        old_five_sql = '''select A.*,B.companyCode,B.companyName,secuAbbr from 
(
select compCode,shHolderCode,shHolderName,round(sum(totChgAMT*totAvgPrice),2) as CHGMONEY   
                                             from sq_sk_sharehdchg 
                                              where changeDire='2' and (shHolderNature='2' or shHolderNature='3' )
                                                    and endDate >='{}' 
                                              group by compCode,shHolderCode,shHolderName
 ) as A
left join 
(                                 
select innerCode,companyCode,companyName,secuAbbr from sq_sk_basicinfo where setype='101' and listStatus='1'
) as B
on A.compCode=B.innerCode'''.format(endDatel)

        cur.execute(old_five_sql)
        old_five_infos = cur.fetchall()

        sql = "INSERT INTO sum_five_value(compCode,shHolderCode,shHolderName,CHGMONEY,companyCode,companyName,secuAbbr)\
        VALUES (%s,%s,%s,%s,%s,%s,%s)"
        val = old_five_infos
        cur.executemany(sql, val)
        mysql_conn.commit()
        print('sum_five_value批量插入数据完成')

        sql_replace = "update sum_five_value set CHGMONEY=0.00 where CHGMONEY is null"
        cur.execute(sql_replace)
        mysql_conn.commit()
        print('sum_five_value批量更新数据完成')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


def lastdate_():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    try:
        # lastdate_sql = '''select * from 
        #         ((select C.*,D.totAvgPrice,D.totChgAMT,ROUND(D.totAvgPrice*D.totChgAMT,2) from 
        #         (select compCode,shHolderCode,shHolderName,max(endDate) as lastdate from sq_sk_sharehdchg  
        #                                where changeDire='2' and (shHolderNature='2' or shHolderNature='3' )
        #                                group by  compCode,shHolderCode,shHolderName) as C 
        #                                left join 
        #                                sq_sk_sharehdchg as D
        #                                on C.compCode=D.compCode and C.shHolderCode=D.shHolderCode 
        #                                  and C.shHolderName=D.shHolderName
        #                                  and C.lastdate=D.endDate


        #         ) as A ,
        #         (
        #         select distinct innerCode,compName,compSname from sq_comp_info 

        #         ) as B)
        #         where A.compCode=B.innerCode'''

        lastdate_sql = """
            select * from 
            (select A.* from (select compCode,shHolderCode,shHolderName, (case when totAvgPrice*totChgAMT is not null then endDate else null end)as lastdate,totAvgPrice, totChgAMT,ROUND(totAvgPrice*totChgAMT,2)  from sq_sk_sharehdchg  
                                   where changeDire='2' and (shHolderNature='2' or shHolderNature='3')) as A, (select compCode,shHolderCode,shHolderName,max(endDate) as lastdate from sq_sk_sharehdchg  
                                   where changeDire='2' and (shHolderNature='2' or shHolderNature='3' )
                                   group by  compCode,shHolderCode,shHolderName)as B where A.compCode=B.compCode and A.shHolderCode=B.shHolderCode and A.shHolderName=B.shHolderName and B.lastdate=A.lastdate

            ) as C ,
            (
            select distinct innerCode,compName,compSname from sq_comp_info 

            ) as D
            where C.compCode=D.innerCode
        """
        cur.execute(lastdate_sql)
        lastdate_infos = cur.fetchall()

        truncate_table_sql = '''truncate table lastdate_value'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清除lastdate_value表中数据，已完毕')

        sql = "INSERT INTO lastdate_value(compCode,shHolderCode,shHolderName,lastdate,totAvgPrice,totChgAMT,total_price,innerCode,compName,compSname)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = lastdate_infos
        cur.executemany(sql, val)
        mysql_conn.commit()
        print('lastdate_value批量插入数据完成')

        sql_replace = "update lastdate_value set total_price=0.0000 where total_price is null"
        cur.execute(sql_replace)
        mysql_conn.commit()
        print('lastdate_value批量更新数据完成')


    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


# 获取过往减持股票对应金额合计5年
def merge_five_year_money():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    try:
        sql = '''SELECT A.*,B.CHGMONEY FROM (select * from tmp_sq_comp_manager_main_11)  as A 
    LEFT JOIN(
            SELECT CHGMONEY, compCode, shHolderName from sum_five_value
    ) as B  ON A.innerCode=B.compCode and A.CName=B.shHolderName'''
        cur.execute(sql)
        sum_five_infos = cur.fetchall()

        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_sum_five_value'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清除tmp_sq_comp_manager_main_sum_five_value表中数据，已完毕')

        sql = "INSERT INTO tmp_sq_comp_manager_main_sum_five_value(innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,CHGMONEY)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = sum_five_infos
        cur.executemany(sql, val)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_main_sum_five_value批量插入数据完成')
        sql_replace = "update tmp_sq_comp_manager_main_sum_five_value set CHGMONEY=0.00 where CHGMONEY is null"
        cur.execute(sql_replace)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_main_sum_five_value批量更新数据完成')


    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


# 持股市值(持股人)
def merge_holder_money(tradeDate):
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    try:
        sql = '''SELECT A.*,B.holderMoney, B.pfHolderamt, (B.totmktcap*10000)as totmktcap  FROM  (select * from  tmp_sq_comp_manager_main_sum_five_value) as A 
LEFT JOIN(
        SELECT holderMoney, pfHolderamt, innerCode,shHolderName, totmktcap  from  holder_market_value where tradeDate='{}'
) as B  ON A.innerCode=B.innerCode and A.CName=B.shHolderName'''.format(tradeDate)
        cur.execute(sql)
        sum_five_infos = cur.fetchall()

        truncate_table_sql = '''truncate table tmp_sq_comp_manager_holder_market_value'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清除tmp_sq_comp_manager_holder_market_value表中数据，已完毕')

        sql = "INSERT INTO tmp_sq_comp_manager_holder_market_value(innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,CHGMONEY,holderMoney,pfHolderamt,totmktcap)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = sum_five_infos
        cur.executemany(sql, val)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_holder_market_value批量插入数据完成')
        sql_replace = "update tmp_sq_comp_manager_holder_market_value set holderMoney=0.00, pfHolderamt=0.00, totmktcap=0.000 where holderMoney is null or pfHolderamt is null or totmktcap is null"
        cur.execute(sql_replace)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_holder_market_value批量更新数据完成')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


# 最近一次减持时间和减持金额
def merge_lastdate_money():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    try:
        sql = '''SELECT A.*,B.lastdate,B.total_price FROM (select * from tmp_sq_comp_manager_holder_market_value) as A 
LEFT JOIN(
        SELECT lastdate,innerCode, shHolderName,total_price from lastdate_value
) as B  ON A.innerCode=B.innerCode and A.CName=B.shHolderName'''
        cur.execute(sql)
        sum_five_infos = cur.fetchall()

        truncate_table_sql = '''truncate table tmp_sq_comp_manager_lastdate_value'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清除tmp_sq_comp_manager_lastdate_value表中数据，已完毕')

        sql = "INSERT INTO tmp_sq_comp_manager_lastdate_value(innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,CHGMONEY,holderMoney,pfHolderamt,totmktcap, lastdate,total_price)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = sum_five_infos
        cur.executemany(sql, val)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_lastdate_value批量插入数据完成')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


# 近5年现金流入合计估算
def merge_five_sum_money():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()

    try:
        sql = '''SELECT A.*,ROUND(A.CHGMONEY+B.fiveYearAvgAnnualRewArd*5+B.fiveYearSumTotalMomey, 2) as sum_price FROM (select * from tmp_sq_comp_manager_lastdate_value) as A 
        LEFT JOIN(
        SELECT (case when fiveYearAvgAnnualRewArd is NULL then 0 else fiveYearAvgAnnualRewArd end)as fiveYearAvgAnnualRewArd, innerCode, CName, (case when fiveYearSumTotalMomey is NULL then 0 else fiveYearSumTotalMomey end)as fiveYearSumTotalMomey  FROM tmp_sq_comp_manager_main_16
) as B  ON A.innerCode=B.innerCode and A.CName=B.CName'''
        cur.execute(sql)
        sum_five_infos = cur.fetchall()

        truncate_table_sql = '''truncate table tmp_sq_comp_manager_sum_price'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清除tmp_sq_comp_manager_sum_price表中数据，已完毕')

        sql = "INSERT INTO tmp_sq_comp_manager_sum_price(innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,CHGMONEY,holderMoney,pfHolderamt,totmktcap,lastdate,total_price,sum_price)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = sum_five_infos
        cur.executemany(sql, val)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_sum_price批量插入数据完成')

        sql_update = '''UPDATE tmp_sq_comp_manager_sum_price D
 INNER JOIN(
SELECT A.*, B.fiveYearSumTotalMomey FROM (select * from tmp_five_year_sum_annualRewArd_message) as A LEFT JOIN(
SELECT innerCode,CName,(case WHEN fiveYearSumTotalMomey is NULL THEN 0 ELSE fiveYearSumTotalMomey END)as fiveYearSumTotalMomey from tmp_sq_comp_manager_main_16 
)as B ON A.innerCode=B.innerCode and A.CName=B.CName
 )as C
 ON D.innerCode=C.innerCode and D.CName= C.CName 
set D.sum_price = ROUND(D.CHGMONEY+C.fiveYearSumAnnualRewArd+C.fiveYearSumTotalMomey, 2)'''
        cur.execute(sql_update)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_sum_price批量更新sum_price字段数据完成')

        sql_replace = "update tmp_sq_comp_manager_sum_price set sum_price=0.00,totmktcap=0.000 where sum_price is null or totmktcap is null"
        cur.execute(sql_replace)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_sum_price批量更新数据完成')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


# 补充主表中公司总市值
def merge_company_totmktcap(tradeDate):
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    try:
        # new_date_sql = '''select tradeDate from sq_qt_skdailyprice group by tradeDate order by tradeDate DESC LIMIT 1'''
        # cur.execute(new_date_sql)
        # new_date_infos = cur.fetchall()
        # tradeDate = new_date_infos[0][0]
        print('merge_company_totmktcap()--------', tradeDate, '--------')

        sql = '''SELECT A.innerCode,A.personalCode,A.CName,A.beginDate,A.endDate,A.dimReason,A.actdutyName,A.companyCode,A.companyName,A.secuCode,A.secuAbbr,A.LC_companyCode,A.CHGMONEY,A.holderMoney,A.pfHolderamt,(B.totmktcap*10000), A.lastdate,A.total_price,A.sum_price FROM (select * from tmp_sq_comp_manager_sum_price) as A 
        LEFT JOIN(
        SELECT companyCode, totmktcap  FROM sq_qt_skdailyprice where tradeDate="{}"
) as B  ON A.companyCode=B.companyCode'''.format(tradeDate)
        cur.execute(sql)
        sum_five_infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_sum_price'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清除表中数据，已完毕')
        sql = "INSERT INTO tmp_sq_comp_manager_sum_price(innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,CHGMONEY,holderMoney,pfHolderamt,totmktcap,lastdate,total_price,sum_price)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = sum_five_infos
        cur.executemany(sql, val)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_sum_price批量插入数据完成')
        sql_replace = "update tmp_sq_comp_manager_sum_price set totmktcap=0.000 where totmktcap is null"
        cur.execute(sql_replace)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_sum_price批量更新数据完成')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


# 合并指标22-31
def merge_personrecord_income_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql1 = '''SELECT A.*,B.* from
        (SELECT * from tmp_sq_comp_manager_main_16) as A left JOIN
        (select * from tmp_sq_comp_manager_sum_price) as B on A.innerCode = B.innerCode and A.CName = B.CName'''
    try:
        cur.execute(select_sql1)
        infos = cur.fetchall()
        i = 0
        L = []
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_17'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功')
        for info in infos:
            i += 1
            print(i)
            info = info[0:17] + info[-7::1]
            L.append(info)
            insert_sql = '''insert into tmp_sq_comp_manager_main_17 (innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearAvgAnnualRewArd,lastYearTotalMoney,fiveYearSumTotalMomey,CHGMONEY,holderMoney,pfHolderamt,totmktcap, lastdate,total_price,sum_price)
                                                            value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            if i % 100 == 0:  # 批量插入，每次一百条
                cur.executemany(insert_sql, L)
                mysql_conn.commit()
                L = []
        cur.executemany(insert_sql, L)
        mysql_conn.commit()


    except Exception as e:
        print("ERROR,{}".format(str(e)))
    finally:
        mysql_conn.close()


# 主表和个人学校信息合并
def merge_personal_school_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    sql = '''select * from tmp_sq_comp_manager_main_17'''
    try:
        cur.execute(sql)
        infos = cur.fetchall()
        i = 0
        list_info = []
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_18'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功')
        for info in infos:
            i += 1
            print(i)
            l_school = []
            personalCode = info[1]
            CName = info[2]
            if personalCode:
                school_sql = '''SELECT personalCode, cName, school FROM extract_school where personalCode="{}" and CName = "{}"'''.format(
                    personalCode, CName)
                if cur.execute(school_sql):
                    school_info = cur.fetchone()
                    school = school_info[2]
                else:
                    school = '无'
            else:
                school = '无'
            l_school.append(school)
            t_school = tuple(l_school)
            info = info + t_school
            list_info.append(info)
            insert_sql = "INSERT INTO tmp_sq_comp_manager_main_18(innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearAvgAnnualRewArd,lastYearTotalMoney,fiveYearSumTotalMomey,CHGMONEY,holderMoney,pfHolderamt,totmktcap, lastdate,total_price,sum_price,school)\
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            if i % 100 == 0:
                cur.executemany(insert_sql, list_info)
                mysql_conn.commit()
                list_info = []
        cur.executemany(insert_sql, list_info)
        mysql_conn.commit()

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


# 主表和人物信息表合并
def merge_comp_personrecord_message():
    now = datetime.datetime.now()
    year = datetime.datetime.strftime(now, '%Y')
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql1 = '''SELECT A.*,B.* from
        (SELECT * from tmp_sq_comp_manager_main_18) as A left JOIN
        (select (case when gender='F' THEN '女' when gender='M' then '男' END)as gender,birthday,
(case when degree='1'then '小学' when degree='2' then '初中' when degree='3' then '高中' when degree='4' then '大专'
when degree='5' then '本科' when degree='6' then '硕士研究生' when degree='7' then '博士研究生' 
when degree='8' then '中专' when degree='99' then '其他' END) as degree,titles,memo,cName,personalCode
 from sq_comp_personrecord) as B on A.CName = B.cName and A.personalCode = B.personalCode'''
    try:
        cur.execute(select_sql1)
        infos = cur.fetchall()
        i = 0
        L = []
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_19'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功')
        for info in infos:
            i += 1
            print(i)
            list_age = []
            birthday = info[26]
            if birthday:
                if len(birthday) >= 4:
                    age = int(year) - int(birthday[0:4])
                else:
                    age = 0
            else:
                age = 0
            list_age.append(age)
            info = info[0:30] + tuple(list_age)
            L.append(info)
            insert_sql = '''insert into tmp_sq_comp_manager_main_19 (innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearAvgAnnualRewArd,lastYearTotalMoney,fiveYearSumTotalMomey,CHGMONEY,holderMoney,pfHolderamt,totmktcap, lastdate,total_price,sum_price,school,gender,birthday,degree,titles,memo,age)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            if i % 100 == 0:  # 批量插入，每次一百条
                cur.executemany(insert_sql, L)
                mysql_conn.commit()
                L = []
        cur.executemany(insert_sql, L)
        mysql_conn.commit()

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


def merge_busniess_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()

    try:
        select_sql1 = '''SELECT A.*, B.SocialUnifiedCreditCode, B.registeredProvinces,  B.registeredCity,B.operationProvinces, B.operationCity,B.companytype, B.guoqi FROM 
(SELECT * FROM tmp_sq_comp_manager_main_19) as A 
LEFT JOIN(
        SELECT companyName, secuAbbr, SocialUnifiedCreditCode, registeredProvinces,  registeredCity,operationProvinces, operationCity,"A股上市" as companytype ,SecuCode, guoqi FROM tmp_sq_comp_info
) as B  ON A.secuCode=B.SecuCode'''
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_20'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功')

        cur.execute(select_sql1)
        infos = cur.fetchall()
        insert_sql = '''insert into tmp_sq_comp_manager_main_20 (innerCode,personalCode,CName,beginDate,endDate,dimReason,actdutyName,companyCode,companyName,secuCode,secuAbbr,LC_companyCode,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearAvgAnnualRewArd,lastYearTotalMoney,fiveYearSumTotalMomey,CHGMONEY,holderMoney,pfHolderamt,totmktcap, lastdate,total_price,sum_price,school,gender,birthday,degree,titles,memo,age, SocialUnifiedCreditCode, registeredProvinces,  registeredCity,operationProvinces, operationCity,companytype,guoqi)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur.executemany(val, infos)
        mysql_conn.commit()
        print('成功！')

        sql_update = "UPDATE tmp_sq_comp_manager_main_20 SET  memo = REPLACE(REPLACE(memo, CHAR(10), ''), CHAR(13), '');"
        cur.execute(sql_update)
        mysql_conn.commit()
        print('更改成功')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


# A股补充个人持股对应市值的数据sql
def supplement_holder_money_message(tradeDate):
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()

    # new_date_sql = '''select tradeDate from sq_qt_skdailyprice group by tradeDate order by tradeDate DESC LIMIT 1'''
    # cur.execute(new_date_sql)
    # new_date_infos = cur.fetchall()
    # tradeDate = new_date_infos[0][0]
    print('supplement_holder_money_message()---------', tradeDate, '------------')

    # totalSKRTO字段单位是% ，取截止目前持股数市值时,会用到totalSKRTO字段 需要除以100
    try:
        select_sql1 = '''SELECT c.CName,(case WHEN personalCode is not NULL and personalCode != '' THEN(CONCAT(innerCode, personalCode)) ELSE innerCode END) as number,c.companyName,c.secuAbbr,c.SocialUnifiedCreditCode,
c.registeredProvinces,c.registeredCity,c.operationProvinces,c.operationCity,c.companytype,c.secuCode,
c.totmktcap,c.guoqi,c.actdutyName,c.beginDate,c.endDate,c.dimReason,c.gender,c.birthday,c.age,c.school,c.degree,c.titles,c.memo,
(case when c.holderMoney = 0 THEN (c.tClose*d.grantNum) ELSE c.holderMoney END) AS holderMoney,c.sum_price,c.CHGMONEY,
c.total_price,c.lastdate,c.pfHolderamt,c.fiveYearAvgAnnualRewArd,c.oneYearAnnualRewArd,c.oneYearCompAnnualRewArd,c.fiveYearSumTotalMomey,c.lastYearTotalMoney, (
CASE
    
    WHEN (CASE WHEN c.holderMoney = 0 or c.holderMoney is null THEN (c.tClose*d.grantNum) ELSE c.holderMoney END ) = (c.tClose*d.grantNum)
    THEN '股权激励名单' 
    WHEN (CASE WHEN c.holderMoney = 0 or c.holderMoney is null THEN (c.tClose*d.grantNum) ELSE c.holderMoney END ) is not null AND (CASE WHEN c.holderMoney = 0 or c.holderMoney is null THEN (c.tClose*d.grantNum) ELSE c.holderMoney END )=c.holderMoney and c.holderMoney is not null
    THEN '股东持股名单' 
    ELSE NULL 
END 
    ) AS holderMoneySource from
(SELECT m.*,p.lClose,p.tClose from tmp_sq_comp_manager_main_20 as m left join (select tradeDate,companyCode,sName,lClose,tClose,totmktcap from sq_qt_skdailyprice 
                             where tradeDate='{}' and isValid='1' and 
                                   (exchange='001002' or   exchange='001003')) as p on p.companyCode=m.companyCode) as c LEFT JOIN
(SELECT b.skincentiveCode,b.compCode,a.incenObjCode,a.incenObj,a.grantNum
from (SELECT incenObjCode,incenObj,skincentiveCode,(grantNum * 10000) AS grantNum  from sq_sk_incentiveobjlist WHERE declareDate > '20170101' GROUP BY skincentiveCode,incenObj)as a LEFT JOIN
(SELECT skincentiveCode,compCode from sq_sk_incentive) as b on a.skincentiveCode = b.skincentiveCode) as d on c.innerCode = d.compCode and c.CName = d.incenObj GROUP BY cName,companyName,number'''.format(tradeDate)
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_21_1'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功21_1')

        cur.execute(select_sql1)
        infos = cur.fetchall()
        insert_sql = '''insert into tmp_sq_comp_manager_main_21_1 (CName,number,companyName,secuAbbr,SocialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companytype,secuCode,totmktcap,guoqi,
        actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sum_price,CHGMONEY,total_price,lastdate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd, fiveYearSumTotalMomey, lastYearTotalMoney, holderMoneySource)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur.executemany(val, infos)
        mysql_conn.commit()
        print('插入数据成功21_1')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()

def update_add_number_01():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = "select c.CName,c.nnumber as number ,c.companyName  ,c.secuAbbr ,c.SocialUnifiedCreditCode  ,c.registeredProvinces  ,c.registeredCity   ,c.operationProvinces   ,c.operationCity    ,c.companytype  ,c.secuCode ,c.totmktcap    ,c.guoqi    ,c.actdutyName  ,c.beginDate    ,c.endDate  ,c.dimReason    ,c.gender   ,c.birthday ,c.age  ,c.school   ,c.degree   ,c.titles   ,c.memo ,c.holderMoney  ,c.sum_price    ,c.CHGMONEY ,c.total_price  ,c.lastdate ,c.pfHolderamt  ,c.fiveYearAvgAnnualRewArd  ,c.oneYearAnnualRewArd  ,c.oneYearCompAnnualRewArd  ,c.fiveYearSumTotalMomey, (case when c.lastYearTotalMoney is not null then d.publishDate else NULL end) as lastYeardivPublishDate,  (case when c.lastYearTotalMoney is not null then d.cashDvarrBegDate else NULL end ) as lastYeartoAccountDate    ,c.lastYearTotalMoney, c.holderMoneySource from (SELECT a.*,(case when length(a.number) = 8 and b.number is not null then b.number else a.number end) as nnumber FROM (select * from tmp_sq_comp_manager_main_21_1) as a LEFT JOIN tmp_sq_comp_number as b on a.SecuCode=b.SecuCode and a.CName=b.CName) as c LEFT JOIN (select A.* from (select innerCode, publishDate, cashDvarrBegDate from sq_sk_dividents where PROJECTTYPE='1' and diviYear='2019'  GROUP BY innerCode order by secuCode desc) as A ,(SELECT * FROM tmp_sq_comp_manager_main_21_1 where lastYearTotalMoney is not null and lastYearTotalMoney != 0 GROUP BY substring(number,1, 8)) as B where A.innerCode=substring(B.number,1,8)) as d ON substring(c.nnumber,1, 8)=d.innerCode"
    try:
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_27'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功27')

        cur.execute(select_sql)
        infos = cur.fetchall()
        insert_sql = '''insert into tmp_sq_comp_manager_main_27 (CName,number,companyName,secuAbbr,SocialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companytype,secuCode,totmktcap,guoqi,
        actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sum_price,CHGMONEY,total_price,lastdate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd, fiveYearSumTotalMomey,lastYeardivPublishDate, lastYeartoAccountDate, lastYearTotalMoney, holderMoneySource)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur.executemany(val, infos)
        mysql_conn.commit()
        print('插入数据成功27')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()


def update_add_number():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    number_sql = '''SELECT max(substring(number,12) ) FROM tmp_sq_comp_number '''
    cur.execute(number_sql)
    i = int(cur.fetchone()[0])+1
    try:
        sql_ = '''
            SELECT * FROM tmp_sq_comp_manager_main_27 WHERE LENGTH(number) = 8
        '''
        cur.execute(sql_)
        infos = cur.fetchall()
        for info in infos:
            n = str(i)
            CName = info[0]
            companyName = info[2]
            number = str(info[1])+'ASK'+n.zfill(5)
            update_sql = "update tmp_sq_comp_manager_main_27 set number='{}' where CName = '{}' and companyName = '{}'".format(number,CName,companyName)
            cur.execute(update_sql)
            mysql_conn.commit()
            print('更新tmp_sq_comp_manager_main_27数据成功',number)

            number_inset_sql = "insert into tmp_sq_comp_number (innerCode,CName,number,secuCode) VALUES('%s','%s','%s','%s')" %(info[1], CName, number, info[10])
            cur.execute(number_inset_sql)
            mysql_conn.commit()
            print('更新tmp_sq_comp_number数据成功',number)
            i += 1
    except Exception as e:
        print("ERROR,{}".format(str(e)))
    else:
        pass
    finally:
        mysql_conn.close()

# 对lastdate格式处理
def update_last_date():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()

    try:
        select_sql1 = '''SELECT * from tmp_sq_comp_manager_main_23 where lastdate is not NULL'''
        cur.execute(select_sql1)
        infos = cur.fetchall()
        i = 1
        for info in infos:
            CName = info[0]
            companyName = info[2]
            lastdate = info[28]
            if len(lastdate) == 8:
                lastdate = lastdate[0:4] + '-' + lastdate[4:6] + '-' + lastdate[6:8]
            update_sql = """update tmp_sq_comp_manager_main_23 set lastdate='{}' where CName = '{}' and companyName = '{}'""".format(lastdate, CName, companyName)
            cur.execute(update_sql)
            mysql_conn.commit()
            i += 1
            print('更新lastdate成功：' + str(i))

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()

# A股解禁数据
def jiejin_AGU():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    try:
        #清空表25_1
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_25_1'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功25_1')

        # 增添innerCode,companyCode用于匹配
        add_sql = "INSERT into tmp_sq_comp_manager_main_25_1 SELECT a.*,b.innerCode,b.companyCode from (SELECT * from tmp_sq_comp_manager_main_27) as a LEFT JOIN (SELECT * from tmp_sq_comp_manager_main_11) as b on a.companyName = b.companyName and a.CName = b.CName"
        cur.execute(add_sql)
        mysql_conn.commit()
        print('添加数据成功25_1')

        #清空表24_1
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_24_1'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功24_1')
        # 获取解禁日期和股数和金额数据
        add_date_sql = '''INSERT into tmp_sq_comp_manager_main_24_1 SELECT c.CName,c.number,c.companyName,c.secuAbbr,c.SocialUnifiedCreditCode,
                    c.registeredProvinces,c.registeredCity,c.operationProvinces,c.operationCity,c.companytype,c.secuCode,
                    c.totmktcap,c.guoqi,c.actdutyName,c.beginDate,c.endDate,c.dimReason,c.gender,c.birthday,c.age,c.school,c.degree,c.titles,c.memo,
                    c.holderMoney,c.sum_price,c.innerCode,c.listDate,c.newListingSKAmt,(c.newListingSKAmt*d.tClose) as limskMoney, c.projectFeature,c.holderMoneySource from (SELECT a.*,b.listDate,b.newListingSKAmt, b.projectFeature from (SELECT * from tmp_sq_comp_manager_main_25_1) as a LEFT JOIN
                    (SELECT * from  tq_comp_limskholder) as b on a.CName = b.limskHolderName and a.innerCode = b.compCode) as c LEFT JOIN
                    (SELECT sName,companyCode, AVG(tClose) as tClose from sq_qt_skdailyprice GROUP BY sName) as d on c.companyCode = d.companyCode'''

        cur.execute(add_date_sql)
        mysql_conn.commit()
        print('添加数据成功24_1')


        #清空表26_1
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_26_1'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功26_1')
        # 解禁添加金额求和数据
        add_sum_sql = "INSERT into tmp_sq_comp_manager_main_26_1 SELECT a.CName,a.number,a.companyName,a.secuAbbr,a.SocialUnifiedCreditCode,a.registeredProvinces,a.registeredCity,a.operationProvinces,a.operationCity,a.companytype,a.secuCode,a.totmktcap,a.guoqi,a.actdutyName,a.beginDate,a.endDate,a.dimReason,a.gender,a.birthday,a.age,a.school,a.degree,a.titles,a.memo,a.holderMoney,a.sum_price,a.innerCode,a.listDate,a.newListingSKAmt,a.limskMoney,a.projectFeature, b.sumLimskMoney,a.holderMoneySource from (SELECT * from tmp_sq_comp_manager_main_24_1) as a LEFT JOIN (SELECT CName,companyName,sum(limskMoney) as sumLimskMoney from tmp_sq_comp_manager_main_24_1 GROUP BY CName, companyName) as b on a.CName = b.CName and a.companyName = b.companyName"
        cur.execute(add_sum_sql)
        mysql_conn.commit()
        print('添加数据成功26_1')

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()

# 同步数据A谷docker
def dwd_ms_acomp_manager_all():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    cur172 = mysql_conn172.cursor()

    select_sql = "select CName,number,companyName,secuAbbr,SocialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companytype,secuCode,totmktcap,guoqi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sum_price,CHGMONEY,total_price,lastdate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd, fiveYearSumTotalMomey,lastYeardivPublishDate,lastYeartoAccountDate, lastYearTotalMoney, holderMoneySource,'{}','{}','{}' from tmp_sq_comp_manager_main_27 ".format(isValid,  dataState, createTime)
    try: 
        cur.execute(select_sql)
        infos = cur.fetchall()
        insert_sql = '''insert into dwd_ms_acomp_manager_all (cName,number,companyName,secuAbbr,socialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companyType,secuCode,totmktcap,guoQi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,holderMoney,sumPrice,chgMoney,totalPrice,lastDate,pfHolderamt,fiveYearAvgAnnualRewArd,oneYearAnnualRewArd,oneYearCompAnnualRewArd,fiveYearSumTotalMomey,lastYeardivPublishDate,lastYeartoAccountDate,lastYearTotalMoney,holderMoneySource,isValid,dataStatus,createTime)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur172.executemany(val, infos)
        mysql_conn172.commit()
        print('插入数据成功dwd_ms_acomp_manager_all')
        return len(infos)

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()
        mysql_conn172.close()

# 同步数据A股解禁docker 
def dwd_ms_abcomp_manager_all():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn172 = pool2.connection()
    cur172 = mysql_conn172.cursor()

    select_sql = "SELECT CName,number,companyName,secuAbbr,SocialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companytype,secuCode,totmktcap,guoqi,    actdutyName, beginDate,  endDate,    dimReason,  gender, birthday,   age,    school, degree, titles, memo,   holderMoney,    sum_price,  listDate,   newListingSKAmt,    limskMoney, sumLimskMoney, holderMoneySource,  '{}', '{}', '{}' FROM tmp_sq_comp_manager_main_26_1 ".format(isValid,  dataState, createTime)
    try:  
        cur.execute(select_sql)
        infos = cur.fetchall()
        insert_sql = '''insert into dwd_ms_abcomp_manager_all (cName,number, companyName, secuAbbr,socialUnifiedCreditCode,registeredProvinces, registeredCity, operationProvinces, operationCity, companyType,secuCode,totmktcap,guoQi,  actdutyName,beginDate,endDate, dimReason,  gender, birthday,   age,    school, degree, titles, memo,   holderMoney,    sumPrice,   listDate,   newListingSKAmt,    limskMonrey,    sumLimskMoney,holderMoneySource, isValid,dataStatus,createTime)
                                                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = insert_sql
        cur172.executemany(val, infos)
        mysql_conn172.commit()
        print('插入数据成功dwd_ms_abcomp_manager_all')
        return len(infos)

    except Exception as e:
        print("ERROR,{}".format(str(e)))

    finally:
        mysql_conn.close()
        mysql_conn172.close()

def main():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    new_date_sql = '''select tradeDate from sq_qt_skdailyprice group by tradeDate order by tradeDate DESC LIMIT 1'''
    cur.execute(new_date_sql)
    new_date_infos = cur.fetchone()
    tradeDate = new_date_infos[0]

    log_sql = """select max(add_date) from dwd_ms_data_log where table_="{}" """.format(table_name1)
    cursor.execute(log_sql)
    log_tradDate1 = cursor.fetchone()[0]

    if tradeDate == log_tradDate1:
        print("Warning 今日无新数据")
        exit()
    tmp_sq_comp_info()  
    tmp_sq_comp_manager_main_01()
    tmp_sq_comp_manager_main_02()
    tmp_sq_comp_manager_main_03()
    tmp_sq_comp_manager_main_03_01()
    wd_a_controllers_01()
    time.sleep(5)
    tmp_sq_comp_manager_main_04()
    tmp_sq_comp_manager_main_05()
    tmp_sq_comp_manager_main_06()
    tmp_sq_comp_manager_main_06_03_01()
    tmp_sq_comp_manager_main_07()
    tmp_sq_comp_manager_main_08()
    time.sleep(5)
    tmp_sq_comp_manager_main_09()
    tmp_sq_comp_manager_main_09_01()
    tmp_sq_comp_manager_main_10()
    tmp_sq_comp_manager_main_10_01()
    tmp_sq_comp_manager_main_11_01()
    tmp_sq_comp_manager_main_11_02()
    time.sleep(5)
    tmp_sq_comp_manager_main_11()
    one_year_annualRewArd_message()
    one_year_comp_annualRewArd_message()
    five_year_avg_annualRewArd_message()
    five_year_sum_annualRewArd_message()
    time.sleep(5)
    one_year_dividents_message()
    five_year_dividents_message()
    comp_one_year_dividents_message()
    comp_five_year_dividents_message()
    holder_(tradeDate)
    time.sleep(5)
    sum_()
    lastdate_()
    merge_five_year_money()
    merge_holder_money(tradeDate)
    merge_lastdate_money()
    time.sleep(5)
    merge_five_sum_money()
    merge_company_totmktcap(tradeDate)
    merge_personrecord_income_message()
    merge_personal_school_message()
    merge_comp_personrecord_message()
    time.sleep(5)
    merge_busniess_message()
    supplement_holder_money_message(tradeDate)
    update_add_number_01()
    update_add_number()
    #  # 不执行 update_last_date() 
    jiejin_AGU()
    add_data_num1 = dwd_ms_acomp_manager_all()    # docker172 
    add_data_num2 = dwd_ms_abcomp_manager_all()   # docker172

    if logging:
        sql_ = '''INSERT INTO {0} (table_,
                                add_date, 
                               add_number
                               )VALUES(%s,%s,%s)'''
        cursor.execute(sql_.format(mysql_log_name),(table_name1,
                                                    tradeDate, 
                                                    str(add_data_num1)
                                                    ))
        db.commit()

        sql_ = '''INSERT INTO {0} (table_,
                                add_date, 
                               add_number
                               )VALUES(%s,%s,%s)'''
        cursor.execute(sql_.format(mysql_log_name),(table_name2,
                                                    tradeDate, 
                                                    str(add_data_num2)
                                                    ))
        db.commit()

    db.close()







'''
# A股新数据和上一次历史数据根据姓名+股票代码对比查找新增数据, 21为新数据, 22为历史数据
INSERT into tmp_sq_comp_manager_main_23 SELECT a.* FROM (SELECT * from tmp_sq_comp_manager_main_21) as a LEFT JOIN
(SELECT CName, secuCode FROM tmp_sq_comp_manager_main_22) as b on a.CName = b.CName and a.secuCode = b.secuCode
WHERE b.CName is NULL GROUP BY CName, secuCode ORDER BY companyName

# A股新数据和上一次历史数据根据姓名+股票代码对比查找同一人同一家公司（公司名称会变，股票代码不变）减持金额和质押期数和分红金额是否变动, 21为新数据, 22为历史数据
INSERT into tmp_sq_comp_manager_main_23 SELECT a.CName,b.number,a.companyName,a.secuAbbr,a.SocialUnifiedCreditCode,
a.registeredProvinces,a.registeredCity,a.operationProvinces,a.operationCity,a.companytype,a.secuCode,
a.totmktcap,a.guoqi,a.actdutyName,a.beginDate,a.endDate,a.dimReason,a.gender,a.birthday,a.age,a.school,a.degree,a.titles,a.memo,
a.holderMoney,a.sum_price,a.CHGMONEY,a.total_price,a.lastdate,a.pfHolderamt,a.fiveYearAvgAnnualRewArd,a.oneYearAnnualRewArd,
a.oneYearCompAnnualRewArd,a.fiveYearSumTotalMomey,a.lastYearTotalMoney FROM (SELECT * from tmp_sq_comp_manager_main_21) as a LEFT JOIN
(SELECT cName, number, secuCode, CHGMONEY, total_price, pfHolderamt, fiveYearSumTotalMomey, lastYearTotalMoney
FROM tmp_sq_comp_manager_main_22) as b on a.CName = b.cName and a.secuCode = b.secuCode
WHERE a.CHGMONEY != b.CHGMONEY AND b.CHGMONEY !=0 or a.total_price != b.total_price and b.total_price != 0 or a.pfHolderamt != b.pfHolderamt and b.pfHolderamt != 0 or 
a.fiveYearSumTotalMomey != b.fiveYearSumTotalMomey and b.fiveYearSumTotalMomey != 0 or a.lastYearTotalMoney != b.lastYearTotalMoney and 
b.lastYearTotalMoney != 0 GROUP BY CName, secuCode ORDER BY companyName

# A股解禁数据处理
增添innerCode,companyCode用于匹配
INSERT into tmp_sq_comp_manager_main_25 SELECT a.*,b.innerCode,b.companyCode from (SELECT * from tmp_sq_comp_manager_main_21) as a LEFT JOIN
(SELECT * from tmp_sq_comp_manager_main_11) as b on a.companyName = b.companyName and a.CName = b.CName

# 获取解禁日期和股数和金额数据
INSERT into tmp_sq_comp_manager_main_24 SELECT c.CName,c.number,c.companyName,c.secuAbbr,c.SocialUnifiedCreditCode,
c.registeredProvinces,c.registeredCity,c.operationProvinces,c.operationCity,c.companytype,c.secuCode,
c.totmktcap,c.guoqi,c.actdutyName,c.beginDate,c.endDate,c.dimReason,c.gender,c.birthday,c.age,c.school,c.degree,c.titles,c.memo,
c.holderMoney,c.sum_price,c.innerCode,c.listDate,c.newListingSKAmt,(c.newListingSKAmt*d.tClose) as limskMoney from (SELECT a.*,b.listDate,b.newListingSKAmt from (SELECT * from tmp_sq_comp_manager_main_25) as a LEFT JOIN
(SELECT * from  tq_comp_limskholder) as b on a.CName = b.limskHolderName and a.innerCode = b.compCode) as c LEFT JOIN
(SELECT sName,companyCode, AVG(tClose) as tClose from sq_qt_skdailyprice GROUP BY sName) as d on c.companyCode = d.companyCode

# 解禁添加金额求和数据
INSERT into tmp_sq_comp_manager_main_26 SELECT a.*, b.sumLimskMoney from (SELECT * from tmp_sq_comp_manager_main_24) as a LEFT JOIN
(SELECT CName,companyName,sum(limskMoney) as sumLimskMoney from tmp_sq_comp_manager_main_24 GROUP BY CName, companyName) as b on a.CName = b.CName and a.companyName = b.companyName

# 删除重复数据
DELETE from business_info WHERE companyName in 
(SELECT companyName from (SELECT companyName from business_info GROUP BY companyName HAVING count(companyName) > 1) a)
and id not in (SELECT id from (SELECT min(id) as id from business_info GROUP BY companyName HAVING count(companyName) > 1) b)
'''

# 吧0.00格式的值转成空，导出来
'''SELECT CName,number,companyName,secuAbbr,SocialUnifiedCreditCode,registeredProvinces,registeredCity,operationProvinces,operationCity,companytype,secuCode,
(CASE WHEN totmktcap = 0.00 THEN NULL ELSE totmktcap END) as totmktcap,guoqi,actdutyName,beginDate,endDate,dimReason,gender,birthday,age,school,degree,titles,memo,
(CASE WHEN holderMoney = 0.00 THEN NULL ELSE holderMoney END) as holderMoney,
(CASE WHEN sum_price = 0.00 THEN NULL ELSE sum_price END) as sum_price,
(CASE WHEN CHGMONEY = 0.00 THEN NULL ELSE CHGMONEY END) as CHGMONEY,
(CASE WHEN total_price = 0.00 THEN NULL ELSE total_price END) as total_price,lastdate,
(CASE WHEN pfHolderamt = 0.00 THEN NULL ELSE pfHolderamt END) as pfHolderamt,
(CASE WHEN fiveYearAvgAnnualRewArd = 0.00 THEN NULL ELSE fiveYearAvgAnnualRewArd END) as fiveYearAvgAnnualRewArd,
(CASE WHEN oneYearAnnualRewArd = 0.00 THEN NULL ELSE oneYearAnnualRewArd END) as oneYearAnnualRewArd,
(CASE WHEN oneYearCompAnnualRewArd = 0.00 THEN NULL ELSE oneYearCompAnnualRewArd END) as oneYearCompAnnualRewArd,
(CASE WHEN fiveYearSumTotalMomey = 0.00 THEN NULL ELSE fiveYearSumTotalMomey END) as fiveYearSumTotalMomey,
(CASE WHEN lastYearTotalMoney = 0.00 THEN NULL ELSE lastYearTotalMoney END) as lastYearTotalMoney from tmp_sq_comp_manager_main_23'''

if __name__ == "__main__":
    t1 = datetime.datetime.now()
    addCopyAGuSourceData.main_Ag()
    main() 
    t2 = datetime.datetime.now()
    print(t2-t1)