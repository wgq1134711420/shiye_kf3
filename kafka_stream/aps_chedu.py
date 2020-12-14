# coding:utf-8
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import os
import smtplib
from email.mime.text import MIMEText
from mysql_yi.mysql_pool import PymysqlPool
from es_yi.es_pool import ES
import psutil


def job_mail(text_content):
    """
    msg_from:发送方的邮箱
    passwd:授权码
    msg_to:接收方的邮箱
    text_content:内容
    file_path:附件的地址
    :return:
    """
    msg = MIMEMultipart()
    msg_from = "1134711420@qq.com"
    passwd = "uasoecfsqttqhief"
    msg_to = "wangguoqiang@shiyejinrong.com"
    subject = "测试邮件"

    file_path = None
    text = MIMEText(text_content)
    msg.attach(text)

    if file_path:  # 附件所在的位置
        demoFile = file_path
        demoFile = MIMEApplication(open(demoFile, 'rb').read())
        demoFile.add_header('Content-Disposition', 'attachment', filename=docFile)
        msg.attach(demoFile)

    msg['Subject'] = subject
    msg['From'] = msg_from
    msg['To'] = msg_to

    try:
        s = smtplib.SMTP_SSL("smtp.qq.com", 465)
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print("successful")
    except smtplib.SMTPException:
        print("failure")
    finally:
        s.quit()

def mysq_es_mail_1():
    """
    每天定时 发送 es和 tisb 和 磁盘情况
    :return:
    """
    disk = es_disk_count()
    es_count_num = es_count()
    mysql_yq_search_count = mysql_yq_search()
    try:
        pass
    except:
        disk = "获取当前磁盘信息失败"
    conet = "{磁盘使用率：" + str(disk) + "}|{新闻的es数据数量：" + str(es_count_num) + "}|{tisb新闻的数据量：" + str(mysql_yq_search_count) + "}"
    job_mail(conet)

def mysq_es_mail_10():
    """
    每十分钟 对  es和 tisb 和 磁盘情况 进行预警
    :return:
    """
    disk = es_disk_count()
    es_count_num = es_count()
    try:
        es_tidb_num = es_tidb_count()
    except:
        es_tidb_num = 0
    mysql_yq_search_count = mysql_yq_search()
    try:
        print(disk_content)
    except:
        cont = "获取当前磁盘信息失败"
        print(cont)
    if disk > "80%":
        job_mail("当前磁盘使用率大于百分之八十请尽快清理")
    if (mysql_yq_search_count - es_count_num) > 30000:
        job_mail("当前新闻数据增长量的差值大于3万，tidb数据量：{}，es数据量{}".format(mysql_yq_search_count,es_count_num))
    if mysql_yq_search_count == es_tidb_num:
        job_mail("当前tidb数据已有十分钟未增长")
    if (mysql_yq_search_count - es_tidb_num) < 30000:
        job_mail("十分钟内tidb的数据增长小于3万请查看是否正常，十分钟前的tidb数据量：{}，本次查询的tidb数据量：{}".format(es_tidb_num,mysql_yq_search_count))
        return
    es_tidn(mysql_yq_search_count)

def mysql_client_125():
    return PymysqlPool('125')
def mysql_yq_search():
    print("进行sq查询")
    conn = mysql_client_125()
    # sql = "SELECT count(*) FROM sy_project_raw.dwa_me_gg_search_wgq_his_yue WHERE pubTime > '2020-11-01' and srcType = '新闻'"
    sql = "SELECT count(*) FROM sy_project_raw.dwa_me_gg_search_his "
    count, infos = conn.getAll(sql)
    conn.dispose()
    tidb_num = 0
    for i in infos:
        tidb_num = i.get("{}".format(i))
    return tidb_num

def es_count():

    """
    查询es的数据总量
    :return:
    """
    query = {
        "query": {"match_all": {}}
    }
    data_count = ES().es.search(index='sy_comp_announ_index_his', body=query, doc_type='doc')
    _count = data_count.get("hits").get("total")
    print(_count)
    return _count


def es_tidb_count():

    """
    查询上一次的tidb数据量
    :return:
    """
    query = {
        "query": {"match_all": {}}
    }
    data_count = ES().es.search(index='tidb_num_index', body=query, doc_type='doc')
    tidb_num = [i.get("_source").get("tidb_num") for i in data_count.get("hits").get("hits")]
    print(tidb_num)
    return tidb_num

def es_disk_count():

    """
    查询磁盘的情况
    :return:
    """
    query = {
        "query": {"match_all": {}}
    }
    data_count = ES().es.search(index='disk_index', body=query, doc_type='doc')
    disk = [i.get("_source").get("mem_percent") for i in data_count.get("hits").get("hits")]
    print(disk)
    return disk

def es_tidn(i_i):
    """
    tidb的数据量写入es
    :return:
    """
    ES().es.index(index="tidb_num_index",doc_type="doc",id=0,body={"tidb_num":i_i})


"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
def linux_disk():
    """
    29.5 计算当前空间的使用率
    :return:
    """
    '''获取磁盘使用情况'''
    disk_stat = psutil.disk_usage('/')
    disk_total = disk_stat.total // 1024 // 1024 // 1024
    disk_free = disk_stat.free // 1024 // 1024 // 1024
    disk_percent = '%s%%' % disk_stat.percent
    disk_data = {'mem_toal': disk_total, 'mem_free': disk_free, 'mem_percent': disk_percent}
    return disk_data

def es_instal():
    """
    29.5 将磁盘的使用情况写入 es
    :return:
    """
    disk_data = linux_disk()
    ES().es.index(index="disk_index",doc_type="doc",id=0,body={"mem_toal":disk_data.get("mem_toal"),"mem_free":disk_data.get("mem_free"),"mem_percent":disk_data.get("mem_percent")})


def job_clean_up():
    """
    29.5 每天定时清理磁盘日志
    :return:
    """

    a = open('/shiye_kf3/gonggao/kafka_stream/ouhno_a', "r+")
    a.truncate()
    print("清空数据a",time.ctime())
    s = open('/shiye_kf3/gonggao/kafka_stream/ouhno_s', "r+")
    s.truncate()
    print("清空数据s",time.ctime())


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    # 每隔10分钟执行一次 job_func;
    # scheduler .add_job(func=es_count, trigger='interval', minutes=1)
    # 每隔1秒执行一次 job_func;
    scheduler .add_job(func=es_instal, trigger='interval', seconds=30)
    # 每天某个时刻  处理日志文件
    scheduler.add_job(func=job_clean_up, trigger='cron', hour='14', minute='15', second='30')
    # 每天某个时刻  发送文件
    # scheduler.add_job(func=mysq_es_mail_1, trigger='cron', hour='7', minute='30', second='30')



    scheduler.start()