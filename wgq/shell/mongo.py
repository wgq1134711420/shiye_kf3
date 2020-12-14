import pymongo
import time, datetime
from dateutil import parser
import csv
from lxml import etree

def is_chinese(string):
    """
    检查整个字符串是否包含中文
    :param string: 需要检查的字符串
    :return: bool
    """
    for ch in string:
        if u'\u4e00' <= ch <= u'\u9fff':
            return True
    return False


def _csv_():
    k = []
    with open("hif_结果.csv", "r", encoding='utf-8', newline="") as f:
        reader = csv.reader(f)
        for i in reader:
            k.append(i[0])
    print(k)
    myclient = pymongo.MongoClient(
                "mongodb://root:shiye1805A@192.168.1.125:10011/admin")

    mydb = myclient["sy_risk_raw"]
    mycol = mydb["sy_feedback_raw"]
    for i in k:
        url = mycol.find({'url':"{}".format(i)})
        list_s = ''
        for i in url:
            # print(i.get("web_contents"))
            urls = i.get("url")
            title = i.get("title")
            department = i.get("company_name")
            if i.get("web_contents"):
                for l,j in enumerate(i.get("web_contents")):
                    if l == 1 or l == 0 or l == 3 or l == 4:
                        list_s = list_s + j
                ret = is_chinese(list_s)
                print(list_s)
                print(ret)
                if ret:
                    with open("hif_未匹配上的.csv", "a", encoding='utf-8', newline="") as f:
                        k = csv.writer(f, dialect="excel")
                        k.writerow([urls, title, department])
                else:
                    with open("hif_图片解析未成功.csv", "a", encoding='utf-8', newline="") as f:
                        k = csv.writer(f, dialect="excel")
                        k.writerow([urls, title, department])

            # else:
            #     with open("hif_解析失败.csv", "a", encoding='utf-8', newline="") as f:
            #         k = csv.writer(f, dialect="excel")
            #         k.writerow([urls, title, department])

def _hif_():
    k = []
    with open("hif_未匹配上的.csv", "r", encoding='utf-8', newline="") as f:
        reader = csv.reader(f)
        for i in reader:
            k.append(i[0])
    print(k)
    myclient = pymongo.MongoClient(
        "mongodb://root:shiye1805A@192.168.1.125:10011/admin")

    mydb = myclient["sy_risk_raw"]
    mycol = mydb["sy_feedback_raw"]
    for i in k:
        url = mycol.find({'url': "{}".format(i)})
        for i in url:
            print(i.get("web_contents"))




def _label_d_v(value):
    re = etree.HTML(value)
    res = re.xpath("//text()")
    string = '\n\n\n'
    for i in res:
        string = string + i
    print(string)
    return string

def _label_d(j,web_contents,value):
    for k_i,k in enumerate(web_contents):
        try:
            if k.split() == j.split():
                value_s = _label_d_v(value)
                web_contents[k_i] = value_s
        except:
            pass
def _label_():
    myclient = pymongo.MongoClient(
        "mongodb://root:shiye1805A@192.168.1.125:10011/admin")

    mydb = myclient["sy_risk_raw"]
    mycol = mydb["sy_feedback_raw"]
    url = mycol.find({'url': "http://static.sse.com.cn/stock/information/c/201905/0a4de2b2f3e841a5b51dd329d3498685.pdf"})
    for i in url:
        web_contents = i.get("web_contents")
        label = i.get("label")
        for j in label:
            _label_d(j,web_contents,label[j])

if __name__ == '__main__':
    _label_()