import requests
import json

url = 'https://money.163.com/special/002557S6/newsdata_gp_index.js?callback=data_callback'

headers = {
# 'Accept': "application/json, text/javascript, */*; q=0.01",
#     'Accept-Encoding': "gzip, deflate",
#     'Accept-Language': "zh-CN,zh;q=0.9",
#     'Connection': "keep-alive",
#     'Content-Type': "application/x-www-form-urlencoded",
#     'Cookie': "SESSION=214a872f-c8ee-42f4-9135-cb75f1e4f153",
#     'Host': "credit.wuhu.gov.cn",
#     'Origin': "http://credit.wuhu.gov.cn",
#     'Referer': "https://www.baidu.com/",
#     'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
#     'X-Requested-With': "XMLHttpRequest",
}

post_data = {
'limit':'10',
    'offset':'0',
    'tyshxydm':'',
    'ztlx':'',
    'xzqh':'',
    'bmbh':'',
    'zdly':'',
}
# resp = requests.post(url,data=post_data,headers=headers)
resp = requests.get(url,headers=headers)
resp.encoding = "gbk"
# print(json.loads(resp.text))
print(resp.text)