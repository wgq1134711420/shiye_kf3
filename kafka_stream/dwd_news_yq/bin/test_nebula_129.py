#-*-coding:utf-8-*-

import requests,json

company = '北京小桔科技有限公司'

# url = 'http://192.168.1.129:11068/relation/'

r = requests.post('http://192.168.1.129:11068/relation/',data=json.dumps({'company':company.replace('(','（').replace(')','）')}))
result_list = r.json()['data']
print(result_list,len(result_list))