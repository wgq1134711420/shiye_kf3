#coding:utf-8
#zhangcongcong@seeyii
'''
1，输入为科创板HTML解析html文件；
2，对html文件处理，存入对应mongo（该Mongo文件已经存在，且字段存在，追溯回去即可）
3，对更新后的Mongo中数据处理，如表格处理、误换行处理
4，写入ES
5，更新当前gtime sql 做为增量标志
'''
import os
import pymongo
import pymysql
import time
import re
#from tqdm import tqdm
import sys
import math
import argparse
import json
import htmlparser
import jieba
from itertools import islice
from PIL import Image
import base64
import subprocess


mongo_name = 'api_szzq_wxhj_html'

update_data_name = 'api_szzq_wxhj_html_test'

# 读取连接数据库配置文件
load_config='/home/seeyii/increase_nlp/db_config.json'
with open(load_config, 'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)

client = pymongo.MongoClient(config_local['cluster_mongo'])

mongodb = client['EI_BDP']
select_mongo_obj = mongodb[mongo_name]
update_mongo_obj = mongodb[update_data_name]
# dir_html = '/home/seeyii/xrh/'
# paths = []
# for root, directory, files in os.walk(dir_html):
# 	for filename in files:
# 		name, suf = os.path.splitext(filename)
# 		if suf == '.htm':
# 			paths.append(os.path.join(root, filename))
# print(paths)
class Rewrite2Monogo():
	"""docstring for rewrite2monogo"""
	def __init__(self, dir_html='',dir_pic='', file_name=''):
		super(Rewrite2Monogo, self).__init__()
		self.dir_html = dir_html
		self.dir_pic = dir_pic
		self.file_name = file_name
		self.suffix = '.htm'
	def getFiles(self): # 查找根目录，文件后缀 
		paths = []

		for root, directory, files in os.walk(self.dir_html):  # =>当前根,根下目录,目录下的文件
			for filename in files:
				name, suf = os.path.splitext(filename) # =>文件名,文件后缀
				if suf == self.suffix and name == self.file_name:
					paths.append(os.path.join(root, filename)) # =>吧一串字符串组合成路径
		# paths_new=[]
		# for path in paths:
		# 	gtime=int(path.split('/')[-1].split('#')[0])
		# 	#path_end='/'.join(path.split('/')[:-1])+'/'+path.split('/')[-1].split('#')[1]
		# 	if gtime>last_gtime:
		# 		#print(path)
		# 		paths_new.append(path)
		print(paths, '<<<<<<<<<<<<<<<')
		return paths
	#paths = getFiles('/database/data/sse_kcb/html_add/html20190903/','.htm')

	def base_64_fn(self,pic_path):
		#print(pic_path)
		with open(pic_path, 'rb') as f:
			base64_data = base64.b64encode(f.read())
		s = base64_data.decode()
		return s
	def imgsize1_fn(self,path):
		img = Image.open(path)
		return img.size
	def imgsize_fn(self,path):
		#返回图片大小
		size_ = os.path.getsize(path)
		return size_
	def get_pic_fn(self,path):
		#图片 替换为% 或反回图片base64格式
		pic_dict={}
		pic_small=[]
		pic_path=self.dir_pic+ path.split('/')[-1].replace('.htm','')
		#print(222222,self.dir_pic+ path.split('/')[-1].replace('.htm',''))
		img_list=[]
		root_path=''
		for root, directory, files in os.walk(pic_path):
			root_path=root
			img_list=files
		for filename in img_list:
			pic_path=os.path.join(root_path, filename)

			imgsize = self.imgsize_fn(pic_path)

			if imgsize<=300:
				pic_small.append(filename)
			else:
				#将大的图片转为base64
				pic_dict[filename]=self.base_64_fn(pic_path)
		return pic_small,pic_dict
	def process(self,html_path):
		pic_small,pic_dict =self.get_pic_fn(html_path)
		#print(pic_small)
		with open(html_path, 'r', encoding='utf8') as F:
			html_output = F.read()
############################################
#替换所有满足条件的%
		all_imgs = re.findall('<img.*?\>', html_output, re.S | re.I | re.M)#先取出所有的包干img部分
		for i in all_imgs:
			for j in pic_small:
				if j  in i:
					html_output = html_output.replace(i,'%')
#########################################
#先处理table

		table_list_parttern = '(<table.*?</table>)'
		table_list = re.findall(table_list_parttern, html_output, re.S | re.I | re.M)
		label = {}
		for count, table_item in enumerate(table_list):
			index_table_num = count + 1
			node = 'table_node_{}'.format(index_table_num) + '\n'
			html_output = html_output.replace(table_item, '<p>' + node + '</p>')
			table_item = re.sub('<img.*?>', '', table_item, re.S | re.I | re.M)
			label[node] = table_item

##########################################
#处理非lable的image

		for i in all_imgs:

			for k in pic_dict.keys():
				if k  in i:
					node = '#img_0_0_'+k.replace('Image_','').replace('.png','').replace('.jpg','').replace('.gif','')+'#'
					html_output = html_output.replace(i,node)
		pic_dict_new={}
		dict2sort=sorted(pic_dict.items(), key = lambda item : len(item[1]), reverse=True)#从大到小排序，保留最大。去除最小
		for j,k in enumerate(dict2sort):
			if j<=50:#图片大于50不保留
				node = '#img_0_0_'+k[0].replace('Image_','').replace('.png','').replace('.jpg','').replace('.gif','')+'#'
				pic_dict_new[node]=k[1]
			else:
				html_output= html_output.replace(node,'')
		web_contents = ''
		if html_output == '':
			pass
		else:
			xml = htmlparser.Parser(html_output)
			try:
				content_list = xml.xpathall('''//p | //table | //h1 | //h2 | //h3 | //h4 | //h5 | //h6''')
				for con in content_list:
					con = con.text().strip()
					if type(con) == bytes:
						con = str(con, encoding='utf8')
					if con and con!='%':#只有%忽略

						web_contents += ('\n\n\n' + con + '\n')
			except:
				pass
		return label,web_contents,pic_dict_new

	def main_execute(self):

		paths = self.getFiles()

		for path in paths:
			table_list = []
			label1={}
			label2={}
			path_pdf=path.split('/')[-1].replace('.htm','.pdf')
			label,web_contents,pic_dict = self.process(path)
			table_list.append(label)
			return label,web_contents,pic_dict
			# print(label)
			# print('===========================')
			# print(web_contents)
			# print('===========================')
			# print(pic_dict, '<<<<<<<<<')
			# if len(list(label.keys()))>10:
			# 	#print(len(list(label.keys())))
			# 	length_=math.ceil(len(list(label.keys()))/2)
			# 	label_list = list(self.chunks(label,length_))
			# 	label1=label_list[0]
			# 	label2=label_list[1]
			# else:
			# 	label1=label#如果表格少于10 作为label1写回mongo1
			uid_=list(select_mongo_obj.find({'gshf_content.file_name':path_pdf}))
			print(path_pdf)
			# if uid_!=[]:
			# 	uid_=uid_[0]['uid']
			# 	save_mongo_table_obj.save({'_id':uid_,'lable':label2})#将大的table存在另一个文件。将图片的base64格式也存于字典
			# 	try:
			# 		save_mongo_img_obj.save({'_id':uid_,'img':pic_dict})
			# 	except:
			# 		save_mongo_img_obj.save({'_id':uid_,'img':{}})
			# 		pass
			# m=update_mongo_obj.update({'gshf_content.file_name':path_pdf},{'$set':{'web_contents':web_contents,'label':label}})
			# print('操作mongo状态',m)
	def chunks(self,data,SIZE=2):
		it = iter(data)
		for i in range(0, len(data), SIZE):
			yield {k:data[k] for k in islice(it, SIZE)}
rewrite2monogo=Rewrite2Monogo(dir_html='/home/seeyii/szzq_html/htmlfile/',dir_pic='/home/seeyii/szzq_html/picfile/',file_name='NMK300364314021828HF')
if rewrite2monogo.main_execute():
	label,web_contents,pic_dict = rewrite2monogo.main_execute()
	print(label.keys())
	print('===========================')
	print(web_contents)
	print('===========================')
	print(pic_dict, '<<<<<<<<<')