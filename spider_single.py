# encoding: utf-8
import requests
import datetime
import logging
import pymysql
import json
import time
import csv
import re
from bs4 import BeautifulSoup




def getTable(code, page):
	params = {
		'type': 'SR',
		'sty': 'GGSR',
		'js': 'var HnlQDDXz={"data":[(x)],"pages":"(pc)","update":"(ud)","count":"(count)"}',
		'ps': 100,  # 每页条目数量
		'p': page,
		# 'mkt': 0,
		# 'stat': 0,
		# 'cmd': 2,
		'code': code,
		'rt': 51878147
	}	

	url = 'http://datainterface.eastmoney.com//EM_DataCenter/js.aspx'
	
	while True:
		try:
			response = requests.get(url, params=params)
			break
		except Exception as e:
			print ('【getTable】获取%s失败' % url)
			logging.exception(e)
			time.sleep(3)

		
	return response.text


def getNumberOfPages(code):
	res = getTable(code, 1)
	pattern = re.compile('\"pages\":\"(\d+)\"')
	result = re.search(pattern, res)

	if result:
		num_of_pages = result.group(1)
	else:
		num_of_pages = 0
	return num_of_pages


def parseTableContent(prefix, code, page, cursor): 
	# 提取出list，可以使用json.dumps和json.loads
	res = getTable(code, page)
	pattern = re.compile('var.*?=(.*)', re.S)
	data = re.search(pattern, res).group(1)
	data = json.loads(data)['data']
	
	# 获取研报html
	cnt = 0
	data_len = len(data)
	while cnt < data_len:

		# print ('page %s, item %s' % (page, cnt+1))
		d = data[cnt]

		datetime_tmp = datetime.datetime.strptime(d['datetime'], '%Y-%m-%dT%H:%M:%S').strftime('%Y%m%d')

		try:
			r = requests.get('http://data.eastmoney.com/report/' + datetime_tmp + '/' + d['infoCode'] + '.html')

			soup = BeautifulSoup(r.text, 'lxml')
			# print((str(soup.select('.report-content .report-infos a'))))

			d['datetime'] = datetime_tmp

			if len(d['insName']) >= 20:
				d['insName'] = d['insName'][0:19]

			if len(d['title']) >= 200:
				d['title'] = d['title'][0:199]
	 
			d['yb_pdf'] = ''
			d['yb_content'] = ''

			tmp1 = soup.select('.report-content .report-infos a')
			if len(tmp1) >= 2:
				d['yb_pdf'] = str(tmp1[1]['href'])

			tmp2 = soup.select('#ContentBody .newsContent')
			if len(tmp2) >= 1:
				d['yb_content'] = str(tmp2[0])

			data[cnt] = d
			
			cnt += 1

		except Exception as e:
			print ('【parseTableContent】获取page %s, item %s, link: %s失败' % (page, cnt+1, ('http://data.eastmoney.com/report/' + datetime_tmp + '/' + d['infoCode'] + '.html')))
			logging.exception(e)
			time.sleep(2)

	# for i in range(len(data)):
	# 	print(json.dumps(data[i], indent=4, separators=(',', ':'), ensure_ascii=False))

	# writeTable(data, page, "研报" + str(code))
	writeToMysql(data, prefix + code, page, cursor)
	


# 写入表头
def writeHeader(category):
	with open('{}.csv' .format(category), 'a', encoding='utf_8_sig', newline='') as f:
		headers = ['代码', '报告日期', '评级类别', '评级变动', '机构名称', '研报标题', '研报PDF', '研报内容html']
		writer = csv.writer(f)
		writer.writerow(headers)


def writeTable(data, category):
	headers = ['secuFullCode', 'datetime', 'rate', 'change', 'insName', 'title', 'yb_pdf', 'yb_content']

	with open('{}.csv' .format(category), 'a', encoding='utf_8_sig', newline='') as f:
		w = csv.writer(f)

		for d in data:
			content = []
			for h in headers:
				if h == 'secuFullCode':
					tmp = d[h].split('.')
					content.append(tmp[1] + tmp[0])
				else:
					content.append(d[h])

			w.writerow(content)



def writeToMysql(data, table_name, page, cursor):
	headers = ['datetime', 'sratingName', 'change', 'insName', 'title', 'yb_pdf', 'yb_content']

	cnt = 0
	for d in data:
		cnt += 1
		content = []
		for h in headers:
			content.append(d[h])

		sql = 'INSERT INTO ' + table_name + '(报告日期, 评级类别, 评级变动, 机构名称, 研报标题, 研报链接, 研报内容) values(%s, %s, %s, %s, %s, %s, %s)'
		try:
		    cursor.execute(sql, content)
		    db.commit()
		    # print('commit success!!!')
		except:
			print('【ROLLBACK】 %d %s !!!' % (cnt, d['title']))
			db.rollback()

	print ('第%s页写入完成,共%s条' % (page, len(data)))
	return





if __name__ == '__main__':

	# 创建数据库
	db = pymysql.connect(host='localhost', user='root', password='123456', port=3306)
	cursor = db.cursor()
	cursor.execute("CREATE DATABASE IF NOT EXISTS GGYB DEFAULT CHARACTER SET utf8")
	db.close()

	# writeHeader("研报" + str(code))

	db = pymysql.connect(host='localhost', user='root', password='123456', port=3306, db='GGYB')
	cursor = db.cursor()

	with open('./stockList.txt', 'r') as f:
		for line in f.readlines():

			tmp = line.strip()
			prefix = tmp[0:2]
			code = tmp[2:]
			num_of_pages = int(getNumberOfPages(code))
			print('\n正在获取' , line.strip(), ', 总页数：', num_of_pages)


			sql = 'CREATE TABLE IF NOT EXISTS ' + prefix + code + \
			 	' (报告日期 VARCHAR(9), 评级类别 VARCHAR(20), 评级变动 VARCHAR(20), 机构名称 VARCHAR(20), 研报标题 VARCHAR(200), 研报链接 VARCHAR(60), 研报内容 TEXT)'
			cursor.execute(sql)
			cursor.execute('DELETE FROM ' + prefix + code)
			
			for p in range(1, num_of_pages+1):
				# print('正在下载第 %s 页表格' % p)
				# start_time = time.time()
				
				parseTableContent(prefix, code, p, cursor)
				
				# end_time = time.time() - start_time
				# print('下载用时: {:.1f} s' .format(end_time))
			
			
	db.close()
