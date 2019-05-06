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
		'type': 'SR',  # 研报
		'sty': 'HYSR', # 行业研报
		'sc': code,  # 行业代码
		'js': 'var HnlQDDXz={"data":[(x)],"pages":"(pc)","update":"(ud)","count":"(count)"}',
		'ps': 100,  # 每页条目数量
		'p': page,
		'mkt': 0,
		'stat': 0,
		# 'cmd': 2,
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


def parseTableContent(industry, code, page, cursor=None): 
	# 提取出list，可以使用json.dumps和json.loads
	res = getTable(code, page)
	pattern = re.compile('var.*?=(.*)', re.S)
	data = re.search(pattern, res).group(1)
	data = json.loads(data)['data'] # 这里的data是一个list [' ', ' ', ' ']
	

	# 获取研报html
	cnt = 0
	data_len = len(data)
	data_parsed = []
	while cnt < data_len:

		d = data[cnt].split(',')

		# print ('page %s, item %s' % (page, cnt+1))

		tmp = []
		tmp.append(datetime.datetime.strptime(d[1], '%Y/%m/%d %H:%M:%S').strftime('%Y%m%d')) # 报告日期
		tmp.append(d[7]) # 评级类别
		tmp.append(d[0]) # 评级变动
		
		if len(d[4]) >= 20:
			d[4] = d[4][0:19]
		tmp.append(d[4]) # 机构名称

		d[9] = d[9].replace('&sbquo;', ',').replace('&quot;', ',')
		if len(d[9]) >= 200:
			d[9] = d[9][0:199]
		tmp.append(d[9]) # 研报标题

		try:
			r = requests.get('http://data.eastmoney.com/report/' + tmp[0] + '/hy,' + d[2] + '.html', headers={'Connection':'close'})

			soup = BeautifulSoup(r.text, 'lxml')
			# print((str(soup.select('.report-content .report-infos a'))))

			tmp1 = soup.select('.report-content .report-infos a')
			if len(tmp1) >= 2:
				tmp.append(str(tmp1[1]['href']))  # 研报pdf链接
			else:
				tmp.append('')

			tmp2 = soup.select('#ContentBody .newsContent')
			if len(tmp2) >= 1:
				tmp.append(str(tmp2[0]))  # 研报内容html
			else:
				tmp.append('')

			cnt += 1
			data_parsed.append(tmp)
		
		except Exception as e:
			print ('【parseTableContent】获取page %s, item %s, link: %s失败' % (page, cnt+1, ('http://data.eastmoney.com/report/' + tmp[0] + '/hy,' + d[2] + '.html')))
			logging.exception(e)
			time.sleep(2)
			# tmp.append('') # 空的研报pdf链接
			# tmp.append('') # 空的研报内容html


	writeToMysql(data_parsed, industry + code, page, cursor)

	return


def writeToMysql(data, table_name, page, cursor):
	cnt = 0
	for d in data:
		cnt += 1

		sql = 'INSERT INTO ' + table_name + '(报告日期, 评级类别, 评级变动, 机构名称, 研报标题, 研报链接, 研报内容) values(%s, %s, %s, %s, %s, %s, %s)'
		try:
		    cursor.execute(sql, d)
		    db.commit()
		    # print('commit success!!!')
		except:
			print('【ROLLBACK】 %d %s !!!' % (cnt, d[4]))
			db.rollback()

	print ('第%s页写入完成,共%s条' % (page, len(data)))
	return


if __name__ == '__main__':

	# 创建数据库
	db = pymysql.connect(host='localhost', user='root', password='123456', port=3306)
	cursor = db.cursor()
	cursor.execute("CREATE DATABASE IF NOT EXISTS HYYB DEFAULT CHARACTER SET utf8")
	db.close()


	db = pymysql.connect(host='localhost', user='root', password='123456', port=3306, db='HYYB')
	cursor = db.cursor()

	with open('./industryList.txt', 'r') as f:
		for line in f.readlines():

			tmp = line.split()
			industry = tmp[0]
			code = tmp[1]
			num_of_pages = int(getNumberOfPages(code))
			print('\n正在获取' , line.strip(), ', 总页数：', num_of_pages)


			sql = 'CREATE TABLE IF NOT EXISTS ' + industry + code + \
			 	' (报告日期 VARCHAR(9), 评级类别 VARCHAR(20), 评级变动 VARCHAR(20), 机构名称 VARCHAR(20), 研报标题 VARCHAR(200), 研报链接 VARCHAR(60), 研报内容 TEXT)'
			cursor.execute(sql)
			cursor.execute('DELETE FROM ' + industry + code)
			
			for p in range(1, num_of_pages+1):
				
				parseTableContent(industry, code, p, cursor)

			
	db.close()