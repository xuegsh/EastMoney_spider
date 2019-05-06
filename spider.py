# encoding: utf-8
import requests
import datetime
import json
import time
import csv
import re
from bs4 import BeautifulSoup




def getTable(page):
	params = {
		'type': 'SR',
		'sty': 'GGSR',
		'js': 'var HnlQDDXz={"data":[(x)],"pages":"(pc)","update":"(ud)","count":"(count)"}',
		'ps': 50,
		'p': page,
		'mkt': 0,
		'stat': 0,
		'cmd': 2,
		'code': '',
		'rt': 51875666
	}	

	url = 'http://datainterface.eastmoney.com//EM_DataCenter/js.aspx'
	response = requests.get(url, params=params)
	# print(response.text)
	return response.text


def getNumberOfPages():
	res = getTable(1)
	pattern = re.compile('\"pages\":\"(\d+)\"')
	num_of_pages = re.search(pattern, res).group(1)
	return num_of_pages


def parseTableContent(page): 
	# 提取出list，可以使用json.dumps和json.loads
	res = getTable(page)
	pattern = re.compile('var.*?=(.*)', re.S)
	data = re.search(pattern, res).group(1)
	data = json.loads(data)['data']
	
	# 获取研报html
	for d in data:
		r = requests.get('http://data.eastmoney.com/report/' + datetime.datetime.strptime(d['datetime'], '%Y-%m-%dT%H:%M:%S').strftime('%Y%m%d') +
			'/' + d['infoCode'] + '.html')

		soup = BeautifulSoup(r.text, 'lxml')
		# print((str(soup.select('.report-content .report-infos a'))))

		d['yb_content'] = str(soup.select('#ContentBody .newsContent')[0])
		d['yb_pdf'] = str(soup.select('.report-content .report-infos a')[1]['href'])


	# for i in range(5):
	# 	print(json.dumps(data[i], indent=4, separators=(',', ':'), ensure_ascii=False))

	writeTable(data, page, '研报') ################
	


# 写入表头
def writeHeader(category):
	with open('{}.csv' .format(category), 'a', encoding='utf_8_sig', newline='') as f:
		headers = ['代码', '日期', '名称', '研报标题', '原文评级', '评级变动', '机构', '2018收益', '2019收益', '2018市盈率', '2019市盈率', '研报PDF', '研报内容html']
		writer = csv.writer(f)
		writer.writerow(headers)


def writeTable(data, page, category):
	headers = ['secuFullCode', 'datetime', 'secuName', 'title', 'rate', 'change', 'insName', 'sys', 'syls', 'yb_pdf', 'yb_content']

	with open('{}.csv' .format(category), 'a', encoding='utf_8_sig', newline='') as f:
		w = csv.writer(f)

		for d in data:
			content = []
			for h in headers:
				if h == 'secuFullCode':
					tmp = d[h].split('.')
					content.append(tmp[1] + tmp[0])
				elif h == 'sys' or h == 'syls':
					content.append(d[h][0])
					content.append(d[h][1])
				else:
					content.append(d[h])

			w.writerow(content)


if __name__ == '__main__':
	num_of_pages = getNumberOfPages()
	writeHeader("研报")

	num_of_pages = 5
	for p in range(1, num_of_pages+1):
		print('正在下载第 %s 页表格' % p)
		start_time = time.time()
		
		parseTableContent(p)
		
		end_time = time.time() - start_time
		print('下载用时: {:.1f} s' .format(end_time))