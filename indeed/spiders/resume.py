# -*- coding: utf-8 -*-
import scrapy
from indeed.items import IndeedItem
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class ResumeSpider(scrapy.Spider):
	name = "resume"
	allowed_domains = ["https://www.indeed.com/"]
	start_urls = ["https://www.indeed.com/resumes"]

	def parse(self, response):
		for url in self.getUrl():
			yield scrapy.Request(url, callback=self.parse_first, dont_filter=True)
			
	def parse_first(self, response):
		item = IndeedItem()
		location = response.xpath('//input[@id="location"]/@value').extract()[0]
		item['city'], item['state'] = location.replace(' ', '').split(',')
		table = response.xpath('//div[@id="refinements"]/ul')
		titles = table.xpath('@data-group-title').extract()
		if 'Locations' not in titles:
			response.meta['item'] = item
			yield self.parse_resume(response)
		else:
			pos = titles.index('Locations')
			locations = table[pos].xpath('li/a')
			for l in locations:
				lhs = l.xpath('text()').extract()[0].lower()
				rhs = location.lower()
				if lhs == rhs:
					url = l.xpath('@href').extract()[0]
					url = response.urljoin(url)
					request = scrapy.Request(url, callback=self.parse_resume, dont_filter=True)
					request.meta['item'] = item
					yield request
					
	def parse_resume(self, response):
		item = response.meta['item']
		total = response.xpath('//div[@id="result_count"]/text()').extract()
		if total:
			item['total'] = int(filter(unicode.isdigit, total[0]))
		table = response.xpath('//div[@id="refinements"]/ul')
		for t in table:
			title = t.xpath('@data-group-title').extract()[0]
			if 'Job' in title:
				item['job_titles'] = self.parse_table(t)
			elif 'Experience' in title:	
				item['work_experience'] = self.parse_table(t)
			elif 'Education' in title:
				item['education'] = self.parse_table(t)
			elif 'Companies' in title:
				item['company'] = self.parse_table(t)
		print item['city'], item['state'], item.get('total', '******************************************')
		return item
		
	def getUrl(self):
		urls = []
		connStr = 'mysql+mysqldb://root:931005@127.0.0.1/us'
		engine = create_engine(connStr, echo=False)
		DB_session = sessionmaker(bind=engine)
		session = DB_session()
		
		# WA
		
		result = session.execute("select distinct(Name), State from city").fetchall()
		for city, state in result:
			yield 'https://www.indeed.com/resumes?q=any&l='+ city.replace(' ', '+') +'%2C+'+ state + '&co=US&radius=0'
		
	def parse_table(self, table):
		li = table.xpath('li/a/@title').extract()
		item = {}
		for l in li:
			first, second = l.split('(')
			item[first.strip()] = int(filter(unicode.isdigit, second))
		return item