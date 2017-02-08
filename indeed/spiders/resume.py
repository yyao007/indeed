# -*- coding: utf-8 -*-
import scrapy
from indeed.items import IndeedItem
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from stem import Signal
from stem.control import Controller
	
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
		item['city'], item['state'] = [x.strip() for x in location.split(',')]
		table = response.xpath('//div[@id="refinements"]/ul')
		titles = table.xpath('@data-group-title').extract()
		if 'Locations' not in titles:
			response.meta['item'] = item
			yield self.parse_resume(response)
		else:
			pos = titles.index('Locations')
			locations = table[pos].xpath('li/a')
			for l in locations:
				rhs = l.xpath('text()').extract()[0].lower()
				lhss = item['city'].lower().split()
				for lhs in lhss:
					if lhs in rhs:
						url = l.xpath('@href').extract()[0]
						url = response.urljoin(url)
						request = scrapy.Request(url, callback=self.parse_resume, dont_filter=True)
						request.meta['item'] = item
						yield request
						return
			response.meta['item'] = item
			yield self.parse_resume(response)
					
	def parse_resume(self, response):
		footer = response.xpath('//p[@id="footer_nav_sec"]')
		if not footer:
			print 'IP is banned. Create a new Tor Circuit and retry...'
			self.new_circuit()
			return scrapy.Request(response.url, callback=self.parse_resume, dont_filter=True)
		else:	
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
			if not item.get('total'):
				print response.url, footer.xpath('@id').extract()
			print item['city'], item['state'], item.get('total', '******************************************')
			return item

	def new_circuit(self):
		with Controller.from_port(port = 9151) as controller:
			controller.authenticate('931005')
			controller.signal(Signal.NEWNYM)
			print 'Done!'
	
	def getUrl(self):
		urls = []
		connStr = 'mysql+mysqldb://root:931005@127.0.0.1/us'
		engine = create_engine(connStr, echo=False)
		DB_session = sessionmaker(bind=engine)
		session = DB_session()
		exist = session.execute("SELECT DISTINCT(state) FROM resumes").fetchall()
		exist = [i[0] for i in exist]
		not_complete = session.execute('select t1.state from (select count(distinct(name)) cities, state from city group by state order by state) as t1 left join (select count(*) cities, state from resumes group by state) as t2 on t1.state=t2.state where t2.cities is not NULL and t1.cities!=t2.cities').fetchall()
		states = [i[0] for i in not_complete]
		stateStr = ''
		for i, s in enumerate(states):
			stateStr += 'state=\'' + s + '\''
			if i < len(states)-1:
				stateStr += ' or '
		
		result = session.execute("select t1.name, t1.state from (select distinct(name), state from city where %s) as t1 left join resumes as t2 on (t1.name=t2.city and t1.state=t2.state) where t2.city is NULL" %(stateStr)).fetchall()
		# result = session.execute("SELECT DISTINCT(Name), State FROM city").fetchall()
		for city, state in result:
			# if state not in exist:
				# yield 'https://www.indeed.com/resumes?q=any&l='+ city.replace(' ', '+') +'%2C+'+ state + '&co=US&radius=0'
			yield 'https://www.indeed.com/resumes?q=any&l='+ city.replace(' ', '+') +'%2C+'+ state + '&co=US&radius=0'
		
	def parse_table(self, table):
		li = table.xpath('li/a/@title').extract()
		item = {}
		for l in li:
			first, second = l.split('(')
			item[first.strip()] = int(filter(unicode.isdigit, second))
		return item