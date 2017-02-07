# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class IndeedItem(scrapy.Item):
	# define the fields for your item here like:
	city = scrapy.Field()
	state = scrapy.Field()
	job_titles = scrapy.Field()
	work_experience = scrapy.Field()
	education = scrapy.Field()
	company = scrapy.Field()
	total = scrapy.Field()
	
