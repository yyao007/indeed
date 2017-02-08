# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.http import TextResponse
import random
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.conf import settings
from stem import Signal
from stem.control import Controller

class RandomUserAgentMiddleware(object):
	def process_request(self, request, spider):
		ua = random.choice(settings.get('USER_AGENT_LIST'))
		if ua:
			# print 'User-Agent: ', ua
			request.headers.setdefault('User-Agent', ua)

class ProxyMiddleware(object):
    def process_request(self, request, spider):
        request.meta['proxy'] = settings.get('HTTP_PROXY')
			

class ChangeProxyMiddleware(RetryMiddleware):
	def process_response(self, request, response, spider):
		t = TextResponse(request.url)
		footer = t.xpath('//p[@id="footer_nav_sec"]')
		print t
		if not footer:
			reason = 'IP is banned. Create a new Tor Circuit and retry.'
			self.new_circuit()
			return self._retry(request, reason, spider)
		else:
			return response
		
	def new_circuit(self):
		with Controller.from_port(port = 9151) as controller:
			controller.authenticate('931005')
			controller.signal(Signal.NEWNYM)
			print 'Done!'
	

			
class IndeedDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
