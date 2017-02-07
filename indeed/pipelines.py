# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import CHAR, Integer, String, DateTime
from sqlalchemy.sql import func
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
class Resumes(Base):
    __tablename__ = 'resumes'
    
    id = Column(Integer, primary_key=True)
    city = Column(String(50))
    state = Column(String(2))
    total = Column(INTEGER(unsigned=True))
    y0_1 = Column(INTEGER(unsigned=True))
    y1_2 = Column(INTEGER(unsigned=True))
    y3_5 = Column(INTEGER(unsigned=True))
    y6_10 = Column(INTEGER(unsigned=True))
    y10_ = Column(INTEGER(unsigned=True))
    doctor = Column(INTEGER(unsigned=True))
    master = Column(INTEGER(unsigned=True))
    bachelor = Column(INTEGER(unsigned=True))
    associate = Column(INTEGER(unsigned=True))
    diploma = Column(INTEGER(unsigned=True))
    crawl_time = Column(DateTime(timezone=True), server_default=func.now())

class Jobs(Base):
    __tablename__ = 'jobs'
    
    rid = Column(Integer, ForeignKey('resumes.id', ondelete='CASCADE'), primary_key=True)
    title = Column(String(1000), primary_key=True)
    amount = Column(INTEGER(unsigned=True))
    
class Companies(Base):
    __tablename__ = 'companies'
    
    rid = Column(Integer, ForeignKey('resumes.id', ondelete='CASCADE'), primary_key=True)
    name = Column(String(1000), primary_key=True)
    amount = Column(INTEGER(unsigned=True))
    
class IndeedPipeline(object):
    def open_spider(self, spider):
        connStr = 'mysql+mysqldb://root:931005@127.0.0.1/us'
        self.engine = create_engine(connStr, convert_unicode=True, echo=False)
        self.DB_session = sessionmaker(bind=self.engine)
        self.session = self.DB_session()
        Base.metadata.create_all(self.engine)
        self.count = 0
        
    def close_spider(self, spider):
        self.session.commit()
        self.session.close()
        self.engine.dispose()
    
    def process_item(self, item, spider):
        # Commit session every 100 items
        self.count += 1
        if self.count % 100 == 0:
            self.session.commit()
            
        resume = self.create_resume(item)
        self.session.add(resume)
        self.session.flush()
        jobs = self.create_jobs(item, resume.id)
        companies = self.create_companies(item, resume.id)
        self.session.add_all(jobs + companies)
        self.session.flush()
        
        return item
        
    def create_resume(self, item):
        resume = Resumes(city=item.get('city'), state=item.get('state'), total=item.get('total'))
        work_experience = item.get('work_experience', [])
        education = item.get('education', [])
        
        for k in work_experience:
            if 'Less' in k:
                resume.y0_1 = work_experience[k]
            elif '1-2' in k:
                resume.y1_2 = work_experience[k]
            elif '3-5' in k:
                resume.y3_5 = work_experience[k]
            elif '6-10' in k:
                resume.y6_10 = work_experience[k]
            elif 'More' in k:
                resume.y10_ = work_experience[k]
        
        for k in education:
            if 'Doc' in k:
                resume.doctor = education[k]
            if 'Mas' in k:
                resume.master = education[k]
            if 'Bac' in k:
                resume.bachelor = education[k]
            if 'Ass' in k:
                resume.associate = education[k]
            if 'Dip' in k:
                resume.diploma = education[k]
        
        return resume

    def create_jobs(self, item, rid):
        job_titles = item.get('job_titles', [])
        jobs = []
        for t in job_titles:
            jobs.append(Jobs(rid=rid, title=t, amount=job_titles[t]))
            
        return jobs
        
    def create_companies(self, item, rid):
        company = item.get('company', [])
        companies = []
        for c in company:
            companies.append(Companies(rid=rid, name=c, amount=company[c]))
        
        return companies
        
        
class DuplicatesPipeline(object):
	def __init__(self):
		self.cities = set()
		
	def process_item(self, item, spider):
		city = (item.get('city'), item.get('state'))
		if city in self.cities:
			raise DropItem("Duplicate item found: %s" % str(city))
		else:
			self.cities.add(city)
			return item
		
		
        
        
        
        
        
        
        
        