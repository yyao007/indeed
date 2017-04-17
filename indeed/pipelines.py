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
from stem import Signal
from stem.control import Controller

Base = declarative_base()
class Resumes(Base):
    __tablename__ = 'resume'
    
    # id = Column(Integer, primary_key=True)
    city = Column(String(50), primary_key=True)
    state = Column(String(2), primary_key=True)
    crawl_time = Column(DateTime(timezone=True), primary_key=True, server_default=func.now())
    total = Column(INTEGER(unsigned=True))
    y0_1 = Column(INTEGER)
    y1_2 = Column(INTEGER)
    y3_5 = Column(INTEGER)
    y6_10 = Column(INTEGER)
    y10_ = Column(INTEGER)
    doctor = Column(INTEGER)
    master = Column(INTEGER)
    bachelor = Column(INTEGER)
    associate = Column(INTEGER)
    diploma = Column(INTEGER)
    

class Jobs(Base):
    __tablename__ = 'resumeJobs'
    
    # rid = Column(Integer, ForeignKey('resumes.id', ondelete='CASCADE'), primary_key=True)
    city = Column(String(50), primary_key=True)
    state = Column(String(2), primary_key=True)
    crawl_time = Column(DateTime(timezone=True), primary_key=True, server_default=func.now())
    jobTitle = Column(String(500), primary_key=True)
    jobCount = Column(INTEGER(unsigned=True))
    
class Companies(Base):
    __tablename__ = 'resumeCompanies'
    
    # rid = Column(Integer, ForeignKey('resumes.id', ondelete='CASCADE'), primary_key=True)
    city = Column(String(50), primary_key=True)
    state = Column(String(2), primary_key=True)
    crawl_time = Column(DateTime(timezone=True), primary_key=True, server_default=func.now())
    companyName = Column(String(500), primary_key=True)
    companyCount = Column(INTEGER(unsigned=True))
    
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
        # self.count += 1
            
        # if self.count % 150 == 0:
            # self.new_circuit()
        resume = self.create_resume(item)
        jobs = self.create_jobs(item, resume.city, resume.state)
        companies = self.create_companies(item, resume.city, resume.state)
        # try:
        self.session.add(resume)
        self.session.add_all(jobs + companies)
        self.session.commit()
        # # except:
            # raise
            # self.session.rollback()
            # print "rollback..."   
        # finally:
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

    def create_jobs(self, item, c, s):
        job_titles = item.get('job_titles', [])
        titles_seen = set()
        jobs = []
        for t in job_titles:
            if t.lower() not in titles_seen:
                if len(t) > 500:
                    print 'Job title too long: ', t
                    continue   
                titles_seen.add(t.lower())
                jobs.append(Jobs(city=c, state=s, jobTitle=t, jobCount=job_titles[t]))
            else:
                print 'Duplicate job title found:', t               
        return jobs
        
    def create_companies(self, item, c, s):
        company = item.get('company', [])
        companies_seen = set()
        companies = []
        for comp in company:
            if comp.lower() not in companies_seen: 
                if len(comp) > 500:
                    print 'Compny name too long: ', comp
                    continue
                companies_seen.add(comp.lower())
                companies.append(Companies(city=c, state=s, companyName=comp, companyCount=company[comp]))
            else:
                print 'Duplicate company found:', comp
        return companies
    
    def new_circuit(self):
        with Controller.from_port(port = 9151) as controller:
            print 'Changeing IP...',
            controller.authenticate('931005')
            controller.signal(Signal.NEWNYM)
            print 'Done!' 
        
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
        
        
        
        
        
        
        
        
        
        