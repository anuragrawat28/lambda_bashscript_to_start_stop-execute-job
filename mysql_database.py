import json
import urllib
import boto3
import zipfile
import io
import pandas as pd
import sys
import logging
import re
import datetime
import helpers
import database_connection as database
from pandas.io import sql
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData, TEXT, Integer, Table, Column, ForeignKey , String , TIMESTAMP, BOOLEAN

def create_tables():
  logger = helpers.setup_logging()
  logger.info("Start time of program :%s",helpers.start_time())
  db = database.db_connection()
  meta = MetaData(bind=db)
  logger.info("INFO: Creating tables schema")
  try:
    table_realm_type = Table('realm_type',meta,
    	Column('id',Integer,primary_key=True,autoincrement=True),
    	Column('name',TEXT(50), nullable=True),
    	Column('category' ,TEXT(50), nullable=True),
    	Column('is_active' ,BOOLEAN, nullable=True),
      Column('created_at', TIMESTAMP, onupdate=datetime.datetime.now),
      Column('last_updated_at', TIMESTAMP, onupdate=datetime.datetime.now)
    )

    table_category = Table('category',meta,
        Column('id',Integer,primary_key=True, autoincrement=True),
        Column('name' ,TEXT(50), nullable=True),
        Column('is_active' ,BOOLEAN, nullable=True),
        Column('created_at', TIMESTAMP, onupdate=datetime.datetime.now),
        Column('last_updated_at', TIMESTAMP, onupdate=datetime.datetime.now)
    )

    table_dealer_master = Table('dealer_master', meta,
        Column('dealer_id',Integer,primary_key=True, autoincrement=False),
        Column('dealer_name' ,TEXT(255),nullable=True),
        Column('realm_name',TEXT(50), nullable=True),
        Column('customer_id' ,Integer, nullable=True),
        Column('customer_name' ,TEXT(255),nullable=True),
        Column('category_name' ,TEXT(50), nullable=True),
        Column('city' ,TEXT(255),nullable=True),
        Column('state_code' ,TEXT(10),nullable=False),
        Column('zip_code' ,Integer,nullable=True),
        Column('country_code' ,TEXT(10),nullable=False),
        Column('area_code' ,Integer,nullable=True),
        Column('start_date' ,TIMESTAMP,nullable=True),
        Column('expire_date' ,TIMESTAMP,nullable=True),
        Column('created_at', TIMESTAMP, onupdate=datetime.datetime.now),
        Column('last_updated_at', TIMESTAMP, onupdate=datetime.datetime.now)
    )

    table_ads_data = Table('ads_data', meta,
        Column('ad_id', Integer, primary_key=True, autoincrement=False),
        Column('dealer_id', Integer, ForeignKey('dealer_master.dealer_id')),
        Column('make_id', Integer, nullable=True),
        Column('make_name', TEXT, nullable=True),
        Column('ad_status', TEXT, nullable=True),
       	Column('created_at', TIMESTAMP,nullable=True),
       	Column('last_updated_at', TIMESTAMP,nullable=True),
       	Column('expire_date', TIMESTAMP,nullable=True),
       	Column('create_date', TIMESTAMP,nullable=True),
        Column('created_at', TIMESTAMP, onupdate=datetime.datetime.now),
        Column('last_updated_at', TIMESTAMP, onupdate=datetime.datetime.now)
    )

    table_rep_master = Table('rep_master', meta,
        Column('rep_id' ,Integer, primary_key=True, autoincrement=False),
        Column('rep_name' ,TEXT(255),nullable=True),
        Column('is_active' ,BOOLEAN, nullable=True),
        Column('created_at', TIMESTAMP, onupdate=datetime.datetime.now),
        Column('last_updated_at', TIMESTAMP, onupdate=datetime.datetime.now)
    )

    table_ManagerMaster = Table('manager_master', meta,
        Column('manager_id' ,Integer, primary_key=True, autoincrement=False),
        Column('manager_name' ,TEXT(255),nullable=True),
        Column('is_active' ,BOOLEAN, nullable=True),
        Column('created_at', TIMESTAMP, onupdate=datetime.datetime.now),
        Column('last_updated_at', TIMESTAMP, onupdate=datetime.datetime.now)
    )

    table_DealerRepManagerMapping = Table('dealer_rep_manager_mapping', meta,
        Column('id' ,Integer, primary_key=True, autoincrement=True),
        Column('dealer_id' ,Integer, ForeignKey('dealer_master.dealer_id')),
        Column('rep_id' ,Integer, ForeignKey('rep_master.rep_id')),
        Column('manager_id' ,Integer, ForeignKey('manager_master.manager_id')),
        Column('created_at', TIMESTAMP, onupdate=datetime.datetime.now),
        Column('last_updated_at', TIMESTAMP, onupdate=datetime.datetime.now)
    )

    table_Billing  = Table('billing_data', meta,
        Column('dealer_id' ,Integer, ForeignKey('dealer_master.dealer_id')),
        Column('billing_id' ,Integer, primary_key=True, autoincrement=False),
        Column('bill_amount' ,Integer, nullable=True),
        Column('billing_month' ,Integer, nullable=True),
        Column('billing_year' ,Integer, nullable=True),
        Column('payment_method' ,TEXT(255),nullable=True),
        Column('package_name' ,TEXT(255),nullable=True),
        Column('dealer_change_status' ,TEXT(2), default='A'),
        Column('billing_change_previous_month' ,Integer, nullable=True),
        Column('billing_date' ,TIMESTAMP,nullable=True),
        Column('created_at', TIMESTAMP, onupdate=datetime.datetime.now),
        Column('last_updated_at', TIMESTAMP, onupdate=datetime.datetime.now)
    )

    table_CoopData = Table('co_op_data', meta,
    	  Column('id' ,Integer, primary_key=True, autoincrement=True),
        Column('dealer_id' ,Integer, ForeignKey('dealer_master.dealer_id')),
        Column('co_op_month' ,Integer, nullable=True),
        Column('co_op_year' ,Integer, nullable=True),
        Column('co_op_flag' ,BOOLEAN, nullable=True),
        Column('created_at', TIMESTAMP, onupdate=datetime.datetime.now),
        Column('last_updated_at', TIMESTAMP, onupdate=datetime.datetime.now)
    )

    meta.create_all(db)
  except SQLAlchemyError as sqlalchemy_error:
    logger.error("ERROR: %s",sqlalchemy_error)
    sys.exit()
  logger.info("SUCCESS: Tables created")
  insert_to_realm = table_realm_type.insert()
  try:
    insert_to_realm.execute({'id':1,'name':'CYCLE', 'category':'COMMERCIAL','is_active':1},
                            {'id':2,'name':'RV', 'category':'COMMERCIAL','is_active':1},
                            {'id':3,'name':'TRUCK', 'category':'RECREATIONAL','is_active':1},
                            {'id':4,'name':'EQUIPMENT', 'category':'RECREATIONAL','is_active':1})
  except SQLAlchemyError as err:
    print (err)
    logger.error("ERROR: %s",err)
  logger.info("SUCCESS: Inserted values")
  return meta

create_tables()
