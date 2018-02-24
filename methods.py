import helpers
import database_connection as database
import json
import boto3
import zipfile
import io
import pandas as pd
import logging
import datetime
import mysql_database
from pandas.io import sql
from sqlalchemy.exc import SQLAlchemyError

def etl_job():
  data = json.load(open('/home/ubuntu/ti_etl/secrets.json'))
  logger = helpers.setup_logging()
  s3_client = boto3.client('s3',aws_access_key_id=data['aws_access_key_id'],
                        aws_secret_access_key=data['aws_secret_access_key'])
  s3_resource = boto3.resource('s3',aws_access_key_id=data['aws_access_key_id'],
                        aws_secret_access_key=data['aws_secret_access_key'])
  keys = []
  resp = s3_client.list_objects_v2(Bucket='dealer-churn-analysis')
  for obj in resp['Contents']:
      keys.append(obj['Key'])
  for key in keys:
      names =  key.split("/")
      obj = s3_resource.Bucket('dealer-churn-analysis').Object(helpers.zip_file_name())
  file_name = 'praxis/etl/logs/log_{file}.txt'.format(file=helpers.date())
  obj_log = s3_resource.Bucket('dealer-churn-analysis').Object(file_name)
  buffer = io.BytesIO(obj.get()["Body"].read())
  zip_file = zipfile.ZipFile(buffer,'r')
  logger.info("Name of csv in zip file :%s",zip_file.namelist())
  logs = ""
  dataframe = pd.DataFrame()
  for name_of_zipfile in zip_file.namelist():
      zip_open = pd.read_csv(zip_file.open(name_of_zipfile))
      dataframe['created_at'] = pd.Series([datetime.datetime.now()] * len(zip_open))
      dataframe['last_updated_at'] = pd.Series([datetime.datetime.now()] * len(zip_open))
      zip_open = pd.concat([dataframe,zip_open], axis=1)
      zip_open = zip_open.dropna()
      table_name = "{name}_table".format(name=name_of_zipfile.replace('.csv',''))
      #print (zip_open)
      try :
        zip_open.to_sql(name=name_of_zipfile.replace('.csv',''), con=database.db_connection(), if_exists = 'append', index=False)
      except SQLAlchemyError as sqlalchemy_error:
        print (sqlalchemy_error)
        logs = '\n{table_name}\n{error}\n{logs}'.format(logs=logs,error=sqlalchemy_error,table_name=table_name)
        logger.error(" %s",sqlalchemy_error)
      database.db_connection().execute('SET FOREIGN_KEY_CHECKS=1;')
  end_time = datetime.datetime.now()
  logger.info("End time of program : %s",end_time)
  logs = '{logs} \nstart_time : {start_time} \nend_time : {end_time}'.format(start_time=helpers.start_time(),logs=logs,end_time=end_time)
  print (logs)
  obj_log.put(Body=logs)

result = etl_job()
