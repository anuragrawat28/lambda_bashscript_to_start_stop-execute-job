import helpers
import json
import sys
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

def db_connection():
  data = json.load(open('/home/ubuntu/ti_etl/secrets.json'))
  Base = declarative_base()
  logger = helpers.setup_logging()
  DB_URI = "mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
  try:
    engine = create_engine(DB_URI.format(user = data['user'],
      password = data['password'],
      port = data['port'],
      host = data['host'],
      db = data['db']),echo=True)
    engine.execute('SET FOREIGN_KEY_CHECKS=0;')
  except SQLAlchemyError as sqlalchemy_error:
    logger.error("%s",sqlalchemy_error)
    sys.exit()
  return engine