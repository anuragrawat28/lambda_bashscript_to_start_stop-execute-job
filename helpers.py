import json
import datetime
import logging

def setup_logging():
	logger = logging.getLogger()
	logger.setLevel(logging.INFO)
	handler = logging.FileHandler('/home/ubuntu/ti_etl/logs.log')
	handler.setLevel(logging.INFO)
	# create a logging format
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	handler.setFormatter(formatter)
	# add the handlers to the logger
	logger.addHandler(handler)
	return logger

def start_time():
	start_time = datetime.datetime.now()
	return start_time

def date():
	date = start_time().strftime('%d')
	year = start_time().strftime('%Y')
	month = start_time().strftime('%m')
	now = '{year}_{month}_{date}'.format(year=year,date=date,month=month)
	return now

def zip_file_name():
	year = start_time().strftime('%Y')
	month = start_time().strftime('%m')
	zip_file_name = 'praxis/etl/data/{month}_{year}.zip'.format(year=year,month=month)
	return zip_file_name