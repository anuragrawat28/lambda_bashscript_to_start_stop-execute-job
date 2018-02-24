import boto3
import botocore
import paramiko
import json

ec2_credentials = json.load(open('aws_ec2_credentials.json'))
key = paramiko.RSAKey.from_private_key_file('praxis-key-pair.pem')
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

def start_instance():
    try:
    	instances = []
    	instances.append(ec2_credentials['instances'])
        ec2 = boto3.client('ec2', region_name=ec2_credentials['region'])
        ec2.stop_instances(InstanceIds=instances)
        print 'stopped your instances: ' + str(instances)
    except Exception, e:
        raise e

def lambda_handler(event, context):
	try:
		stop_instance()
	except Exception, e:
	    print e
	return "success"
