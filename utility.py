import re
import json
import boto.s3
import boto.sqs

keyre = re.compile('^AWSAccessKeyId=(.*)$')

def getKeys(file):
	with open(file,'r') as inf:
		hdr = inf.readline()
		
		# JSON format
		if hdr[0] == '{':
			inf.seek(0)
			return json.load(inf)
		
		# Root key format
		if keyre.match(hdr):
			out = dict()
			while hdr:
				parts = hdr.split('=')
				out[parts[0]] = parts[1].strip()
				hdr = inf.readline();
			return {
				'aws_access_key_id' : out['AWSAccessKeyId'], 
				'aws_secret_access_key' : out['AWSSecretKey']
			}
		# Colon format
		elif hdr[0] == '#':
			while hdr[0] == '#':
				hdr = inf.readline()
			out = dict()
			while hdr:
				parts = hdr.split(':')
				out[parts[0]] = parts[1].strip()
				hdr = inf.readline();
			return {
				'aws_access_key_id' : out['accessKeyId'], 
				'aws_secret_access_key' : out['secretKey']
			}
		
		# IAM format
		else:
			keys = inf.readline().split(',')
			return {
				'aws_access_key_id' : keys[1].strip(), 
				'aws_secret_access_key' : keys[2].strip()
			}


def get_bucket(file, region, bucketName):
	conn = boto.s3.connect_to_region(region, **getKeys(file))
	return conn.get_bucket(bucketName)


def get_queue(file, region, queueName):
	conn = boto.sqs.connect_to_region(region, **getKeys(file))
	return conn.get_queue(queueName)


def delete_all_messages(file, region, queueName):
	queue = get_queue(file, region, queueName)
	while (1):
		message = queue.read()
		if message:
			print 'Deleting message %s\n' % message
			message.delete()
		else:
			break
	print 'Finished deleting all messages in queue\n'


def delete_all_keys(file, region, bucketName):
	bucket = get_bucket(file, region, bucketName)
	for key in bucket.get_all_keys():
		print 'Deleting key %s \n' % key
		key.delete()
	print 'Finished deleting all objects in bucket\n'


def prepare_system(file, region, bucketName, queueName):
	delete_all_messages(file, region, queueName)
	delete_all_keys(file, region, bucketName)

