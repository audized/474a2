import json
import re
import random

import boto.sqs
from boto.sqs.message import Message

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

def test(file):
  sqs = boto.sqs.connect_to_region('us-west-2', **getKeys(file))
  qname = 'test_queue' + str(random.randint(0, 1e6))
  q = sqs.create_queue(qname)
  m = Message()
  m.set_body('hello')
  q.write(m)
  sqs.delete_queue(q)
  print "Test SUCCEEDED"
