#!/usr/bin/env python
"""
This script facilitates using Liquid Web's Object Storage for cPanel/WHM backups.
"""

### Import things ###
import sys, os, boto, math, logging, time, os.path, traceback
from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
from boto.s3.key import Key
from filechunkio import FileChunkIO

### Get the command line args ###
command, pwd = sys.argv[1:3]
cmdParams = sys.argv[3:-2]
host, userName = sys.argv[-2:]

### Object Storage Connection Setup ###
objStoreHost = 'objects.liquidweb.services'
accessKey = userName
secretKey = os.environ['PASSWORD']
bucketName = host;

objStoreConn = S3Connection(
	aws_access_key_id = accessKey,
	aws_secret_access_key = secretKey,
	host = objStoreHost,
	calling_format = boto.s3.connection.OrdinaryCallingFormat()
)

objStoreBucket = objStoreConn.get_bucket(bucketName)

key = Key(objStoreBucket)

### Set up logging
logFileName = 'cpBackupToObjStore - ' + time.strftime('%Y-%m-%d') + '.log'
logDir = 'log'
if not os.path.isdir(logDir): # I like to live dangeriously, just checks for the existance of the log directory by checking if it *is* a directory. Theoretically, the log directory could be a file, so..
	os.mkdir(logDir)
logPath = os.path.join(logDir, logFileName)
logging.basicConfig(filename = logPath, level=logging.INFO)

def logTime():
	return time.strftime('%Y-%m-%d %H:%M:%S')

### Functions by command ###

def get(remoteFile, localFile):
	key.key = remoteFile
	key.get_contents_to_filename(localFile)

def put(localFile, remoteFile):

	fileSize = os.stat(localFile).st_size

	if fileSize < 1000000000: # 1G - in decimal - yes, it's arbitrary
		logging.info(logTime() + ' :: Sending the file in one chunk')
		key.key = remoteFile
		key.set_contents_from_filename(localFile)
	else:
		logging.info(logTime() + ' :: Sending file in multiple parts')
		multiPart = objStoreBucket.initiate_multipart_upload(remoteFile)

		# Establish chunk size and count
		chunkSize = 100000000 # 100M Decimal
		chunkCount = int(math.ceil(fileSize / float(chunkSize)))

		# Send the parts
		for i in range(chunkCount):
			logMsg = logTime() + ' :: Sending chunk ' + str(i + 1) + ' of ' + str(chunkCount)
			logging.info(logMsg)

			offset = chunkSize * i
			byteCount = min(chunkSize, fileSize - offset)

			with FileChunkIO(localFile, 'r', offset = offset, bytes = byteCount) as fp:
				multiPart.upload_part_from_file(fp, part_num = i + 1)

		# Finish the send
		logging.info(logTime() + ' :: Combining file parts')
		multiPart.complete_upload()

def ls(path):
	keyList = objStoreBucket.get_all_keys(prefix = path[1:]) # The slice is to knock off the first / since this won't exist in the key
	for key in keyList:
		keyName = key.name.split('/')[-1]
		keySize	= key.size
		keyDate = key.last_modified
		keyMeta = key.metadata

		print str(keySize) + '\t' + keyDate + '\t' + keyName

def mkdir(path):
	pass # You don't need to 'make' a directory in Object Storage since it's automagically created via the key on file upload

def chdir(path):
	print path

def rmdir(path):
	keyList = objStoreBucket.get_all_keys(prefix = path[1:])
	for key in keyList:
		key.delete()

def delete(path):
	key.key = path
	key.delete()

### End Function Defs ###


### Call and log the command passed in
logMsg = logTime() + ' :: ' + command + ' operation called. Params: ' + str(cmdParams) + ' Bucket: ' + bucketName
logging.info(logMsg)

try:
	globals()[command](*cmdParams)
except:
	e = sys.exc_info()
	excHeader = logTime() + ' :: ' + str(e[0])
	logging.critical(excHeader)

	for x in traceback.extract_tb(e[2]):
		logLine = logTime() + ' :: ' + x[0] + ': ' + str(x[1]) + ' - ' + x[2] + ' - ' + x[3]
		logging.critical(logLine)
