#!/usr/bin/env python
"""
This script facilitates using Liquid Web's Object Storage for cPanel/WHM backups.
"""

### Import things ###
import sys, os, boto, math, logging, time, os.path, traceback, isodate
from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
from boto.s3.key import Key
from filechunkio import FileChunkIO
from Queue import Queue
from threading import Thread
from tzlocal import get_localzone

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

### Setup threading
threadList = [] # So we can iterate against this and setup the join.
THREADS = 4 # Adjust as needed

class chunkWorker(Thread):
	def __init__(self, queue):
		self.queue = queue
		Thread.__init__(self)
	def run(self):
		while not self.queue.empty():
			jobData = self.queue.get()

			multiPart = jobData['multiPart']
			fp = jobData['fp']
			part_num = jobData['part_num']
			chunkCount = jobData['chunkCount']

			logMsg = logTime() + ' :: Sending chunk ' + str(part_num) + ' of ' + str(chunkCount)
			logging.info(logMsg)
			multiPart.upload_part_from_file(fp, part_num)

		return

### Functions by command ###

def get(remoteFile, localFile):
	key.key = remoteFile
	key.get_contents_to_filename(localFile)

def put(localFile, remoteFile):
	t1 = time.time()

	fileSize = os.stat(localFile).st_size

	if fileSize < 1000000000: # 1G - in decimal - yes, it's arbitrary
		logging.info(logTime() + ' :: Sending the file in one chunk')
		key.key = remoteFile
		key.set_contents_from_filename(localFile)

	else:
		logging.info(logTime() + ' :: Sending file in multiple parts using ' + str(THREADS) + ' threads')
		multiPart = objStoreBucket.initiate_multipart_upload(remoteFile)

		# Establish chunk size and count
		chunkSize = 100000000 # 100M Decimal
		chunkCount = int(math.ceil(fileSize / float(chunkSize)))

		# Build queue and queue the parts
		partQueue = Queue(maxsize = 0)

		for i in range(chunkCount):
			

			offset = chunkSize * i
			byteCount = min(chunkSize, fileSize - offset)

			fp = FileChunkIO(localFile, 'r', offset = offset, bytes = byteCount)
			queueData = {
				'fp': fp,
				'part_num': i + 1,
				'multiPart': multiPart,
				'chunkCount': chunkCount
			}
			
			partQueue.put(queueData)

		# Run the queue
		for i in range(THREADS):
			t = chunkWorker(partQueue)
			threadList.append(t)
			t.start()

		for t in threadList:
			t.join()

		# Finish the send
		logging.info(logTime() + ' :: Combining file parts')
		multiPart.complete_upload()

	t2 = time.time()
	logging.info(logTime() + ' :: ' + str(t2 - t1) + ' seconds to send ' + localFile)

def ls(path):
	#
	# If the end of the path contains a slash, it will break. I'll fix this later if needed
	#

	subDirList = {}
	lsData = {}
	sizeLength = 0 # This will be used to determine the width of the size column
	fileLength = 0 # This will be used to determine the width of the file column
	dateLength = 1 # This will be used to determin the width of the day column

	if path[0] is '/':
		keyList = objStoreBucket.get_all_keys(prefix = path[1:]) # The slice is to knock off the first / since this won't exist in the key
		pathComponent = path.split('/')[1:] # Split the path into components. The slice is because the first element is empty from the split
	else:
		keyList = objStoreBucket.get_all_keys(prefix = path)
		pathComponent = path.split('/')

	prefixSize = len(pathComponent) # Get the length so we know where to start looking at the key name

	for key in keyList:
		keySplit = key.name.split('/')[prefixSize:] # Get rid of the leading prefix stuff
		
		### Key modification time stuff - common to 'subdirectories' and 'real' files
		keyDate = isodate.parse_datetime(key.last_modified).astimezone(get_localzone())

		keyTimeData = {}

		keyTimeData['month']	= keyDate.strftime('%b')

		if keyDate.strftime('%d')[0] is not '0': # Double digit day
			keyTimeData['day']		= keyDate.strftime('%d')
		else: # Single digit day - doing this to match ls -l mind you
			keyTimeData['day']		= keyDate.strftime('%d')[1]

		keyTimeData['time']		= keyDate.strftime('%H:%M')

		## Other common stuff
		owner = 'root'
		group = 'root'

		### Directory check
		### Since there are no directories, per se, in Object Storage/S3, emulate subdirectories where appropriate
		if len(keySplit) > 1: # We have a 'file in a subdirectory' if this is > 1
			if keySplit[0] not in subDirList:
				sdir = subDirList[keySplit[0]] = {}
				for k, v in keyTimeData.iteritems():
					sdir[k] = v
				sdir['size'] = '4096'
				sdir['perms'] = 'drwx------'
				sdir['owner'] = owner
				sdir['group'] = group
				sdir['hardcnt'] = '2'
				sdir['file'] = keySplit[0]

				if len(sdir['size']) > sizeLength:
					sizeLength = len(sdir['size'])
				if len(sdir['file']) > fileLength:
					fileLength = len(sdir['file'])
				if len(sdir['day']) > 1:
					dateLength = 2
		else:
			keyName = keySplit[0]
			keySize	= key.size

			keyData = lsData[keyName] = {}
			for k, v in keyTimeData.iteritems():
				keyData[k] =  v
			keyData['size'] = str(keySize)
			keyData['perms'] = '-rwx------'
			keyData['owner'] = owner
			keyData['group'] = group
			keyData['hardcnt'] = '1'
			keyData['file'] = keyName

			if len(keyData['size']) > sizeLength:
				sizeLength = len(keyData['size'])
			if len(keyData['file']) > fileLength:
				fileLength = len(keyData['file'])
			if len(keyData['day']) > 1:
				dateLength = 2

	# Bring in subdir stuff
	for skey, svalue in subDirList.iteritems():
		lsData[skey] = svalue

	# Output
	lsFmt = '{perms} {hardcnt} {owner} {group} {size: >' + str(sizeLength) + '} {month} {day: >' + str(dateLength) + '} {time} {file: >' + str(fileLength) + '}'
	for k, v in lsData.iteritems():
		print lsFmt.format(**v)
	#pprint(lsData) #DEBUG
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
