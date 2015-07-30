#!/usr/bin/env python
import pika
import swiftclient
import urllib
import mimetypes
import json
import random
import time
import subprocess
import os
from sys import argv

def printUsage():
        print "Wrong number of arguments: ", len(argv)
        print "Usage: python aggregator-producer <amqpAddress> <swift_user> <swift_key> <queueName> <aggregatorExecPath> <inputFolderPath> <outputFolderPath>"

def getRabbitMQConnection(amqpURL,queueName):
        connection = pika.BlockingConnection(pika.URLParameters(amqpURL))
        channel = connection.channel()
        channel.queue_declare(queue=queueName,durable=True)
        return (connection,channel)

def getSwiftConnection(user,key):
        osOptions = dict()
        osOptions["user_domain_name"] = "Externos"
        osOptions["project_domain_name"] = "Externos"
        osOptions["project_name"] = "SD-Cloud"

        print "Establishing connection to Swift..."

        conn = swiftclient.Connection(
                user=user,
                key=key,
                authurl="http://10.5.0.14:5000/v3",
                #insecure=True,
                os_options=osOptions,
                auth_version=3,
        )

        print "Connected to Swift"

        return conn

def getFileMimeType(filePath):
        url = urllib.pathname2url(filePath)
        return mimetypes.guess_type(url)[0]

def storeFileOnSwift(swiftConn,filePath):
        fileName = filePath.split('/')[-1]
        fileMimeType = getFileMimeType(filePath)
        container_name = "tarciso-container"

        print "Transfering file..."

        with open(filePath, 'r') as input_file:
                swiftConn.put_object(container_name, fileName,
                        contents= input_file.read(),
                        content_type=fileMimeType)

        print "File successfully transfered!"
        return fileName

def sendRMQMsg(rmqChannel,queue,message):
        rmqChannel.basic_publish(exchange='',
                      routing_key=queue,
                      body=message,
                        properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
        print " [x] Sent %r" %(message,)

def prepareMsg(fileName):
	proctime = random.randint(1,10)
	msg_data = [{"file_path":fileName, "proc_time": proctime}]
	msg_str = json.dumps(msg_data)
	return msg_str

def runAggregator(execPath,inputDir,outputDir):
	execCommand = [execPath,str(1),inputDir,outputDir]
	proc = subprocess.Popen(execCommand,stdout=subprocess.PIPE)
	for line in proc.stdout:
            print line

def getFilesInDir(dirPath):
	dirFiles = [ os.path.join(dirPath,f) for f in os.listdir(dirPath) if os.path.isfile(os.path.join(dirPath,f)) ]
	return dirFiles

MIN_NUM_ARGS = 8

if (len(argv) < MIN_NUM_ARGS):
        printUsage()
        exit(1)

amqpAddress = argv[1]
swift_user = argv[2]
swift_key = argv[3]
queueName = argv[4]
aggregatorExecPath = argv[5]
inputFolderPath = argv[6]
outputFolderPath = argv[7]

runAggregator(aggregatorExecPath,inputFolderPath,outputFolderPath)
files = getFilesInDir(outputFolderPath)

swiftConn = getSwiftConnection(swift_user,swift_key)

for file in files:
	fileName = storeFileOnSwift(swiftConn,file)
	message = prepareMsg(fileName)

	msgSent = False
	while (not msgSent):
		try:
			rmqConn, rmqChannel = getRabbitMQConnection(amqpAddress,queueName)
			sendRMQMsg(rmqChannel,queueName,message)
			rmqConn.close()
			msgSent = True
		except(pika.exceptions.ConnectionClosed):
			print "Connection Closed.\nTrying again in 5s"
			time.sleep(5)
	



