#!/usr/bin/env python
import pika
import swiftclient
import urllib
import mimetypes
import json
import random
import time
from sys import argv
from linuxUtils import LinuxUtils

def printUsage():
        print "Wrong number of arguments: ", len(argv)
        print "Usage: python swift-consumer.py <amqpAddress> <queueName> <swift_user> <swift_key> <outputFolderPath> <procScriptPath> <confFilePath> <responseQueue>  <verbose? 0 : 1>"

def getRabbitMQConnection(aqmpAddress,queueName):
	connection = pika.BlockingConnection(pika.URLParameters(aqmpAddress))
	channel = connection.channel()
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(callback,
                      queue=queueName)
	return (connection,channel)

def getSimpleRabbitMQConnection(aqmpAddress):
        connection = pika.BlockingConnection(pika.URLParameters(aqmpAddress))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        return (connection,channel)

def getSwiftConnection(user,key):
        osOptions = dict()
        osOptions["user_domain_name"] = "Externos"
        osOptions["project_domain_name"] = "Externos"
        osOptions["project_name"] = "SD-Cloud"

        if (verbose):
                print "Establishing connection to Swift..."

        conn = swiftclient.Connection(
                user=user,
                key=key,
                authurl="http://10.5.0.14:5000/v3",
                #insecure=True,
                os_options=osOptions,
                auth_version=3,
        )
        if (verbose):
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

def sendRMQMsg(amqpAddress,queue,message):
	msgSent = False
	while (not msgSent):
        	try:
                	rmqConn, rmqChannel = getSimpleRabbitMQConnection(amqpAddress)
			rmqChannel.queue_declare(queue=queue, durable=True)
		        rmqChannel.basic_publish(exchange='',
                	      routing_key=queue,
                      		body=message,
	                        properties=pika.BasicProperties(
        	                 delivery_mode = 2, # make message persistent
                	))
 		        print " [x] Sent %r" %(message,)	                
        	        rmqConn.close()
                	msgSent = True
	        except(pika.exceptions.ConnectionClosed):
        	        print "Connection Closed.\nTrying again in 5s"
                	time.sleep(5)


def prepareMsg(fileName):
        msg_data = [{"file_path":fileName}]
        msg_str = json.dumps(msg_data)
        return msg_str


def retrieveSwiftObject(swiftConn,objectPath):
        fileName = objectPath.split("/")[-1]
        outputFilePath = outputFolderPath + "/" + fileName
        container_name = "tarciso-container"
        if (verbose):
                print "Downloading Object", objectPath,"..."
        obj_tuple = swiftConn.get_object(container_name, objectPath)
        with open(outputFilePath, 'w') as my_object:
                my_object.write(obj_tuple[1])
        print "Object downloaded successfully"
	return outputFilePath

def runImageProcessingUnit(scriptPath,inputFilePath,configFilePath,resultFilePath):
	commandArray = ["/usr/bin/python",scriptPath,inputFilePath,configFilePath,resultFilePath]
	LinuxUtils.runLinuxCommand(" ".join(commandArray))


def sendResponse(amqpAddress,swiftC,resFilePath):
	fileName = storeFileOnSwift(swiftC,resFilePath)
	message = prepareMsg(fileName)
	sendRMQMsg(amqpAddress,responseQueue,message)

def callback(ch, method, properties, body):
    print " [x] Received %r" % (body,)
    decoded_msg = json.loads(body)
    filePath = decoded_msg[0]["file_path"]
    localFilePath = retrieveSwiftObject(swiftConn,filePath)
    resultFilePath = outputFolderPath + "/res-" + localFilePath.split("/")[-1]
    print "Processing..."
    runImageProcessingUnit(procScriptPath,localFilePath,confFilePath,resultFilePath)
    sendResponse(amqpAddress,swiftConn,resultFilePath)
    print " [x] Done"
    ch.basic_ack(delivery_tag = method.delivery_tag)

MIN_NUM_ARGS = 10

if (len(argv) < MIN_NUM_ARGS):
        printUsage()
        exit(1)

amqpAddress = argv[1]
queueName = argv[2]
swift_user = argv[3]
swift_key = argv[4]
outputFolderPath = argv[5]
procScriptPath = argv[6]
confFilePath = argv[7]
responseQueue = argv[8]
verbose = True if int(argv[9]) == 1 else False

rmqConn, rmqChannel = getRabbitMQConnection(amqpAddress,queueName)
swiftConn = getSwiftConnection(swift_user,swift_key)

while (True):
	try:
		rmqConn, rmqChannel = getRabbitMQConnection(amqpAddress,queueName)
		print ' [*] Waiting for messages. To exit press CTRL+C'		
		rmqChannel.start_consuming()
	except (pika.exceptions.ConnectionClosed):
		print "Connection Closed.\nTrying again in 5s"
                time.sleep(5)

