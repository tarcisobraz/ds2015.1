#!/usr/bin/env python
import pika
import swiftclient
import urllib
import mimetypes
import json
import random
import time
from sys import argv

def printUsage():
        print "Wrong number of arguments: ", len(argv)
        print "Usage: python swift-consumer.py <amqpAddress> <queueName> <swift_user> <swift_key> <outputFolderPath> <verbose? 0 : 1>"

def getRabbitMQConnection(aqmpAddress,queueName):
	connection = pika.BlockingConnection(pika.URLParameters(aqmpAddress))
	channel = connection.channel()
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(callback,
                      queue=queueName)
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

def callback(ch, method, properties, body):
    print " [x] Received %r" % (body,)
    decoded_msg = json.loads(body)
    filePath = decoded_msg[0]["file_path"]
    retrieveSwiftObject(swiftConn,filePath)
    print "Processing..."
    print " [x] Done"
    ch.basic_ack(delivery_tag = method.delivery_tag)

MIN_NUM_ARGS = 7

if (len(argv) < MIN_NUM_ARGS):
        printUsage()
        exit(1)

amqpAddress = argv[1]
queueName = argv[2]
swift_user = argv[3]
swift_key = argv[4]
outputFolderPath = argv[5]
verbose = True if int(argv[6]) == 1 else False

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

