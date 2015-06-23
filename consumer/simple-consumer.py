from kafka import KafkaClient, SimpleConsumer
from sys import argv

MIN_NUM_ARGS = 4

if (len(argv) < MIN_NUM_ARGS):
        print "Wrong number of arguments: ", len(argv)
        print "Usage: python simple-consumer <broker-address> <topic-name> <consumer-group-id>"
        exit(1)

kafkaClientAddress = argv[1]
topicName = argv[2]
consumerGroupId = argv[3]

kafkaClient = KafkaClient(kafkaClientAddress)
consumer = SimpleConsumer(kafkaClient, consumerGroupId, topicName)
consumer.max_buffer_size=0
consumer.seek(0,2)
for message in consumer:
 print("OFFSET: "+str(message[0])+"\t MSG: "+str(message[1][3]))
