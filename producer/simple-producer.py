from kafka import SimpleProducer, KafkaClient
from sys import argv

MIN_NUM_ARGS = 3

if (len(argv) < MIN_NUM_ARGS):
        print "Wrong number of arguments: ", len(argv)
        print "Usage: python simple-producer <brokerAddress> <topic-name>"
        exit(1)

kafkaClientAddress = argv[1]
topic = argv[2]

# To send messages synchronously
client = KafkaClient(kafkaClientAddress)
print "Connected to Kafka Client at: ", kafkaClientAddress
producer = SimpleProducer(client)
print "Producer set up!"
print "Messages will be sent to topic: ", topic

while(True):
        msgContent = raw_input("Type messages to send to kafka client: ")
        producer.send_messages(topic, msgContent)

