import io
import json
from fdk import response 
from confluent_kafka import Consumer
from confluent_kafka import Producer, KafkaError  
import certifi
import traceback
import logging
import time
import random

# Based on the following documentatioN: 
# Pre-requisites: https://docs.oracle.com/en/cloud/paas/integration-cloud/stream-service-adapter/prerequisites-creating-connection.html
# Code      : https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/streaming-kafka-python-client-quickstart.htm 
# Code: https://blogs.oracle.com/developers/post/integrating-oracle-functions-and-oracle-streaming-service

__REGION='sa-santiago-1'
__ORIGIN_STREAM='sample_stream'
__DESTINATION_STREAM='outgoing_stream'
__TENANCY_NAME='ecrcloud'
__STREAMPOOL_OCID='ocid1.streampool.oc1.sa-santiago-1.amaaaaaatwfhi7yann7kjkdd4ftxo6xjzutflmqvb7za3xovrm4nc6qwmxia'
__OCI_USERNAME='oracleidentitycloudservice/denny.alquinta@oracle.com'
__SASL_USERNAME=__TENANCY_NAME+'/'+__OCI_USERNAME+'/'+__STREAMPOOL_OCID
__SASL_TOKEN='T_YUSe3W7VIbMui2kbj:'
delivered_records = 0  



def handler(ctx, data: io.BytesIO = None):       
    __RESP = oci_consumer(__ORIGIN_STREAM, __SASL_USERNAME, __SASL_TOKEN, __REGION)
    oci_producer(__DESTINATION_STREAM, __RESP, __SASL_USERNAME, __SASL_TOKEN, __REGION)
    return response.Response(
            ctx,
            response_data=json.dumps(__RESP),
            headers={"Content-Type": "application/json"}
    )      


def oci_consumer(origin_stream, sasl_username, sasl_token, region):
    before=time.time()
    topic = origin_stream
    conf = {  
        'bootstrap.servers': 'cell-1.streaming.'+region+'.oci.oraclecloud.com', 
        'security.protocol': 'SASL_SSL',       
        'ssl.ca.location': certifi.where(),   
        'sasl.mechanism': 'PLAIN',
        'sasl.username': sasl_username,  
        'sasl.password': sasl_token,  
        'group.id':'group-0',
        'api.version.request': False,
        'session.timeout.ms': 6000,
    }  

    # Create Consumer instance
    consumer = Consumer(conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    try:        
        msg = consumer.poll(0.5)
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting for message or event/error in poll()")       
            response = { "No new messages": "OK", "Time": str(to_ms(time.time()-before))+" ms" }     
            
        else:
            # Check for Kafka message
            record_key = "Null" if msg.key() is None else msg.key().decode('utf-8')
            record_value = msg.value().decode('utf-8')         
            returnable_value = "Consumed record with key "+ record_key + " and value " + record_value
            response = { returnable_value: "OK", "Time": str(to_ms(time.time()-before))+" ms"}
            consumer.close()
            
    except Exception as e:
        response = {"Error: "+str(e): "FAULT"}
        logging.exception(e, exc_info=True)
        consumer.close()
    return response
    
        
def oci_producer(destination_stream, message, sasl_username, sasl_token, region):
    topic = destination_stream
    conf = {  
        'bootstrap.servers': 'cell-1.streaming.'+region+'.oci.oraclecloud.com', 
        'security.protocol': 'SASL_SSL',      
        'ssl.ca.location': certifi.where(),               
        'sasl.mechanism': 'PLAIN',
        'sasl.username': sasl_username,  
        'sasl.password': sasl_token,  
        'group.id':'group-0',
        'api.version.request': False,
        'session.timeout.ms': 6000,
    }  
    producer = Producer(**conf)  
    
    
    
    record_key = "messageKey" + str(random.randint(1, 1000))
    record_value = "messageValue" + str(message)  
    print("Producing record: {}\t{}".format(record_key, record_value))  
    producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)      
    producer.poll(0) 
    producer.flush() 
            
def to_ms(seconds):
    return seconds * 1000.0

def acked(err, msg):  
    global delivered_records  
    """Delivery report handler called on  
        successful or failed delivery of message """  
    if err is not None:  
        print("Failed to deliver message: {}".format(err))  
    else:  
        delivered_records += 1  
        

